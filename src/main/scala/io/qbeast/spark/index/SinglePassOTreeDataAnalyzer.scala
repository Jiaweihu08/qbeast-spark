/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.index

import io.qbeast.IISeq
import io.qbeast.core.model._
import io.qbeast.core.transform.Transformation
import io.qbeast.spark.index.DoublePassOTreeDataAnalyzer.{addRandomWeight, estimateCubeWeights}
import io.qbeast.spark.index.QbeastColumns.{cubeToReplicateColumnName, weightColumnName}
import io.qbeast.spark.index.SinglePassColStatsUtils.{
  getTransformations,
  initializeColStats,
  mergedColStats,
  updatedColStats
}
import org.apache.spark.qbeast.config.CUBE_WEIGHTS_BUFFER_CAPACITY
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import scala.collection.mutable

object SinglePassOTreeDataAnalyzer extends OTreeDataAnalyzer with Serializable {

  private[index] def estimatePartitionCubeWeights(
      numElements: Long,
      revision: Revision,
      indexStatus: IndexStatus,
      isReplication: Boolean): DataFrame => Dataset[CubeWeightAndStats] =
    (weightedDataFrame: DataFrame) => {
      val spark = SparkSession.active
      import spark.implicits._

      val indexColumns = if (isReplication) {
        Seq(weightColumnName, cubeToReplicateColumnName)
      } else {
        Seq(weightColumnName)
      }
      val columnsToIndex = revision.columnTransformers.map(_.columnName)

      val cols = columnsToIndex ++ indexColumns

      val numPartitions: Int = weightedDataFrame.rdd.getNumPartitions
      val bufferCapacity: Long = CUBE_WEIGHTS_BUFFER_CAPACITY

      val selected = weightedDataFrame
        .select(cols.map(col): _*)
      val weightIndex = selected.schema.fieldIndex(weightColumnName)

      var partitionColStats = initializeColStats(columnsToIndex, selected.schema)

      selected
        .mapPartitions(rows => {
          val (iterForStats, iterForCubeWeights) = rows.duplicate
          if (iterForStats.isEmpty) {
            Seq[CubeWeightAndStats]().iterator
          } else {
            iterForStats.foreach { row =>
              partitionColStats = partitionColStats.map(stats => updatedColStats(stats, row))
            }

            val partitionRevision =
              revision.copy(transformations = getTransformations(partitionColStats))

            val weights =
              new CubeWeightsBuilder(
                indexStatus = indexStatus,
                numPartitions = numPartitions,
                numElements = numElements,
                bufferCapacity = bufferCapacity)

            iterForCubeWeights.foreach { row =>
              val point = RowUtils.rowValuesToPoint(row, partitionRevision)
              val weight = Weight(row.getAs[Int](weightIndex))
              if (isReplication) {
                val parentBytes = row.getAs[Array[Byte]](cubeToReplicateColumnName)
                val parent = Some(revision.createCubeId(parentBytes))
                weights.update(point, weight, parent)
              } else weights.update(point, weight)
            }
            weights
              .result()
              .map { case CubeNormalizedWeight(cubeBytes, weight) =>
                CubeWeightAndStats(cubeBytes, weight, partitionColStats)
              }
              .iterator
          }
        })
    }

  private[index] def toGlobalCubeWeights(
      partitionedEstimatedCubeWeights: Dataset[CubeWeightAndStats],
      revision: Revision,
      schema: StructType): (Dataset[CubeNormalizedWeight], IISeq[Transformation]) = {

    val colsToIndex = revision.columnTransformers.map(_.columnName)
    // Initialize global column stats
    var globalColStats: Seq[ColStats] =
      initializeColStats(colsToIndex, schema)

    // Gather all column stats
    val partitionColumnStats: Set[Seq[ColStats]] =
      partitionedEstimatedCubeWeights.collect().map(_.colStats).toSet

    // Merge to get global column stats. Ths process can be simplified
    // by using Customized Accumulators
    partitionColumnStats.foreach { partitionColStats =>
      globalColStats = globalColStats.zip(partitionColStats).map { case (global, local) =>
        mergedColStats(global, local)
      }
    }

    val globalTransformations = getTransformations(globalColStats)

    val dimensionCount = colsToIndex.size

    val spark = SparkSession.active
    import spark.implicits._

    val globalCubeAndOverlaps = partitionedEstimatedCubeWeights.flatMap {
      case CubeWeightAndStats(
            cubeBytes: Array[Byte],
            cubeWeight: NormalizedWeight,
            colStats: Seq[ColStats]) =>
        val allCubeOverlaps = mutable.ArrayBuffer[CubeNormalizedWeight]()

        val cube = CubeId(dimensionCount, cubeBytes)

        val cubeGlobalCoordinates = 0
          .until(dimensionCount)
          .map(i =>
            toGlobalCoordinates(
              cube.from.coordinates(i),
              cube.to.coordinates(i),
              colStats(i),
              globalColStats(i)))

        var cubeCandidates = Seq(CubeId.root(dimensionCount))
        (0 until cube.depth).foreach { _ =>
          cubeCandidates = cubeCandidates.flatMap(c => c.children)
        }

        var cubeOverlaps = 0.0
        cubeCandidates.foreach { candidate =>
          var isOverlapping = true
          val candidateCoordinates = candidate.from.coordinates.zip(candidate.to.coordinates)
          val dimensionOverlaps = candidateCoordinates.zip(cubeGlobalCoordinates).map {
            case ((candidateFrom, candidateTo), (cubeFrom, cubeTo)) =>
              if (candidateFrom < cubeTo && cubeFrom < candidateTo) {
                val cubeDimWidth = cubeTo - cubeFrom
                (candidateTo - cubeFrom)
                  .min(cubeTo - candidateFrom)
                  .min(cubeDimWidth) / cubeDimWidth
              } else {
                isOverlapping = false
                0.0
              }
          }
          if (isOverlapping) {
            val candidateOverlap = dimensionOverlaps
              .foldLeft(1.0)((acc, dimOverlap) => acc * dimOverlap)
            cubeOverlaps += candidateOverlap
            allCubeOverlaps += CubeNormalizedWeight(cubeBytes, cubeWeight / candidateOverlap)
          }
        }
        // Make sure that all overlapping cubes from the same depth are found
        assert(math.abs(1.0 - cubeOverlaps) < 1e-15)
        allCubeOverlaps
    }

    (globalCubeAndOverlaps, globalTransformations)
  }

  override def analyze(
      dataFrame: DataFrame,
      indexStatus: IndexStatus,
      isReplication: Boolean): (DataFrame, TableChanges) = {
    if (dataFrame.take(1).isEmpty) {
      throw new RuntimeException(
        "The DataFrame is empty, why are you trying to index an empty dataset?")
    }

    // Add weight column to dataFrame
    val weightedDataFrame =
      dataFrame.transform(addRandomWeight(indexStatus.revision))

    // Estimate the cube weights at partition level
    val partitionedEstimatedCubeWeights = weightedDataFrame.transform(
      estimatePartitionCubeWeights(0, indexStatus.revision, indexStatus, isReplication))

    // Map partition cube weights to global cube weights
    val (globalEstimatedCubeWeights, transformations) =
      toGlobalCubeWeights(
        partitionedEstimatedCubeWeights,
        indexStatus.revision,
        weightedDataFrame.schema)
    val lastRevision = indexStatus.revision.copy(transformations = transformations)

    // Compute the overall estimated cube weights
    val estimatedCubeWeights =
      globalEstimatedCubeWeights
        .transform(estimateCubeWeights(lastRevision))
        .collect()
        .toMap

    val revChange = Some(
      RevisionChange(
        supersededRevision = indexStatus.revision,
        timestamp = System.currentTimeMillis(),
        transformationsChanges = transformations.map(Option(_))))
    // Gather the new changes
    val tableChanges = BroadcastedTableChanges(
      revChange,
      indexStatus.copy(revision = lastRevision),
      estimatedCubeWeights,
      if (isReplication) indexStatus.cubesToOptimize
      else Set.empty[CubeId])

    (weightedDataFrame, tableChanges)
  }

  def toGlobalCoordinates(
      from: Double,
      to: Double,
      local: ColStats,
      global: ColStats): (Double, Double) = {
    assert(local.colName == global.colName && local.dType == global.dType)
    if (global.dType == "StringDataType" || global.min == local.min && global.max == local.max) {
      (from, to)
    } else {
      val (gScale, lScale) = (global.max - global.min, local.max - local.min)
      val scale = lScale / gScale
      val offset = (local.min - global.min) / gScale
      (from * scale + offset, to * scale + offset)
    }
  }

}