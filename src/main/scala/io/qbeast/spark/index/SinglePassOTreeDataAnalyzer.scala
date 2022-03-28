/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.index

import io.qbeast.core.model._
import io.qbeast.spark.index.DoublePassOTreeDataAnalyzer.addRandomWeight
import io.qbeast.spark.index.QbeastColumns.{cubeToReplicateColumnName, weightColumnName}
import io.qbeast.spark.index.SinglePassColStatsUtils._
import org.apache.spark.qbeast.config.CUBE_WEIGHTS_BUFFER_CAPACITY
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import scala.collection.mutable

object SinglePassOTreeDataAnalyzer extends OTreeDataAnalyzer with Serializable {

  private[index] def estimatePartitionCubeWeights(
      numElements: Long,
      columnsToIndex: Seq[String],
      colStatsAcc: ColStatsAccumulator,
      revision: Revision,
      indexStatus: IndexStatus,
      isReplication: Boolean): DataFrame => Dataset[CubeWeightAndStats] =
    (selected: DataFrame) => {
      val spark = SparkSession.active
      import spark.implicits._

      val numPartitions: Int = selected.rdd.getNumPartitions
      val bufferCapacity: Long = CUBE_WEIGHTS_BUFFER_CAPACITY
      val weightIndex = selected.schema.fieldIndex(weightColumnName)

      val initialColStats: Seq[ColStats] = initializeColStats(columnsToIndex, selected.schema)

      selected
        .mapPartitions(rows => {
          val (iterForStats, iterForCubeWeights) = rows.duplicate
          if (iterForStats.isEmpty) {
            Seq[CubeWeightAndStats]().iterator
          } else {
            val partitionColStats = iterForStats.foldLeft(initialColStats) {
              case (colStats, row) =>
                colStats.map(stats => updateColStats(stats, row))
            }

            colStatsAcc.add(partitionColStats)

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
                val parent = Some(partitionRevision.createCubeId(parentBytes))
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
      partitionedEstimatedCubeWeights: Array[CubeWeightAndStats],
      dimensionCount: Int,
      globalColStats: Seq[ColStats]): Array[CubeNormalizedWeight] = {

    partitionedEstimatedCubeWeights.flatMap {
      case CubeWeightAndStats(
            cubeBytes: Array[Byte],
            cubeWeight: NormalizedWeight,
            colStats: Seq[ColStats]) =>
        val allCubeOverlaps = mutable.ArrayBuffer[CubeNormalizedWeight]()
        if (colStats.zip(globalColStats).forall { case (local, global) =>
            local.min == global.min && local.max == global.max
          }) {
          allCubeOverlaps
        } else {
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
    }
  }

  override def analyze(
      dataFrame: DataFrame,
      indexStatus: IndexStatus,
      isReplication: Boolean): (DataFrame, TableChanges) = {
    val sparkContext = SparkSession.active.sparkContext

//    if (dataFrame.take(1).isEmpty) {
//      throw new RuntimeException(
//        "The DataFrame is empty, why are you trying to index an empty dataset?")
//    }

    // Add weight column to dataFrame
    val weightedDataFrame =
      dataFrame.transform(addRandomWeight(indexStatus.revision))

    val columnsToIndex = indexStatus.revision.columnTransformers.map(_.columnName)

    val oTreeColumns = if (isReplication) {
      Seq(weightColumnName, cubeToReplicateColumnName)
    } else {
      Seq(weightColumnName)
    }
    val cols = columnsToIndex ++ oTreeColumns

    val selected = weightedDataFrame
      .select(cols.map(col): _*)

    val globalColStatsAcc = new ColStatsAccumulator(
      initializeColStats(columnsToIndex, selected.schema))
    sparkContext.register(globalColStatsAcc, "globalColStatsAcc")

    // Estimate the cube weights at partition level
    val partitionedEstimatedCubeWeights =
      selected
        .transform(
          estimatePartitionCubeWeights(
            0,
            columnsToIndex,
            globalColStatsAcc,
            indexStatus.revision,
            indexStatus,
            isReplication))
        .collect()

    val globalColStats = globalColStatsAcc.value
    // Map partition cube weights to global cube weights
    val globalEstimatedCubeWeights =
      toGlobalCubeWeights(partitionedEstimatedCubeWeights, columnsToIndex.size, globalColStats)

    val transformations = getTransformations(globalColStats)
    val lastRevision = indexStatus.revision.copy(transformations = transformations)

    // Compute the overall estimated cube weights
    val estimatedCubeWeights: Map[CubeId, NormalizedWeight] =
      globalEstimatedCubeWeights
        .groupBy(cw => lastRevision.createCubeId(cw.cubeBytes))
        .mapValues(cubeWeights => 1.0 / cubeWeights.map(1.0 / _.normalizedWeight).sum)

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

}
