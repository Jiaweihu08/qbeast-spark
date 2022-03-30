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

object SinglePassOTreeDataAnalyzer extends OTreeDataAnalyzer with Serializable {

  private[index] def estimatePartitionCubeWeights(
      numElements: Long,
      initialColStats: Seq[ColStats],
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

  private[index] def hasOverlap(
      candidate: CubeId,
      cubeGlobalCoordinates: Seq[(Double, Double)]): Boolean = {
    // For two cubes to overlap, all dimensions must overlap
    candidate.from.coordinates.indices.forall { i =>
      val cubeFrom = cubeGlobalCoordinates(i)._1
      val cubeTo = cubeGlobalCoordinates(i)._2
      candidate.from.coordinates(i) < cubeTo && cubeFrom < candidate.to.coordinates(i)
    }
  }

  private[index] def toGlobalCubeWeights(
      partitionedEstimatedCubeWeights: Array[CubeWeightAndStats],
      dimensionCount: Int,
      globalColStats: Seq[ColStats]): Array[CubeNormalizedWeight] = {

    partitionedEstimatedCubeWeights.flatMap {
      case CubeWeightAndStats(
            cubeBytes: Array[Byte],
            cubeWeight: NormalizedWeight,
            localColStats: Seq[ColStats]) =>
        val hasGlobalRanges = (0 until dimensionCount).forall { idx =>
          localColStats(idx).min == globalColStats(idx).min &&
          localColStats(idx).max == globalColStats(idx).max
        }
        if (hasGlobalRanges) {
          // If a CubeWeightAndStats has global ranges, return directly
          Seq(CubeNormalizedWeight(cubeBytes, cubeWeight))
        } else {
          val cube = CubeId(dimensionCount, cubeBytes)
          val cubeGlobalCoordinates = toGlobalCoordinates(
            cube.from.coordinates,
            cube.to.coordinates,
            localColStats,
            globalColStats)

          // Find the overlapping cubes that are from cube.depth. Considering only
          // the useful branches at each iteration.
          val overlappingCubes = (0 until cube.depth).foldLeft(Seq(CubeId.root(dimensionCount))) {
            case (candidates, _) =>
              candidates.flatMap(_.children.filter(c => hasOverlap(c, cubeGlobalCoordinates)))
          }

          // Compute overlaps for each overlapping cube
          var cubeOverlaps = 0.0
          val overlappingCubeWeights = overlappingCubes.map { candidate =>
            val dimensionOverlaps = cube.from.coordinates.indices.map { i =>
              val (cubeFrom, cubeTo) = cubeGlobalCoordinates(i)
              val candidateFrom = candidate.from.coordinates(i)
              val candidateTo = candidate.to.coordinates(i)
              assert(candidateFrom < cubeTo && cubeFrom < candidateTo)
              val cubeDimWidth = cubeTo - cubeFrom
              (candidateTo - cubeFrom)
                .min(cubeTo - candidateFrom)
                .min(cubeDimWidth) / cubeDimWidth
            }
            val candidateOverlap = dimensionOverlaps.product
            assert(candidateOverlap > 0.0, "Non-existent overlap")
            cubeOverlaps += candidateOverlap
            CubeNormalizedWeight(cubeBytes, cubeWeight / candidateOverlap)
          }
          // Make sure that all overlapping cubes from the same depth are found
          assert(math.abs(1.0 - cubeOverlaps) < 1e-15)
          overlappingCubeWeights
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

    val initialColStats = initializeColStats(columnsToIndex, selected.schema)
    val globalColStatsAcc = new ColStatsAccumulator(initialColStats)
    sparkContext.register(globalColStatsAcc, "globalColStatsAcc")

    // Estimate the cube weights at partition level
    val partitionedEstimatedCubeWeights =
      selected
        .transform(
          estimatePartitionCubeWeights(
            0,
            initialColStats,
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
