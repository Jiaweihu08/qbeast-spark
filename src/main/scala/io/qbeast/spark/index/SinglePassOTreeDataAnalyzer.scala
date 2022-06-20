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
      colStatsAcc: ColStatsAccumulator,
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
          if (rows.isEmpty) {
            Seq[CubeWeightAndStats]().iterator
          } else {
            val (iterForStats, iterForCubeWeights) = rows.duplicate
            val epsilon = 42.0
            var numElems = 0L
            val partitionColStats = iterForStats
              .foldLeft(colStatsAcc.value) { case (colStats, row) =>
                numElems += 1
                colStats.map(stats => updateColStats(stats, row))
              }
              .map(stats =>
                if (stats.min == stats.max) {
                  if (stats.max + epsilon < Double.MaxValue) {
                    stats.copy(max = stats.max + epsilon)
                  } else {
                    stats.copy(min = stats.min - epsilon)
                  }
                } else {
                  stats
                })

            colStatsAcc.add(partitionColStats)

            val partitionRevision =
              indexStatus.revision.copy(transformations = getTransformations(partitionColStats))

            val weights =
              new CubeWeightsBuilder(
                indexStatus = indexStatus,
                numPartitions = numPartitions,
                numElements = numPartitions * numElems,
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
          // Compute cube's global coordinates
          val cubeGlobalCoordinates = toGlobalCoordinates(
            cube.from.coordinates,
            cube.to.coordinates,
            localColStats,
            globalColStats)

          // Find the global-level overlapping cubes that are from the same level as cube.depth.
          // Only the useful branches are considered at each iteration.
          val overlappingCubes = (0 until cube.depth).foldLeft(Seq(CubeId.root(dimensionCount))) {
            case (candidates, _) =>
              candidates.flatMap(_.children.filter(c => hasOverlap(c, cubeGlobalCoordinates)))
          }

          // Compute overlapping cube weights from overlapping cubes
          var cubeOverlaps = 0.0
          val overlappingCubeWeights = overlappingCubes
            .map { candidate =>
              val overlap = cubeGlobalCoordinates.indices.map { i =>
                val (cubeFrom, cubeTo) = cubeGlobalCoordinates(i)
                val candidateFrom = candidate.from.coordinates(i)
                val candidateTo = candidate.to.coordinates(i)
                val cubeDimWidth = cubeTo - cubeFrom
                assert(candidateFrom < cubeTo && cubeFrom < candidateTo)
                (candidateTo - cubeFrom)
                  .min(cubeTo - candidateFrom)
                  .min(cubeDimWidth) / cubeDimWidth
              }.product

              cubeOverlaps += overlap
              CubeNormalizedWeight(candidate.bytes, cubeWeight / overlap)
            }
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

    val globalColStatsAcc = new ColStatsAccumulator(
      initializeColStats(columnsToIndex, selected.schema))
    sparkContext.register(globalColStatsAcc, "globalColStatsAcc")

    // Estimate the cube weights at partition level
    val partitionedEstimatedCubeWeights =
      selected
        .transform(estimatePartitionCubeWeights(0, globalColStatsAcc, indexStatus, isReplication))
        .collect()

    val globalColStats = globalColStatsAcc.value
    val transformations = getTransformations(globalColStats)
    val lastRevision = indexStatus.revision.copy(transformations = transformations)

    // Map partition cube weights to global cube weights
//    val startTime = System.currentTimeMillis()

    val globalEstimatedCubeWeights =
      toGlobalCubeWeights(partitionedEstimatedCubeWeights, columnsToIndex.size, globalColStats)

    // Compute the overall estimated cube weights
    val estimatedCubeWeights: Map[CubeId, NormalizedWeight] =
      globalEstimatedCubeWeights
        .groupBy(cw => lastRevision.createCubeId(cw.cubeBytes))
        .mapValues(cubeWeights => 1.0 / cubeWeights.map(1.0 / _.normalizedWeight).sum)

    // scalastyle:off
    val dcs = indexStatus.revision.desiredCubeSize
    val partitionCubeCount = partitionedEstimatedCubeWeights.length
    println(s""">>> SinglePass;
         |desiredCubeSize: $dcs,
         |Number of partition cubes: $partitionCubeCount,
         |Number of final estimated cubes: ${estimatedCubeWeights.size}
         |""".stripMargin.replaceAll("\n", ""))

//    val runtime = System.currentTimeMillis() - startTime
//    println(">>>>>>>>>>>>>>")
//    println(s"GREP: toGlobalCubeWeights took $runtime ms")
//    println(">>>>>>>>>>>>>>\n")

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
