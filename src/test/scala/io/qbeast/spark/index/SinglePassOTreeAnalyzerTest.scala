package io.qbeast.spark.index

import io.qbeast.IISeq
import io.qbeast.TestClasses._
import io.qbeast.core.model._
import io.qbeast.core.transform.Transformer
import io.qbeast.spark.QbeastIntegrationTestSpec
import io.qbeast.spark.index.QbeastColumns.weightColumnName
import io.qbeast.spark.index.SinglePassColStatsUtils.{getTransformations, initializeColStats}
import io.qbeast.spark.index.SinglePassOTreeDataAnalyzer.{
  estimatePartitionCubeWeights,
  toGlobalCubeWeights
}
import io.qbeast.spark.utils.SparkToQTypesUtils
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.{Dataset, SparkSession}

class SinglePassOTreeAnalyzerTest extends QbeastIntegrationTestSpec {

  private def createDFAndStatus(size: Int, spark: SparkSession): (Dataset[T3], IndexStatus) = {
    import spark.implicits._

    val data = 0
      .to(size)
      .map(i => T3(i, i.toDouble, i.toString, i.toFloat))
      .toDF()
      .as[T3]

    val columnsSchema = data.schema
    val columnTransformers = createTransformers(columnsSchema)
    val revision =
      Revision(0, 1000, QTableID("test"), 1000, columnTransformers, Seq.empty.toIndexedSeq)
    val indexStatus = IndexStatus(revision, Set.empty)

    (data, indexStatus)
  }

  private def createTransformers(columnsSchema: Seq[StructField]): IISeq[Transformer] = {
    columnsSchema
      .map(field => Transformer(field.name, SparkToQTypesUtils.convertDataTypes(field.dataType)))
      .toIndexedSeq
  }

  it should "correctly output local and global limits correctly" in withSpark { spark =>
    val (data, indexStatus) = createDFAndStatus(10000, spark)

    val weightedDataFrame =
      data.withColumn(weightColumnName, lit(scala.util.Random.nextInt()))
    val columnsToIndex = indexStatus.revision.columnTransformers.map(_.columnName)

    val selected = weightedDataFrame
      .select((columnsToIndex :+ weightColumnName).map(col): _*)
      .repartition(10)

    val globalColStatsAcc =
      new ColStatsAccumulator(initializeColStats(columnsToIndex, selected.schema))
    val ColStatsIdMapAcc = new ColStatsIdAccumulator(Map.empty[Int, Seq[ColStats]])

    spark.sparkContext.register(globalColStatsAcc, "globalColStatsAcc")
    spark.sparkContext.register(ColStatsIdMapAcc, "colStatsIdMapAcc")

    // Estimate the cube weights at partition level
    val _ =
      estimatePartitionCubeWeights(
        selected,
        0,
        globalColStatsAcc,
        ColStatsIdMapAcc,
        indexStatus,
        isReplication = false)
        .collect()

    val globalColStats = globalColStatsAcc.value
    val colStatsIdMap = ColStatsIdMapAcc.value

    val allPartitionColStats = colStatsIdMap.values
    // All partition ColStats should have the correct number of dimensions
    allPartitionColStats.forall(colStats => colStats.size == columnsToIndex.size) shouldBe true

    // All partition ColStats should have the correct columns in the correct order
    allPartitionColStats.forall { partitionColStats =>
      partitionColStats.zip(columnsToIndex).forall { case (stats, colName) =>
        stats.colName == colName
      }
    } shouldBe true

    // All partition ColStats should have existent ranges
    allPartitionColStats.forall { partitionColStats =>
      partitionColStats.forall { stats =>
        if (stats.dType == "StringDataType") true
        else stats.min < stats.max
      }
    } shouldBe true

    // Global ColStats should have the correct number of dimensions
    globalColStats.size shouldBe columnsToIndex.size

    // Global ColStats should have the correct columns in the correct order
    globalColStats.zip(columnsToIndex).forall { case (stats, colName) =>
      stats.colName == colName
    } shouldBe true

    // Global ColStats should have existent ranges
    globalColStats.forall { stats =>
      if (stats.dType == "StringDataType") true
      else stats.min < stats.max
    } shouldBe true

    // All partition ColStats should be contained in the global ColStats
    allPartitionColStats.forall { partitionColStats =>
      partitionColStats zip globalColStats forall { case (local, global) =>
        local.colName == global.colName &&
          local.min >= global.min && local.max <= global.max
      }
    } shouldBe true

  }
  it should "estimatePartitionCubeWeights" in withSpark { spark =>
    val (data, indexStatus) = createDFAndStatus(10000, spark)

    // Add weights to DF to index
    val weightedDataFrame =
      data.withColumn(weightColumnName, lit(scala.util.Random.nextInt()))

    val columnsToIndex = indexStatus.revision.columnTransformers.map(_.columnName)
    val selected = weightedDataFrame
      .select((columnsToIndex :+ weightColumnName).map(col): _*)
      .repartition(10)

    val globalColStatsAcc =
      new ColStatsAccumulator(initializeColStats(columnsToIndex, selected.schema))
    val ColStatsIdMapAcc = new ColStatsIdAccumulator(Map.empty[Int, Seq[ColStats]])
    spark.sparkContext.register(globalColStatsAcc, "globalColStatsAcc")
    spark.sparkContext.register(ColStatsIdMapAcc, "colStatsIdMapAcc")

    // Estimate the cube weights at partition level
    val partitionedEstimatedCubeWeights =
      estimatePartitionCubeWeights(
        selected,
        0,
        globalColStatsAcc,
        ColStatsIdMapAcc,
        indexStatus,
        isReplication = false)
        .collect()

    val numPartitions = weightedDataFrame.rdd.getNumPartitions

    partitionedEstimatedCubeWeights
      .groupBy(_.cubeBytes)
      .foreach { case (_, cw: Array[CubeWeightAndPartitionId]) =>
        cw.foreach(w => w.normalizedWeight shouldBe >(0.0))
        cw.length shouldBe <=(numPartitions)
      }
  }

  it should "compute the correct amount of global Cube Weights" in withSpark { spark =>
    val (data, indexStatus) = createDFAndStatus(10000, spark)

    val weightedDataFrame =
      data.withColumn(weightColumnName, lit(scala.util.Random.nextInt()))

    val columnsToIndex = indexStatus.revision.columnTransformers.map(_.columnName)
    val selected = weightedDataFrame
      .select((columnsToIndex :+ weightColumnName).map(col): _*)
      .repartition(10)

    val globalColStatsAcc =
      new ColStatsAccumulator(initializeColStats(columnsToIndex, selected.schema))
    val ColStatsIdMapAcc = new ColStatsIdAccumulator(Map.empty[Int, Seq[ColStats]])
    spark.sparkContext.register(globalColStatsAcc, "globalColStatsAcc")
    spark.sparkContext.register(ColStatsIdMapAcc, "colStatsIdMapAcc")

    // Estimate the cube weights at partition level
    val partitionedEstimatedCubeWeights =
      estimatePartitionCubeWeights(
        selected,
        0,
        globalColStatsAcc,
        ColStatsIdMapAcc,
        indexStatus,
        isReplication = false)
        .collect()

    val globalColStats = globalColStatsAcc.value
    val colStatsIdMap = ColStatsIdMapAcc.value

    // Map partition cube weights to global cube weights
    val globalEstimatedCubeWeights =
      toGlobalCubeWeights(
        partitionedEstimatedCubeWeights,
        columnsToIndex.size,
        globalColStats,
        colStatsIdMap)

    // For each partition-level cube there can be multiple overlapping cubes from
    // the global space
    partitionedEstimatedCubeWeights.length <= globalEstimatedCubeWeights.length
  }

  it should "estimate Cube Weights correctly" in withSpark { spark =>
    val (data, indexStatus) = createDFAndStatus(10000, spark)

    // Add weights to DF to index
    val weightedDataFrame =
      data.withColumn(weightColumnName, lit(scala.util.Random.nextInt()))

    val columnsToIndex = indexStatus.revision.columnTransformers.map(_.columnName)
    val selected = weightedDataFrame
      .select((columnsToIndex :+ weightColumnName).map(col): _*)

    val globalColStatsAcc =
      new ColStatsAccumulator(initializeColStats(columnsToIndex, selected.schema))
    val ColStatsIdMapAcc = new ColStatsIdAccumulator(Map.empty[Int, Seq[ColStats]])

    spark.sparkContext.register(globalColStatsAcc, "globalColStatsAcc")
    spark.sparkContext.register(ColStatsIdMapAcc, "colStatsIdMapAcc")

    // Estimate the cube weights at partition level
    val partitionedEstimatedCubeWeights =
      estimatePartitionCubeWeights(
        selected,
        0,
        globalColStatsAcc,
        ColStatsIdMapAcc,
        indexStatus,
        isReplication = false)
        .collect()

    val globalColStats = globalColStatsAcc.value
    val colStatsIdMap = ColStatsIdMapAcc.value
    val transformations = getTransformations(globalColStats)
    val lastRevision = indexStatus.revision.copy(transformations = transformations)

    // Map partition cube weights to global cube weights
    val globalEstimatedCubeWeights =
      toGlobalCubeWeights(
        partitionedEstimatedCubeWeights,
        columnsToIndex.size,
        globalColStats,
        colStatsIdMap)

    // Compute the overall estimated cube weights
    val estimatedCubeWeights: Map[CubeId, NormalizedWeight] =
      globalEstimatedCubeWeights
        .groupBy(cw => lastRevision.createCubeId(cw.cubeBytes))
        .mapValues(cubeWeights => 1.0 / cubeWeights.map(1.0 / _.normalizedWeight).sum)

    estimatedCubeWeights.foreach { case (_, weight) =>
      weight shouldBe >(0.0)
    }

    globalEstimatedCubeWeights.length >= estimatedCubeWeights.size

  }

  "toGlobalCubeWeights" should
    "return its input when partition limits are the same as global limits " in withSpark {
      spark =>
        val (data, indexStatus) = createDFAndStatus(10000, spark)
        val columnsToIndex = indexStatus.revision.columnTransformers.map(_.columnName)

        val weightedDataFrame =
          data.toDF().withColumn(weightColumnName, lit(scala.util.Random.nextInt()))

        // Merge all partitions into one to ensure partition stats are the same as
        // global stats
        val selected = weightedDataFrame
          .select((columnsToIndex :+ weightColumnName).map(col): _*)
          .coalesce(1)

        val globalColStatsAcc =
          new ColStatsAccumulator(initializeColStats(columnsToIndex, selected.schema))
        val ColStatsIdMapAcc = new ColStatsIdAccumulator(Map.empty[Int, Seq[ColStats]])
        spark.sparkContext.register(globalColStatsAcc, "globalColStatsAcc")
        spark.sparkContext.register(ColStatsIdMapAcc, "colStatsIdMapAcc")

        // Estimate the cube weights at partition level
        val partitionedEstimatedCubeWeights =
          estimatePartitionCubeWeights(
            selected,
            0,
            globalColStatsAcc,
            ColStatsIdMapAcc,
            indexStatus,
            isReplication = false)
            .collect()

        val globalColStats = globalColStatsAcc.value
        val colStatsIdMap = ColStatsIdMapAcc.value

        // Map partition cube weights to global cube weights
        val globalEstimatedCubeWeights =
          toGlobalCubeWeights(
            partitionedEstimatedCubeWeights,
            columnsToIndex.size,
            globalColStats,
            colStatsIdMap)

        val partitionCubeNormalizedWeights = partitionedEstimatedCubeWeights.map {
          case CubeWeightAndPartitionId(cubeBytes, normalizedWeight, _) =>
            CubeNormalizedWeight(cubeBytes, normalizedWeight)
        }
        partitionCubeNormalizedWeights shouldBe globalEstimatedCubeWeights
    }

  it should "return the same cubeWeights when partition and global limits are the same" in {
    val cubes = CubeId.root(2).children

    val localColStats = Seq(
      ColStats("x", "DoubleDataType", 0.0, 100.0),
      ColStats("y", "DoubleDataType", 0.0, 100.0))

    val globalColStats = Seq(
      ColStats("x", "DoubleDataType", 0.0, 100.0),
      ColStats("y", "DoubleDataType", 0.0, 100.0))

    val cubeWeightAndPartitionId =
      cubes.map(c => CubeWeightAndPartitionId(c.bytes, 1.0, 0)).toArray
    val rootOverlap =
      toGlobalCubeWeights(cubeWeightAndPartitionId, 2, globalColStats, Map(0 -> localColStats))

    cubeWeightAndPartitionId.map { case CubeWeightAndPartitionId(bytes, w, _) =>
      CubeNormalizedWeight(bytes, w)
    } shouldBe rootOverlap
  }

  it should "return 4 correct overlapping cubesWeights" in {
    // The top left cube CubeId(2, 1, A) with the proper partition limits should evenly
    // overlap with all four cubes from the first level.
    val cubes = CubeId.root(2).children.toList

    val localColStats = Seq(
      ColStats("x", "DoubleDataType", 37.5, 87.5),
      ColStats("y", "DoubleDataType", 37.5, 87.5))

    val globalColStats = Seq(
      ColStats("x", "DoubleDataType", 0.0, 100.0),
      ColStats("y", "DoubleDataType", 0.0, 100.0))

    val cubeWeightAndPartitionId = Array(CubeWeightAndPartitionId(cubes.head.bytes, 1.0, 0))
    val overlappingCubeWeights =
      toGlobalCubeWeights(cubeWeightAndPartitionId, 2, globalColStats, Map(0 -> localColStats))

    val overlappingCubes = overlappingCubeWeights.map(cw => CubeId(2, cw.cubeBytes))
    val overlappingWeights = overlappingCubeWeights.map(_.normalizedWeight).toSet

    overlappingCubes.length shouldBe cubes.length

    cubes.forall(overlappingCubes.contains) shouldBe true

    overlappingWeights.size shouldBe 1

    overlappingWeights.head shouldBe 1.0 / 0.25
  }

  it should "return 2 correct overlapping cubesWeights" in {
    // Top right cube CubeId(2, 1, g) with the proper partition limits should evenly
    // overlaps with two cubes from the first level.
    val cubes = CubeId.root(2).children.toList

    val localColStats = Seq(
      ColStats("x", "DoubleDataType", 37.5, 87.5),
      ColStats("y", "DoubleDataType", 37.5, 87.5))

    val globalColStats = Seq(
      ColStats("x", "DoubleDataType", 0.0, 100.0),
      ColStats("y", "DoubleDataType", 0.0, 100.0))

    val cubeWeightAndPartitionId = Array(CubeWeightAndPartitionId(cubes(2).bytes, 1.0, 0))
    val overlappingCubeWeights =
      toGlobalCubeWeights(cubeWeightAndPartitionId, 2, globalColStats, Map(0 -> localColStats))

    val overlappingCubes = overlappingCubeWeights.map(cw => CubeId(2, cw.cubeBytes))
    val overlappingWeights = overlappingCubeWeights.map(_.normalizedWeight).toSet

    val resultCubes = cubes.slice(2, 4)

    overlappingCubes.length shouldBe resultCubes.length

    resultCubes.forall(overlappingCubes.contains) shouldBe true

    overlappingWeights.size shouldBe 1

    overlappingWeights.head shouldBe 1.0 / 0.5
  }
}
