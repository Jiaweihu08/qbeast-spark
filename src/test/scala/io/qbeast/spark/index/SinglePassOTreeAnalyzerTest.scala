package io.qbeast.spark.index

import io.qbeast.IISeq
import io.qbeast.TestClasses._
import io.qbeast.core.model._
import io.qbeast.core.transform.{LinearTransformation, Transformer}
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

  it should "estimatePartitionCubeWeights" in withSpark { spark =>
    val (data, indexStatus) = createDFAndStatus(10000, spark)

    // Add weights to DF to index
    val weightedDataFrame =
      data.withColumn(weightColumnName, lit(scala.util.Random.nextInt()))

    val columnsToIndex = indexStatus.revision.columnTransformers.map(_.columnName)
    val selected = weightedDataFrame
      .select((columnsToIndex :+ weightColumnName).map(col): _*)
      .repartition(5)

    val globalColStatsAcc =
      new ColStatsAccumulator(initializeColStats(columnsToIndex, selected.schema))
    spark.sparkContext.register(globalColStatsAcc, "globalColStatsAcc")

    // Compute partition-level CubeWeights
    val cubeWeightsAndStats = selected
      .transform(
        estimatePartitionCubeWeights(0, globalColStatsAcc, indexStatus, isReplication = false))
      .collect()

    val numPartitions = weightedDataFrame.rdd.getNumPartitions

    cubeWeightsAndStats
      .groupBy(_.cubeBytes)
      .foreach { case (_, weights: Array[CubeWeightAndStats]) =>
        weights.foreach(w => w.normalizedWeight shouldBe >(0.0))
        weights.length shouldBe <=(numPartitions)
      }

    cubeWeightsAndStats.foreach(r => r.colStats.size shouldBe columnsToIndex.size)
  }

  it should "compute global Cube Weights correctly" in withSpark { spark =>
    val (data, indexStatus) = createDFAndStatus(10000, spark)

    // Add weights to DF to index
    val weightedDataFrame =
      data.withColumn(weightColumnName, lit(scala.util.Random.nextInt()))

    val columnsToIndex = indexStatus.revision.columnTransformers.map(_.columnName)
    val selected = weightedDataFrame
      .select(((columnsToIndex :+ weightColumnName)).map(col): _*)

    val globalColStatsAcc =
      new ColStatsAccumulator(initializeColStats(columnsToIndex, selected.schema))
    spark.sparkContext.register(globalColStatsAcc, "globalColStatsAcc")
    // Compute partition-level CubeWeights
    val partitionedEstimatedCubeWeights = selected
      .transform(
        estimatePartitionCubeWeights(0, globalColStatsAcc, indexStatus, isReplication = false))
      .collect()

    // Get global column min/max for all indexing columns and compute global-level CubeWeights
    val globalColStats = globalColStatsAcc.value
    val globalEstimatedCubeWeights =
      toGlobalCubeWeights(partitionedEstimatedCubeWeights, columnsToIndex.size, globalColStats)

    val transformations = getTransformations(globalColStats)

    val allPartitionColStats = partitionedEstimatedCubeWeights.map(_.colStats)
    // Partition-level range should be smaller than global-level range
    allPartitionColStats foreach { partitionColStats =>
      // Make sure the dimensions are right
      partitionColStats.size shouldBe transformations.size

      // Make sure the partition-level min/max ranges are
      // contained in global min/max ranges
      partitionColStats zip transformations foreach {
        case (
              ColStats(_, _, colMin: Double, colMax: Double),
              LinearTransformation(gMin: Double, gMax: Double, _, _)) =>
          colMin shouldBe >=(gMin)
          colMax shouldBe <=(gMax)
        case _ => true
      }
    }

    // For each partition-level cube there can be more than more overlapping cube from
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
    spark.sparkContext.register(globalColStatsAcc, "globalColStatsAcc")
    // Estimate partition-level cube weights
    val partitionedEstimatedCubeWeights =
      selected
        .transform(
          estimatePartitionCubeWeights(0, globalColStatsAcc, indexStatus, isReplication = false))
        .collect()

    // Get global column min/max for all indexing columns and compute global-level CubeWeights
    val globalColStats = globalColStatsAcc.value
    val globalEstimatedCubeWeights =
      toGlobalCubeWeights(partitionedEstimatedCubeWeights, columnsToIndex.size, globalColStats)

    val transformations = getTransformations(globalColStats)
    val lastRevision = indexStatus.revision.copy(transformations = transformations)

    // Compute the overall estimated cube weights
    val estimatedCubeWeights: Map[CubeId, NormalizedWeight] =
      globalEstimatedCubeWeights
        .groupBy(cw => lastRevision.createCubeId(cw.cubeBytes))
        .mapValues(cubeWeights => 1.0 / cubeWeights.map(1.0 / _.normalizedWeight).sum)

    // scalastyle:off println
//    estimatedCubeWeights.toList.sortBy(_._1).foreach(println)
    estimatedCubeWeights.foreach { case (_, weight) =>
      weight shouldBe >(0.0)
    }

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

        val initialColStats = initializeColStats(columnsToIndex, selected.schema)
        val globalColStatsAcc = new ColStatsAccumulator(initialColStats)
        spark.sparkContext.register(globalColStatsAcc, "globalColStatsAcc")

        // Estimate the cube weights at partition level
        val partitionedEstimatedCubeWeights =
          selected
            .transform(estimatePartitionCubeWeights(0, globalColStatsAcc, indexStatus, false))
            .collect()

        val globalColStats = globalColStatsAcc.value
        // Map partition cube weights to global cube weights
        val globalEstimatedCubeWeights =
          toGlobalCubeWeights(
            partitionedEstimatedCubeWeights,
            columnsToIndex.size,
            globalColStats)

        val partitionCubeNormalizedWeights = partitionedEstimatedCubeWeights.map {
          case CubeWeightAndStats(cubeBytes, normalizedWeight, _) =>
            CubeNormalizedWeight(cubeBytes, normalizedWeight)
        }
        partitionCubeNormalizedWeights shouldBe globalEstimatedCubeWeights
    }

  it should "return the same cubeWeights" in withSpark { spark =>
    val cubes = CubeId.root(2).children

    val localColStats = Seq(
      ColStats("x", "DoubleDataType", 0.0, 100.0),
      ColStats("y", "DoubleDataType", 0.0, 100.0))

    val globalColStats = Seq(
      ColStats("x", "DoubleDataType", 0.0, 100.0),
      ColStats("y", "DoubleDataType", 0.0, 100.0))

    val cubeWeightStats = cubes.map(c => CubeWeightAndStats(c.bytes, 1.0, localColStats)).toArray
    val rootOverlap = toGlobalCubeWeights(cubeWeightStats, 2, globalColStats)

    cubeWeightStats.map { case CubeWeightAndStats(bytes, w, _) =>
      CubeNormalizedWeight(bytes, w)
    } shouldBe rootOverlap
  }

  it should "return the correct overlapping cubesWeights" in withSpark { spark => }

}
