package io.qbeast.spark.index

import io.qbeast.IISeq
import io.qbeast.TestClasses._
import io.qbeast.core.model.{IndexStatus, QTableID, Revision}
import io.qbeast.core.transform.{LinearTransformation, Transformer}
import io.qbeast.spark.QbeastIntegrationTestSpec
import io.qbeast.spark.index.QbeastColumns.weightColumnName
import io.qbeast.spark.index.SinglePassColStatsUtils.initializeColStats
import io.qbeast.spark.index.SinglePassOTreeDataAnalyzer.{
  estimatePartitionCubeWeights,
  toGlobalCubeWeights
}
import io.qbeast.spark.utils.SparkToQTypesUtils
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.{Dataset, SparkSession}

class SinglePassOTreeAnalyzerTest extends QbeastIntegrationTestSpec {

  private def createDF(size: Int, spark: SparkSession): Dataset[T3] = {
    import spark.implicits._

    0.to(size)
      .map(i => T3(i, i.toDouble, i.toString, i.toFloat))
      .toDF()
      .as[T3]

  }

  private def createTransformers(columnsSchema: Seq[StructField]): IISeq[Transformer] = {
    columnsSchema
      .map(field => Transformer(field.name, SparkToQTypesUtils.convertDataTypes(field.dataType)))
      .toIndexedSeq
  }

  it should "estimatePartitionCubeWeights" in withSpark { spark =>
    val data = createDF(10000, spark)
    val columnsSchema = data.schema
    val columnTransformers = createTransformers(columnsSchema)

    val revision =
      Revision(0, 1000, QTableID("test"), 1000, columnTransformers, Seq.empty.toIndexedSeq)
    val indexStatus = IndexStatus(revision, Set.empty)

    val weightedDataFrame =
      data.withColumn(weightColumnName, lit(scala.util.Random.nextInt()))

    val columnsToIndex = indexStatus.revision.columnTransformers.map(_.columnName)
    val cols: Seq[String] = columnsToIndex :+ weightColumnName
    val selected = weightedDataFrame.select(cols.map(col): _*)

    val initialColStats = initializeColStats(columnsToIndex, selected.schema)
    val colStatsAcc = new ColStatsAccumulator(initialColStats)
    spark.sparkContext.register(colStatsAcc, "globalColStatsAcc")

    val cubeWeightsAndStats: Dataset[CubeWeightAndStats] = selected.transform(
      estimatePartitionCubeWeights(
        0,
        revision,
        indexStatus,
        isReplication = false,
        initialColStats,
        colStatsAcc))

    val partitions = weightedDataFrame.rdd.getNumPartitions

    val results = cubeWeightsAndStats.collect()

    // scalastyle:off println
//    data.agg(min("c"), max("c")).show()
    colStatsAcc.value.foreach(println)

    results
      .groupBy(_.cubeBytes)
      .foreach { case (_, weights: Array[CubeWeightAndStats]) =>
        weights.foreach(w => w.normalizedWeight shouldBe >(0.0))
        weights.length shouldBe <=(partitions)
      }

    results.foreach(r => r.colStats.size shouldBe columnTransformers.size)
  }

  it should "toGlobalCubeWeights" in withSpark { spark =>
    val data = createDF(10000, spark)
    val columnsSchema = data.schema
    val columnTransformers = createTransformers(columnsSchema)

    val revision =
      Revision(0, 1000, QTableID("test"), 1000, columnTransformers, Seq.empty.toIndexedSeq)
    val indexStatus = IndexStatus(revision, Set.empty)

    val weightedDataFrame =
      data.withColumn(weightColumnName, lit(scala.util.Random.nextInt()))

    val columnsToIndex = indexStatus.revision.columnTransformers.map(_.columnName)
    val cols: Seq[String] = columnsToIndex :+ weightColumnName
    val selected = weightedDataFrame.select(cols.map(col): _*)

    val initialColStats = initializeColStats(columnsToIndex, selected.schema)
    val colStatsAcc = new ColStatsAccumulator(initialColStats)
    spark.sparkContext.register(colStatsAcc, "globalColStatsAcc")

    val cubeWeightsAndStats: Dataset[CubeWeightAndStats] = selected.transform(
      estimatePartitionCubeWeights(
        0,
        revision,
        indexStatus,
        isReplication = false,
        initialColStats,
        colStatsAcc))

    val (globalCubeWeights, globalTransformations) =
      toGlobalCubeWeights(cubeWeightsAndStats, colStatsAcc.value, revision)

    val allPartitionColStats = cubeWeightsAndStats.collect().map(_.colStats)

    // Partition-level range should be smaller than global-level range
    allPartitionColStats foreach { partitionColStats =>
      partitionColStats.size shouldBe globalTransformations.size

      partitionColStats zip globalTransformations foreach {
        case (
              ColStats(_, _, colMin: Double, colMax: Double),
              LinearTransformation(gMin: Double, gMax: Double, _)) =>
          colMin shouldBe >=(gMin)
          colMax shouldBe <=(gMax)
        case _ => true
      }
    }

    // For each partition-level cube there can be more than more overlapping cube from
    // the global space
    cubeWeightsAndStats.count() <= globalCubeWeights.count()
  }
//
//  it should "estimateCubeWeights" in withSpark { spark =>
//    val data = createDF(100000, spark)
//    val columnsSchema = data.schema
//    val columnTransformers = createTransformers(columnsSchema)
//
//    val revision =
//      Revision(0, 1000, QTableID("test"), 1000, columnTransformers, Seq.empty.toIndexedSeq)
//
//    val indexStatus = IndexStatus(revision, Set.empty)
//    val weightedDataFrame =
//      data.withColumn(weightColumnName, lit(scala.util.Random.nextInt()))
//
//    val (cubeWeightsAndStats: Dataset[CubeWeightAndStats], globalColStats: Seq[ColStats]) =
//      estimatePartitionCubeWeights(
//        weightedDataFrame,
//        0,
//        revision,
//        indexStatus,
//        isReplication = false)
//
//    val (globalEstimatedCubeWeights, globalTransformations) =
//      toGlobalCubeWeights(cubeWeightsAndStats, globalColStats, revision)
//
//    val estimatedCubeWeights =
//      globalEstimatedCubeWeights
//        .transform(estimateCubeWeights(revision.copy(transformations = globalTransformations)))
//
//    estimatedCubeWeights.columns.length shouldBe 2
//
//    val cubeWeightsCollect = estimatedCubeWeights.collect()
//
//    cubeWeightsCollect.map(_._1).distinct.length shouldBe cubeWeightsCollect.length
//
//    cubeWeightsCollect.foreach { case (_, weight) =>
//      weight shouldBe >(0.0)
//    }
//
//  }

}
