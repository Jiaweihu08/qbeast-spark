package io.qbeast.spark.index

import io.qbeast.IISeq
import io.qbeast.TestClasses._
import io.qbeast.core.model.{IndexStatus, QTableID, Revision}
import io.qbeast.core.transform.{LinearTransformation, Transformer}
import io.qbeast.spark.QbeastIntegrationTestSpec
import io.qbeast.spark.index.QbeastColumns.weightColumnName
import io.qbeast.spark.index.SinglePassColStatsUtils.{getTransformations, mergeColStats}
import io.qbeast.spark.index.SinglePassOTreeDataAnalyzer.{
  computeEstimatedCubeWeights,
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

    // Add weights to DF to index
    val weightedDataFrame =
      data.withColumn(weightColumnName, lit(scala.util.Random.nextInt()))

    val columnsToIndex = indexStatus.revision.columnTransformers.map(_.columnName)

    // Compute partition-level CubeWeights
    val cubeWeightsAndStats = weightedDataFrame
      .transform(
        estimatePartitionCubeWeights(
          0,
          columnsToIndex,
          revision,
          indexStatus,
          isReplication = false))
      .collect()

    val numPartitions = weightedDataFrame.rdd.getNumPartitions

    cubeWeightsAndStats
      .groupBy(_.cubeBytes)
      .foreach { case (_, weights: Array[CubeWeightAndStats]) =>
        weights.foreach(w => w.normalizedWeight shouldBe >(0.0))
        weights.length shouldBe <=(numPartitions)
      }

    cubeWeightsAndStats.foreach(r => r.colStats.size shouldBe columnTransformers.size)
  }

  it should "compute global Cube Weights correctly" in withSpark { spark =>
    val data = createDF(10000, spark)
    val columnsSchema = data.schema
    val columnTransformers = createTransformers(columnsSchema)

    val revision =
      Revision(0, 1000, QTableID("test"), 1000, columnTransformers, Seq.empty.toIndexedSeq)
    val indexStatus = IndexStatus(revision, Set.empty)

    // Add weights to DF to index
    val weightedDataFrame =
      data.withColumn(weightColumnName, lit(scala.util.Random.nextInt()))

    val columnsToIndex = indexStatus.revision.columnTransformers.map(_.columnName)

    // Compute partition-level CubeWeights
    val partitionedEstimatedCubeWeights = weightedDataFrame
      .transform(
        estimatePartitionCubeWeights(
          0,
          columnsToIndex,
          revision,
          indexStatus,
          isReplication = false))
      .collect()

    // Compute global column min/max for all indexing columns
    val globalColStats = partitionedEstimatedCubeWeights.reduce {
      (cwL: CubeWeightAndStats, cwR: CubeWeightAndStats) =>
        val updatedColStats = cwL.colStats.zip(cwR.colStats).map { case (statsL, statsR) =>
          mergeColStats(statsL, statsR)
        }
        cwL.copy(colStats = updatedColStats)
    }.colStats

    // Compute global-level CubeWeights
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
              LinearTransformation(gMin: Double, gMax: Double, _)) =>
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
    val data = createDF(10000, spark)
    val columnsSchema = data.schema
    val columnTransformers = createTransformers(columnsSchema)

    val revision =
      Revision(0, 1000, QTableID("test"), 1000, columnTransformers, Seq.empty.toIndexedSeq)
    val indexStatus = IndexStatus(revision, Set.empty)

    // Add weights to DF to index
    val weightedDataFrame =
      data.withColumn(weightColumnName, lit(scala.util.Random.nextInt()))

    val columnsToIndex = indexStatus.revision.columnTransformers.map(_.columnName)

    // Compute partition-level CubeWeights
    val partitionedEstimatedCubeWeights = weightedDataFrame
      .transform(
        estimatePartitionCubeWeights(
          0,
          columnsToIndex,
          revision,
          indexStatus,
          isReplication = false))
      .collect()

    // Compute global column min/max for all indexing columns
    val globalColStats = partitionedEstimatedCubeWeights.reduce {
      (cwL: CubeWeightAndStats, cwR: CubeWeightAndStats) =>
        val updatedColStats = cwL.colStats.zip(cwR.colStats).map { case (statsL, statsR) =>
          mergeColStats(statsL, statsR)
        }
        cwL.copy(colStats = updatedColStats)
    }.colStats

    // Compute global-level CubeWeights
    val globalEstimatedCubeWeights =
      toGlobalCubeWeights(partitionedEstimatedCubeWeights, columnsToIndex.size, globalColStats)

    val transformations = getTransformations(globalColStats)
    val lastRevision = indexStatus.revision.copy(transformations = transformations)

    // Aggregate global-level CubeWeights
    val estimatedCubeWeights =
      computeEstimatedCubeWeights(globalEstimatedCubeWeights, lastRevision)

    // scalastyle:off println
    estimatedCubeWeights.foreach(println)
    estimatedCubeWeights.foreach { case (_, weight) =>
      weight shouldBe >(0.0)
    }

  }

}
