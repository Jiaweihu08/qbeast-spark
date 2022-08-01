/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.index

import io.qbeast.core.model._
import io.qbeast.spark.index.DoublePassOTreeDataAnalyzer.{
  addRandomWeight,
  calculateRevisionChanges,
  getDataFrameStats
}
import io.qbeast.spark.index.QbeastColumns.{cubeColumnName, weightColumnName}
import io.qbeast.spark.index.SequentialUtils.{addCubeId, calculateTreeDepth, cubeStringToBytes}
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{BinaryType, StructField}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import scala.annotation.tailrec

object SequentialDataAnalyzer extends OTreeDataAnalyzer with Serializable {
  lazy val spark: SparkSession = SparkSession.active

  @tailrec
  private[index] def generateOTreeIndex(
      remainingData: DataFrame,
      desiredCubeSize: Int,
      dimensionCount: Int,
      treeHeight: Int,
      level: Int,
      indexedData: DataFrame,
      cubeWeights: Dataset[(CubeId, NormalizedWeight)])
      : (DataFrame, Dataset[(CubeId, NormalizedWeight)]) = {
    import spark.implicits._

    val levelCubeString: UserDefinedFunction =
      udf((cube: String) => cube.substring(0, level))

    val levelElemWindowSpec = Window.partitionBy("levelCube").orderBy(weightColumnName)
    val dataWithPositionInPayload = remainingData
      .withColumn("levelCube", levelCubeString(col(cubeColumnName)))
      .withColumn("elemPosInPayload", rank.over(levelElemWindowSpec))

    val levelElems = dataWithPositionInPayload
      .where(s"elemPosInPayload <= $desiredCubeSize")
      .drop(cubeColumnName)
      .drop("elemPosInPayload")

    val levelCubeWeightsWindowSpec =
      Window.partitionBy("levelCube").orderBy(desc(weightColumnName))
    val levelCubeWeights = levelElems
      .select("levelCube", weightColumnName)
      .withColumn("weightRank", rank.over(levelCubeWeightsWindowSpec))
      .where("weightRank == 1")
      .map { row =>
        val cube = CubeId(dimensionCount, row.getAs[String]("levelCube"))
        val weight = Weight(row.getAs[Int](weightColumnName))
        (cube, NormalizedWeight(weight))
      }

    val levelElemsWithCubeBytes = levelElems
      .withColumn(cubeColumnName, cubeStringToBytes(col("levelCube"), lit(dimensionCount)))
      .drop("levelCube")
      .drop(weightColumnName)

    val nextIndexedData = indexedData.union(levelElemsWithCubeBytes)
    val nextCubeWeights = cubeWeights.union(levelCubeWeights)

    val nextIterData =
      dataWithPositionInPayload
        .where(s"elemPosInPayload > $desiredCubeSize")
        .drop("levelCube")
        .drop("elemPosInPayload")

    if (level == treeHeight) {
      (nextIndexedData, nextCubeWeights)
    } else {
      generateOTreeIndex(
        nextIterData,
        desiredCubeSize,
        dimensionCount,
        treeHeight,
        level + 1,
        nextIndexedData,
        nextCubeWeights)
    }
  }

  /**
   * Analyze the data to process
   *
   * @param dataFrame     the data to index
   * @param indexStatus   the current status of the index
   * @param isReplication either we are replicating the elements or not
   * @return the changes to the index
   */
  override def analyze(
      dataFrame: DataFrame,
      indexStatus: IndexStatus,
      isReplication: Boolean): (DataFrame, TableChanges) = {
    import spark.implicits._

    val columnTransformers = indexStatus.revision.columnTransformers
    val dataFrameStats = getDataFrameStats(dataFrame, columnTransformers)

    val numElements = dataFrameStats.getAs[Long]("count")
    if (numElements == 0) {
      throw new RuntimeException(
        "The DataFrame is empty, why are you trying to index an empty dataset?")
    }

    val spaceChanges =
      if (isReplication) None
      else calculateRevisionChanges(dataFrameStats, indexStatus.revision)

    // The revision to use
    val revision = spaceChanges match {
      case Some(revisionChange) =>
        revisionChange.createNewRevision
      case None => indexStatus.revision
    }

    val dimensionCount = revision.columnTransformers.size
    val maxOTreeHeight = calculateTreeDepth(numElements, revision.desiredCubeSize, dimensionCount)

    val dataWithWeightAndCube =
      dataFrame
        .transform(addRandomWeight(revision))
        .transform(addCubeId(revision, maxOTreeHeight))
    val dfSchema = dataFrame.schema.add(StructField(cubeColumnName, BinaryType, nullable = true))

    val (outputData, cubeWeightsDS) = generateOTreeIndex(
      dataWithWeightAndCube,
      revision.desiredCubeSize,
      dimensionCount,
      maxOTreeHeight,
      0,
      spark.createDataFrame(spark.sparkContext.emptyRDD[Row], dfSchema),
      spark.emptyDataset[(CubeId, NormalizedWeight)])

    val cubeWeights = cubeWeightsDS.collect().toMap

    val tableChanges = BroadcastedTableChanges(
      spaceChanges,
      indexStatus,
      cubeWeights,
      if (isReplication) indexStatus.cubesToOptimize
      else Set.empty[CubeId])

    (outputData, tableChanges)
  }

}
