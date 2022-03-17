/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.index

import io.qbeast.core.model.{BroadcastedTableChanges, CubeId, _}
import io.qbeast.spark.index.DoublePassOTreeDataAnalyzer.{
  addRandomWeight,
  calculateRevisionChanges,
  getDataFrameStats
}
import io.qbeast.spark.index.QbeastColumns.{cubeColumnName, weightColumnName}
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{BinaryType, StructField}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import scala.annotation.tailrec

object SequentialDataAnalyzer extends OTreeDataAnalyzer with Serializable {
  val spark: SparkSession = SparkSession.active
  import spark.implicits._

  private[index] def calculateTreeDepth(
      elementCount: Long,
      desiredCubeSize: Long,
      dimensionCount: Int): Int = {
    val securitySurplus = 3

    val maxFanout = math.pow(2, dimensionCount)
    val cubeCount = elementCount / desiredCubeSize

    val perfectTreeHeight = logOfBase(maxFanout, 1 - cubeCount * (1 - maxFanout)) - 1

    perfectTreeHeight.toInt + securitySurplus
  }

  private[index] def logOfBase(base: Double, value: Double): Double = {
    math.log10(value) / math.log10(base)
  }

  private[index] def addCubeId(revision: Revision, treeHeight: Int): DataFrame => DataFrame =
    (df: DataFrame) => {
      val columnsToIndex = revision.columnTransformers.map(_.columnName)
      val pointCubeMapper = new PointCubeMapper(revision, treeHeight)
      df.withColumn(
        cubeColumnName,
        pointCubeMapper.findTargetCubeUDF(struct(columnsToIndex.map(col): _*)))
    }

  val cubeStringToBytes: UserDefinedFunction = {
    udf((levelCube: String, dimensionCount: Int) => CubeId(dimensionCount, levelCube).bytes)
  }

  @tailrec
  private[index] def generateOTreeIndex(
      remainingData: DataFrame,
      desiredCubeSize: Int,
      dimensionCount: Int,
      treeHeight: Int,
      level: Int,
      indexedData: DataFrame,
      cubeWeights: Dataset[(CubeId, NormalizedWeight)] =
        spark.emptyDataset[(CubeId, NormalizedWeight)])
      : (DataFrame, Dataset[(CubeId, NormalizedWeight)]) = {

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

    if (nextIterData.isEmpty || level >= treeHeight) {
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
    val indexedData = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], dfSchema)

    val (outputData, cubeWeightsDS) = generateOTreeIndex(
      dataWithWeightAndCube,
      revision.desiredCubeSize,
      dimensionCount,
      maxOTreeHeight,
      0,
      indexedData)

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

class PointCubeMapper(revision: Revision, targetDepth: Int) extends Serializable {

  val findTargetCubeUDF: UserDefinedFunction = {
    udf((row: Row) => {
      val point = RowUtils.rowValuesToPoint(row, revision)
      CubeId.container(point, targetDepth).string
    })
  }

}
