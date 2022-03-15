package io.qbeast.spark.index

import io.qbeast.core.model._
import io.qbeast.spark.index.DoublePassOTreeDataAnalyzer.{
  addRandomWeight,
  calculateRevisionChanges,
  getDataFrameStats
}
import io.qbeast.spark.index.QbeastColumns.{cubeColumnName, weightColumnName}
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}
import org.apache.spark.sql.functions._
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

  @tailrec
  private[index] def generateOTreeIndex(
      data: DataFrame,
      desiredCubeSize: Int,
      level: Int,
      cubeMaxLen: Double,
      dimensionCount: Int,
      cubeWeights: Dataset[CubeNormalizedWeight],
      processedData: DataFrame): (DataFrame, Dataset[CubeNormalizedWeight]) = {

    if (level == 0 || data.isEmpty) {
      return (processedData, cubeWeights)
    }

    val cubeStringLengthFromLevel: Int = math.pow(2, level).toInt

    val cubeFromLevel: UserDefinedFunction =
      udf((cube: String) => cube.substring(0, cubeStringLengthFromLevel))

    val levelElemWindowSpec = Window.partitionBy(cubeColumnName).orderBy(weightColumnName)
    val levelElems = data
      .withColumn("cubeFromLevel", cubeFromLevel(col(cubeColumnName)))
      .withColumn("rank", rank.over(levelElemWindowSpec))
      .where(s"rank <= $desiredCubeSize")

    val levelCubeWeightsWindowSpec =
      Window.partitionBy("cubeFromLevel").orderBy(desc(weightColumnName))
    val levelCubeWeights = levelElems
      .select("cubeFromLevel", weightColumnName)
      .withColumn("rank", rank.over(levelCubeWeightsWindowSpec))
      .where("rank == 1")
      .map { row =>
        val cubeString = row.getAs[String]("cubeFromLevel")
        val cubeBytes = CubeId(dimensionCount, cubeString).bytes
        val weight = Weight(row.getAs[Double](weightColumnName))
        CubeNormalizedWeight(cubeBytes, NormalizedWeight(weight))
      }

    generateOTreeIndex(
      data,
      desiredCubeSize,
      level - 1,
      cubeMaxLen,
      dimensionCount,
      cubeWeights.union(levelCubeWeights),
      processedData.union(levelElems))
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
    val maxOTreeDepth = calculateTreeDepth(numElements, revision.desiredCubeSize, dimensionCount)
    val cubeMaxLength = (math.pow(2, dimensionCount) * maxOTreeDepth).toInt

    val dataWithWeightAndCube =
      dataFrame.transform(addRandomWeight(revision)).transform(addCubeId(revision, cubeMaxLength))

    val (outputData, _) = generateOTreeIndex(
      dataWithWeightAndCube,
      revision.desiredCubeSize,
      cubeMaxLength,
      cubeMaxLength,
      dimensionCount,
      spark.emptyDataset[CubeNormalizedWeight],
      spark.emptyDataFrame)

    (outputData, BroadcastedTableChanges(None, indexStatus, Map.empty))
  }

}

class PointCubeMapper(revision: Revision, targetDepth: Int) {

  val findTargetCubeUDF: UserDefinedFunction = {
    udf((row: Row) => {
      val point = RowUtils.rowValuesToPoint(row, revision)
      CubeId.container(point, targetDepth).string
    })
  }

}
