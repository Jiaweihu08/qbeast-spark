/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.index

import io.qbeast.IISeq
import io.qbeast.core.model._
import io.qbeast.spark.index.DoublePassOTreeDataAnalyzer.{
  addRandomWeight,
  calculateRevisionChanges,
  getDataFrameStats
}
import io.qbeast.spark.index.QbeastColumns.{cubeColumnName, weightColumnName}
import io.qbeast.spark.index.SequentialDataAnalyzer.{
  addCubeId,
  calculateTreeDepth,
  cubeStringToBytes
}
import org.apache.spark.sql.delta.actions.FileAction
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

object SequentialWriter extends Serializable {
  lazy val spark: SparkSession = SparkSession.active

  private[index] def piecewiseSequentialOTreeIndexing(
      dataToIndex: DataFrame,
      level: Int,
      desiredCubeSize: Int,
      dimensionCount: Int): (DataFrame, Map[CubeId, NormalizedWeight], DataFrame) = {
    import spark.implicits._

    // DF with cube string corresponding to the current level
    val leveledData = dataToIndex
      .withColumn("levelCube", col(cubeColumnName).substr(0, level))

    // Level cube string and its estimated weight cut. This is used to reduce the number
    // of elements involved in the window rank operation.
    val levelCubeWeightCut = leveledData
      .groupBy("levelCube")
      .count()
      .select(col("levelCube"), lit(desiredCubeSize * 1.1) / col("count"))
      .map(row => (row.getString(0), Weight(row.getDouble(1)).value))
      .toDF("levelCube", "weightCut")

    // Limiting the data to work on by applying weight cut.
    val preFiltered = leveledData
      .join(levelCubeWeightCut, "levelCube")
      .filter(col(weightColumnName) <= col("weightCut"))
      .drop("weightCut")

    // Selecting the number of elements for the current level.
    val levelElemWindowSpec = Window.partitionBy("levelCube").orderBy(weightColumnName)
    val levelElems = preFiltered
      .withColumn("posInPayload", rank.over(levelElemWindowSpec))
      .where(s"posInPayload <= $desiredCubeSize")

    // Computing level cube weights
    val levelCubeWeightsWindowSpec =
      Window.partitionBy("levelCube").orderBy(desc(weightColumnName))
    val cubeWeights = levelElems
      .withColumn("weightRank", rank.over(levelCubeWeightsWindowSpec))
      .select(col("levelCube"), col(weightColumnName).as("weightCut"))
      .where("weightRank == 1")

    val levelCubeWeightMap = cubeWeights
      .map { row =>
        val cube = CubeId(dimensionCount, row.getAs[String]("levelCube"))
        val weight = Weight(row.getAs[Int]("weightCut"))
        (cube, NormalizedWeight(weight))
      }
      .collect()
      .toMap

    // Data to be written
    val indexedData = levelElems
      .drop(cubeColumnName)
      .withColumn(cubeColumnName, cubeStringToBytes(col("levelCube"), lit(dimensionCount)))
      .drop("levelCube")
      .drop("posInPayload")

    // Data for the next iteration
    val remainingData = leveledData
      .join(cubeWeights, "levelCube")
      .where(col(weightColumnName) > col("weightCut"))
      .drop("levelCube")
      .drop("posInPayload")
      .drop("weightCut")

    (indexedData, levelCubeWeightMap, remainingData)
  }

  def piecewiseSequentialWriting(
      dataWriter: DataWriter[DataFrame, StructType, FileAction],
      schema: StructType,
      tableID: QTableID,
      dataFrame: DataFrame,
      indexStatus: IndexStatus): (TableChanges, IISeq[FileAction]) = {

    val columnTransformers = indexStatus.revision.columnTransformers
    val dataFrameStats = getDataFrameStats(dataFrame, columnTransformers)

    val numElements = dataFrameStats.getAs[Long]("count")
    if (numElements == 0) {
      throw new RuntimeException(
        "The DataFrame is empty, why are you trying to index an empty dataset?")
    }

    val spaceChanges = calculateRevisionChanges(dataFrameStats, indexStatus.revision)

    // The revision to use
    val revision = spaceChanges match {
      case Some(revisionChange) =>
        revisionChange.createNewRevision
      case None => indexStatus.revision
    }

    val dimensionCount = revision.columnTransformers.size
    val maxOTreeHeight = calculateTreeDepth(numElements, revision.desiredCubeSize, dimensionCount)

    val dataToWrite =
      dataFrame
        .transform(addRandomWeight(revision))
        .transform(addCubeId(revision, maxOTreeHeight))

    var fileActions = Seq.empty[FileAction]
    var cubeWeights = Map[CubeId, NormalizedWeight]()
    (0 until maxOTreeHeight).foldLeft(dataToWrite) { case (remainingData, level) =>
      val (indexedData, levelCubeWeights, dataToIndex) = piecewiseSequentialOTreeIndexing(
        remainingData,
        level,
        revision.desiredCubeSize,
        dimensionCount)

      if (levelCubeWeights.isEmpty) {
        val tableChanges =
          BroadcastedTableChanges(spaceChanges, indexStatus, cubeWeights, Set.empty[CubeId])
        return (tableChanges, fileActions.toIndexedSeq)
      }

      val tableChanges =
        BroadcastedTableChanges(spaceChanges, indexStatus, levelCubeWeights, Set.empty[CubeId])

      cubeWeights ++= levelCubeWeights
      fileActions ++= dataWriter.write(tableID, schema, indexedData, tableChanges)
      dataToIndex
    }
    (
      BroadcastedTableChanges(spaceChanges, indexStatus, cubeWeights, Set.empty[CubeId]),
      fileActions.toIndexedSeq)
  }

}
