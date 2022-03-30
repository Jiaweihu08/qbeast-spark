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
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}
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

    val levelCubeString: UserDefinedFunction =
      udf((cube: String) => cube.substring(0, level))

    val levelElemWindowSpec = Window.partitionBy("levelCube").orderBy(weightColumnName)
    val dataWithPositionInPayload = dataToIndex
      .withColumn("levelCube", levelCubeString(col(cubeColumnName)))
      .withColumn("elemPosInPayload", rank.over(levelElemWindowSpec))
//      .queryExecution
//      .executedPlan
//      .execute

    val levelElems = dataWithPositionInPayload
      .where(s"elemPosInPayload <= $desiredCubeSize")

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
      .collect()
      .toMap

    // scalastyle:off println
    println(s"Level($level) cube weight map: $levelCubeWeights")

    val indexedData = levelElems
      .drop(cubeColumnName)
      .withColumn(cubeColumnName, cubeStringToBytes(col("levelCube"), lit(dimensionCount)))
      .drop("levelCube")
      .drop("elemPosInPayload")

    val remainingData =
      dataWithPositionInPayload
        .where(s"elemPosInPayload > $desiredCubeSize")
        .drop("levelCube")
        .drop("elemPosInPayload")
    (indexedData, levelCubeWeights, remainingData)
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
