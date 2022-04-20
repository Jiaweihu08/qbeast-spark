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

class MaxWeightMapper(levelCubeWeightMap: Map[CubeId, Int]) {

  val cubeWeightUdf: UserDefinedFunction = {
    udf((levelCube: String, dimensionCount: Int) => {
      val cube = CubeId(dimensionCount, levelCube)
      if (levelCubeWeightMap.contains(cube)) {
        levelCubeWeightMap(cube)
      } else {
        Int.MinValue
      }
    })
  }

  def filterPreviousRecords(level: Int, dimensionCount: Int): DataFrame => DataFrame =
    (dataToWrite: DataFrame) => {
      dataToWrite
        .withColumn("levelCube", col(cubeColumnName).substr(0, level))
        .withColumn("maxWeight", cubeWeightUdf(col("levelCube"), lit(dimensionCount)))
        .filter(col(weightColumnName) >= col("maxWeight"))
    }

}

object SequentialWriter extends Serializable {
  lazy val spark: SparkSession = SparkSession.active

  private[index] def piecewiseSequentialOTreeIndexing(
      dataToIndex: DataFrame,
      level: Int,
      desiredCubeSize: Int,
      dimensionCount: Int): (DataFrame, Map[CubeId, Int], DataFrame) = {
    import spark.implicits._

    // DF with cube string corresponding to the current level
    val leveledData = dataToIndex
      .withColumn("levelCube", col(cubeColumnName).substr(0, level))

    // Level cube string and its estimated weight cut. This is used to reduce the number
    // of elements involved in the window rank operation.
    val levelCubeWeightCut = leveledData
      .groupBy("levelCube")
      .count()
      .select(col("levelCube"), lit(desiredCubeSize * 2) / col("count"))
      .map(row => (row.getString(0), Weight(row.getDouble(1)).value))
      .toDF("levelCube", "weightCut")

    // Limiting the data to work on by applying weight cut and repartition
    // the data according to their cube
    val preFiltered = leveledData
      .join(levelCubeWeightCut, "levelCube")
      .filter(col(weightColumnName) <= col("weightCut"))
      .drop("weightCut")
      .repartition(col("levelCube"))

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
      .select(col("levelCube"), col(weightColumnName).as("maxWeight"))
      .where("weightRank == 1")

    val levelCubeWeightMap = cubeWeights
      .map { row =>
        val cube = CubeId(dimensionCount, row.getAs[String]("levelCube"))
        val weight = row.getAs[Int]("maxWeight")
        (cube, weight)
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
      .where(col(weightColumnName) > col("maxWeight"))
      .drop("levelCube")
      .drop("posInPayload")
      .drop("maxWeight")

    // scalastyle:off println
    println(s"level: $level")
    dataToIndex.orderBy(weightColumnName).show(20)
    remainingData.orderBy(weightColumnName).show(20)
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

    // Unfolding the dataset to write level by level and aggregate metadata
    var fileActions = Seq.empty[FileAction]
    var cubeWeights = Map[CubeId, NormalizedWeight]()
    var tableChanges =
      BroadcastedTableChanges(spaceChanges, indexStatus, cubeWeights, Set.empty[CubeId])

    var remainingData = dataToWrite
    var continue = true
    var level = 0

    while (true) {
      val (indexedData, levelCubeWeights, dataToIndex) = piecewiseSequentialOTreeIndexing(
        remainingData,
        level,
        revision.desiredCubeSize,
        dimensionCount)

      tableChanges =
        BroadcastedTableChanges(spaceChanges, indexStatus, cubeWeights, Set.empty[CubeId])

      if (levelCubeWeights.isEmpty) {
        return (tableChanges, fileActions.toIndexedSeq)
      }

      val levelDataEliminator = new MaxWeightMapper(levelCubeWeights)
      val testFiltering = {
        dataToWrite.transform(levelDataEliminator.filterPreviousRecords(level, dimensionCount))
      }

      // scalastyle:off println
      println(s"remaining data count: ${dataToIndex.count()}")
      println(s"testFiltering size: ${testFiltering.count()}")

      cubeWeights ++= levelCubeWeights.mapValues(w => NormalizedWeight(Weight(w)))
      fileActions ++= dataWriter.write(tableID, schema, indexedData, tableChanges)
      remainingData = dataToIndex
      level += 1
      if (level > maxOTreeHeight) {
        continue = false
      }
    }
    (tableChanges, fileActions.toIndexedSeq)
  }

}
