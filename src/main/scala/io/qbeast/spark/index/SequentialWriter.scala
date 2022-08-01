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
import io.qbeast.spark.index.SequentialUtils.{addCubeId, calculateTreeDepth, cubeStringToBytes}
import org.apache.spark.sql.delta.actions.FileAction
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

object SequentialWriter extends Serializable {
  lazy val spark: SparkSession = SparkSession.active
  val levelCubeStringName = "levelCubeString"

  private[index] def computeLevelCandidates(
      levelStringLength: Int,
      desiredCubeSize: Int): DataFrame => DataFrame = (data: DataFrame) => {
    import spark.implicits._

    // Add column to represent which cube from the current level each row belongs to
    val df =
      data.withColumn(levelCubeStringName, col(cubeColumnName).substr(0, levelStringLength))

    // Find weight cut to reduce the number of records to work on
    val levelWeightCut = df
      .groupBy(levelCubeStringName)
      .count()
      .select(col(levelCubeStringName), lit(desiredCubeSize * 2.0) / col("count"))
      .map(row => (row.getString(0), Weight(row.getDouble(1)).value))
      .toDF(levelCubeStringName, "weightCut")

    // Filter records with larger weights
    df.join(levelWeightCut, levelCubeStringName)
      .filter(col(weightColumnName) <= col("weightCut"))
      .drop("weightCut")
      .repartition(col(levelCubeStringName))
  }

  private[index] def piecewiseSequentialOTreeIndexing(
      dataToIndex: DataFrame,
      levelStringSize: Int,
      desiredCubeSize: Int,
      dimensionCount: Int): (DataFrame, Map[String, Int]) = {
    import spark.implicits._

    // Filter candidates for the current level using weight cut
    val levelCandidates =
      dataToIndex.transform(computeLevelCandidates(levelStringSize, desiredCubeSize))

    // Select elements for the current level
    val levelElemWindowSpec = Window.partitionBy(levelCubeStringName).orderBy(weightColumnName)
    val levelElems = levelCandidates
      .withColumn("posInPayload", rank.over(levelElemWindowSpec))
      .where(s"posInPayload <= $desiredCubeSize")

    // Compute level cube weights
    val levelCubeWeightsWindowSpec =
      Window.partitionBy(levelCubeStringName).orderBy(desc(weightColumnName))
    val cubeWeightMap = levelElems
      .withColumn("weightRank", rank.over(levelCubeWeightsWindowSpec))
      .select(col(levelCubeStringName), col(weightColumnName).as("maxWeight"))
      .where("weightRank == 1")
      .map { row =>
        val cube = row.getAs[String](levelCubeStringName)
        val weight = row.getAs[Int]("maxWeight")
        (cube, weight)
      }
      .collect()
      .toMap

    // Data to be written
    val indexedData = levelElems
      .drop(cubeColumnName)
      .withColumn(
        cubeColumnName,
        cubeStringToBytes(col(levelCubeStringName), lit(dimensionCount)))
      .drop(levelCubeStringName)
      .drop("posInPayload")

    (indexedData, cubeWeightMap)
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

    val levelStringStepSize = revision.createCubeIdRoot().children.next.string.length
    var remainingData = dataToWrite
    var continue = true
    var level = 0

    while (continue) {
      remainingData.cache()

      val (indexedData, levelCubeWeights) = piecewiseSequentialOTreeIndexing(
        remainingData,
        level * levelStringStepSize,
        revision.desiredCubeSize,
        dimensionCount)

      // If no cubeWeights is returned, it means that the input remainingData is empty
      // Process finished
      if (levelCubeWeights.isEmpty) {
        continue = false
      } else {
        // Add levelCubeWeights and write the level cubes
        cubeWeights ++= levelCubeWeights.map { case (cubeString, w) =>
          (CubeId(dimensionCount, cubeString), NormalizedWeight(Weight(w)))
        }
        tableChanges =
          BroadcastedTableChanges(spaceChanges, indexStatus, cubeWeights, Set.empty[CubeId])

        // Writing level cubes
        fileActions ++= dataWriter.write(tableID, schema, indexedData, tableChanges)

        // Filter dataToWrite to get the remainingData i.e. elements to be placed in the subtree
        val subtreeExtractor = new SubtreeExtractor(levelCubeWeights)
        remainingData = dataToWrite.transform(
          subtreeExtractor.filterPreviousRecords(level * levelStringStepSize, dimensionCount))

        level += 1
      }
    }
    (tableChanges, fileActions.toIndexedSeq)
  }

}

class SubtreeExtractor(levelCubeWeights: Map[String, Int]) extends Serializable {
  val levelCubeStringName = "levelCubeString"

  val isFromCurrentIter: UserDefinedFunction = {
    udf((levelCubeString: String) => levelCubeWeights.contains(levelCubeString))
  }

  val levelWeight: UserDefinedFunction = {
    udf((levelCubeString: String) => {
      levelCubeWeights(levelCubeString)
    })
  }

  def filterPreviousRecords(
      levelCubeStringLength: Int,
      dimensionCount: Int): DataFrame => DataFrame =
    (dataToWrite: DataFrame) => {
      dataToWrite
        .withColumn(
          levelCubeStringName,
          col(cubeColumnName).substr(0, levelCubeStringLength)
        ) // Map rows to cubes from the current level
        .filter(
          isFromCurrentIter(col(levelCubeStringName))
        ) // Required by Spark, actually not necessary for the logic
        .withColumn("maxWeight", levelWeight(col(levelCubeStringName)))
        .filter(
          col(weightColumnName) > col("maxWeight")
        ) // Elements from the subtree have weights > maxWeight
        .drop(levelCubeStringName)
        .drop("maxWeight")
    }

}
