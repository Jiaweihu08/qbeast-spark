/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.index

import io.qbeast.IISeq
import io.qbeast.core.model._
import org.apache.spark.sql.{AnalysisExceptionFactory, DataFrame}

/**
 * Implementation of OTreeAlgorithm.
 */
object SparkOTreeManager extends IndexManager[DataFrame] with Serializable {

  /**
   * Builds an OTree index.
   * @param data the data to index
   * @param indexStatus the current status of the index
   * @return the changes to the index
   */
  override def index(
      data: DataFrame,
      indexStatus: IndexStatus,
      analyzerImp: String): (DataFrame, TableChanges) =
    index(data, indexStatus, isReplication = false, analyzerImp = "double")

  /**
   * Optimizes the index
   * @param data the data to optimize
   * @param indexStatus the current status of the index
   * @return the changes to the index
   */
  override def optimize(
      data: DataFrame,
      indexStatus: IndexStatus,
      analyzerImp: String): (DataFrame, TableChanges) =
    index(data, indexStatus, isReplication = true, analyzerImp)

  /**
   * Analyzes the index
   * @param indexStatus the current status of the index
   * @return the cubes to optimize
   */
  override def analyze(indexStatus: IndexStatus): IISeq[CubeId] = {
    findCubesToOptimize(indexStatus)
  }

  // PRIVATE METHODS //

  private def findCubesToOptimize(indexStatus: IndexStatus): IISeq[CubeId] = {
    val overflowedSet = indexStatus.overflowedSet
    val replicatedSet = indexStatus.replicatedSet

    val cubesToOptimize = overflowedSet
      .filter(cube => {
        !replicatedSet.contains(cube) && (cube.parent match {
          case None => true
          case Some(p) => replicatedSet.contains(p)
        })
      })

    if (cubesToOptimize.isEmpty && replicatedSet.isEmpty) {
      Seq(indexStatus.revision.createCubeIdRoot()).toIndexedSeq
    } else cubesToOptimize.toIndexedSeq
  }

  private def index(
      dataFrame: DataFrame,
      indexStatus: IndexStatus,
      isReplication: Boolean,
      analyzerImp: String): (DataFrame, TableChanges) = analyzerImp match {
    case "sequential" => SequentialDataAnalyzer.analyze(dataFrame, indexStatus, isReplication)
    case "double" =>
      // Analyze the data and compute weight and estimated weight map of the result
      val (weightedDataFrame, tc) =
        DoublePassOTreeDataAnalyzer.analyze(dataFrame, indexStatus, isReplication)

      val pointWeightIndexer = new SparkPointWeightIndexer(tc, isReplication)

      // Add cube and state information to the dataframe
      val indexedDataFrame =
        weightedDataFrame.transform(pointWeightIndexer.buildIndex)

      (indexedDataFrame, tc)
    case _ =>
      throw AnalysisExceptionFactory.create(
        s"Analyzer Implementation: $analyzerImp not supported.")
  }

}
