/*
 * Copyright 2021 Qbeast Analytics, S.L.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.qbeast.spark.index

import io.qbeast.core.model._
import org.apache.spark.internal.Logging
import org.apache.spark.sql.DataFrame

/**
 * Implementation of OTreeAlgorithm.
 */
object SparkOTreeManager extends IndexManager with Serializable with Logging {

  /**
   * Builds an OTree index.
   * @param data
   *   the data to index
   * @param indexStatus
   *   the current status of the index
   * @return
   *   the changes to the index
   */
  override def index(data: DataFrame, indexStatus: IndexStatus): (DataFrame, TableChanges) = {
    // If the DataFrame is empty, we return an empty table changes
    if (data.isEmpty) {
      logInfo("Indexing empty Dataframe. Returning empty table changes.")
      val emptyTableChanges =
        BroadcastTableChanges(
          None,
          indexStatus,
          Map.empty,
          Map.empty,
          isOptimizeOperation = false)
      return (data, emptyTableChanges)
    }
    // Add weight column, analyze the data, compute cube domains, and estimate cube max weights
    val (weightedDataFrame, tc) = DoublePassOTreeDataAnalyzer.analyzeAppend(data, indexStatus)
    // Add cube information to the dataframe. The following will not be executed until an action is called
    val pointWeightIndexer = new SparkPointWeightIndexer(tc)
    val indexedDataFrame = weightedDataFrame.transform(pointWeightIndexer.buildIndex)
    (indexedDataFrame, tc)
  }

}
