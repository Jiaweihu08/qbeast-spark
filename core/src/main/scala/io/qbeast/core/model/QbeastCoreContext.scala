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
package io.qbeast.core.model

import org.apache.spark.sql.types.StructType

/**
 * Qbeast Core main components
 */
trait QbeastCoreContext {
  def metadataManager: MetadataManager
  def dataWriter: DataWriter
  def indexManager: IndexManager
  def revisionBuilder: RevisionFactory
  def columnSelector: ColumnsToIndexSelector
}

/**
 * RevisionFactory
 */
trait RevisionFactory {

  /**
   * Create a new revision for a table with given parameters
   *
   * @param qtableID
   *   the table identifier
   * @param schema
   *   the schema
   * @param options
   *   the options
   * @return
   */
  def createNewRevision(qtableID: QTableID, schema: StructType, options: QbeastOptions): Revision

}
