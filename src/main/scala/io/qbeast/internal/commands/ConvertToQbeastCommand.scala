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
package io.qbeast.internal.commands

import io.qbeast.context.QbeastContext
import io.qbeast.core.model._
import io.qbeast.spark.utils.MetadataConfig.lastRevisionID
import io.qbeast.spark.utils.MetadataConfig.revision
import io.qbeast.spark.utils.QbeastExceptionMessages.incorrectIdentifierFormat
import io.qbeast.spark.utils.QbeastExceptionMessages.partitionedTableExceptionMsg
import io.qbeast.spark.utils.QbeastExceptionMessages.unsupportedFormatExceptionMsg
import org.apache.hadoop.fs.Path
import org.apache.spark.internal.Logging
import org.apache.spark.qbeast.config.DEFAULT_CUBE_SIZE
import org.apache.spark.qbeast.config.DEFAULT_TABLE_FORMAT
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.execution.command.LeafRunnableCommand
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.AnalysisExceptionFactory
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession

case class ConvertToQbeastCommand(
    identifier: String,
    columnsToIndex: Seq[String],
    cubeSize: Int = DEFAULT_CUBE_SIZE,
    tableOptions: Map[String, String] = Map.empty)
    extends LeafRunnableCommand
    with Logging
    with StagingUtils {

  private def resolveTableFormat(spark: SparkSession): (String, TableIdentifier) = {

    val tableIdentifier =
      try {
        spark.sessionState.sqlParser.parseTableIdentifier(identifier)
      } catch {
        case _: AnalysisException =>
          throw AnalysisExceptionFactory.create(incorrectIdentifierFormat(identifier))
      }
    // If the table is a path table, it is a parquet or delta/qbeast table
    val provider = tableIdentifier.database.getOrElse("")
    val isPathTable = new Path(tableIdentifier.table).isAbsolute
    val isCorrectFormat = provider == "parquet" || provider == "delta" || provider == "qbeast"

    if (isPathTable && isCorrectFormat) (provider, tableIdentifier)
    else if (!isCorrectFormat)
      throw AnalysisExceptionFactory.create(unsupportedFormatExceptionMsg(provider))
    else
      throw AnalysisExceptionFactory.create(incorrectIdentifierFormat(identifier))
  }

  override def run(spark: SparkSession): Seq[Row] = {
    val (fileFormat, tableId) = resolveTableFormat(spark)

    val qTableID = QTableID(tableId.table)
    val qbeastSnapshot = QbeastContext.metadataManager.loadSnapshot(qTableID)
    val isQbeast = qbeastSnapshot.loadAllRevisions.nonEmpty

    if (isQbeast) {
      logInfo("The table you are trying to convert is already a qbeast table")
    } else {
      fileFormat match {
        case "parquet" =>
          DEFAULT_TABLE_FORMAT match {
            case "delta" =>
              try {
                spark.sql(s"CONVERT TO DELTA parquet.${tableId.quotedString}")
              } catch {
                case e: AnalysisException =>
                  val deltaMsg = e.getMessage()
                  throw AnalysisExceptionFactory.create(
                    partitionedTableExceptionMsg +
                      s"Failed to convert the parquet table into delta: $deltaMsg")
              }
            case _ =>
              throw new IllegalArgumentException(
                s"ConvertToQbeastCommand for table format $DEFAULT_TABLE_FORMAT not found")
          }
        case "delta" =>
        case _ => throw AnalysisExceptionFactory.create(unsupportedFormatExceptionMsg(fileFormat))
      }

      // Convert to qbeast through metadata modification
      val schema = qbeastSnapshot.schema

      QbeastContext.metadataManager.updateMetadataWithTransaction(qTableID, schema) {
        val convRevision = stagingRevision(qTableID, cubeSize, columnsToIndex)
        val revisionID = convRevision.revisionID

        // Add staging revision to Revision Map, set it as the latestRevision
        Map(
          lastRevisionID -> revisionID.toString,
          s"$revision.$revisionID" -> mapper.writeValueAsString(convRevision)) ++ tableOptions
      }
    }

    Seq.empty[Row]
  }

}
