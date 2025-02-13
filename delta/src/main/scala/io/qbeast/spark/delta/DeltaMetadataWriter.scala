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
package io.qbeast.spark.delta

import io.qbeast.core.model._
import io.qbeast.core.model.PreCommitHook.PreCommitHookOutput
import io.qbeast.core.model.QTableID
import io.qbeast.core.model.QbeastFile
import io.qbeast.core.model.QbeastHookLoader
import io.qbeast.core.model.TableChanges
import io.qbeast.core.model.WriteMode
import io.qbeast.core.model.WriteMode.WriteModeValue
import io.qbeast.spark.utils.QbeastExceptionMessages.partitionedTableExceptionMsg
import io.qbeast.spark.writer.StatsTracker.registerStatsTrackers
import org.apache.spark.internal.Logging
import org.apache.spark.sql.delta.actions._
import org.apache.spark.sql.delta.commands.DeltaCommand
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.DeltaOperations
import org.apache.spark.sql.delta.DeltaOptions
import org.apache.spark.sql.delta.OptimisticTransaction
import org.apache.spark.sql.execution.datasources.BasicWriteJobStatsTracker
import org.apache.spark.sql.execution.datasources.WriteJobStatsTracker
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.AnalysisExceptionFactory
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.SerializableConfiguration

import scala.collection.mutable.ListBuffer

/**
 * DeltaMetadataWriter is in charge of writing data to a table and report the necessary log
 * information
 *
 * @param tableID
 *   the table identifier
 * @param writeMode
 *   SaveMode of the writeMetadata
 * @param deltaLog
 *   deltaLog associated to the table
 * @param qbeastOptions
 *   options for writeMetadata operation
 * @param schema
 *   the schema of the table
 */
private[delta] case class DeltaMetadataWriter(
    tableID: QTableID,
    writeMode: WriteModeValue,
    deltaLog: DeltaLog,
    qbeastOptions: QbeastOptions,
    schema: StructType)
    extends DeltaMetadataOperation
    with DeltaCommand
    with Logging {

  private val deltaOptions = {
    val optionsMap = qbeastOptions.toMap ++ Map("path" -> tableID.id)
    new DeltaOptions(optionsMap, SparkSession.active.sessionState.conf)
  }

  private def isOverwriteOperation: Boolean = writeMode == WriteMode.Overwrite

  private def isOptimizeOperation: Boolean = writeMode == WriteMode.Optimize

  private def saveMode =
    if (!isOverwriteOperation || isOptimizeOperation) SaveMode.Append else SaveMode.Overwrite

  override protected val canMergeSchema: Boolean = deltaOptions.canMergeSchema

  override protected val canOverwriteSchema: Boolean =
    deltaOptions.canOverwriteSchema && isOverwriteOperation && deltaOptions.replaceWhere.isEmpty

  private val sparkSession = SparkSession.active

  /**
   * Creates an instance of basic stats tracker on the desired transaction
   * @param txn
   *   the transaction
   * @return
   */
  private def createStatsTrackers(txn: OptimisticTransaction): Seq[WriteJobStatsTracker] = {
    val statsTrackers: ListBuffer[WriteJobStatsTracker] = ListBuffer()
    // Create basic stats trackers to add metrics on the Write Operation
    val hadoopConf = sparkSession.sessionState.newHadoopConf() // TODO check conf
    val basicWriteJobStatsTracker = new BasicWriteJobStatsTracker(
      new SerializableConfiguration(hadoopConf),
      BasicWriteJobStatsTracker.metrics)
    txn.registerSQLMetrics(sparkSession, basicWriteJobStatsTracker.driverSideMetrics)
    statsTrackers.append(basicWriteJobStatsTracker)
    statsTrackers
  }

  private val preCommitHooks = new ListBuffer[PreCommitHook]()

  // Load the pre-commit hooks
  loadPreCommitHooks().foreach(registerPreCommitHooks)

  /**
   * Register a pre-commit hook
   * @param preCommitHook
   *   the hook to register
   */
  private def registerPreCommitHooks(preCommitHook: PreCommitHook): Unit = {
    if (!preCommitHooks.contains(preCommitHook)) {
      preCommitHooks.append(preCommitHook)
    }
  }

  /**
   * Load the pre-commit hooks from the options
   * @return
   *   the loaded hooks
   */
  private def loadPreCommitHooks(): Seq[PreCommitHook] =
    qbeastOptions.hookInfo.map(QbeastHookLoader.loadHook)

  /**
   * Executes all registered pre-commit hooks.
   *
   * This function iterates over all pre-commit hooks registered in the `preCommitHooks`
   * ArrayBuffer. For each hook, it calls the `run` method of the hook, passing the provided
   * actions as an argument. The `run` method of a hook is expected to return a Map[String,
   * String] which represents the output of the hook. The outputs of all hooks are combined into a
   * single Map[String, String] which is returned as the result of this function.
   *
   * It's important to note that if two or more hooks return a map with the same key, the value of
   * the key in the resulting map will be the value from the last hook that returned that key.
   * This is because the `++` operation on maps in Scala is a right-biased union operation, which
   * means that if there are duplicate keys, the value from the right operand (in this case, the
   * later hook) will overwrite the value from the left operand.
   *
   * Therefore, to avoid unexpected behavior, it's crucial to ensure that the outputs of different
   * hooks have unique keys. If there's a possibility of key overlap, the hooks should be designed
   * to handle this appropriately, for example by prefixing each key with a unique identifier for
   * the hook.
   *
   * @param actions
   *   The actions to be passed to the `run` method of each hook.
   * @return
   *   A Map[String, String] representing the combined outputs of all hooks.
   */
  private def runPreCommitHooks(actions: Seq[QbeastFile]): PreCommitHookOutput = {
    preCommitHooks.foldLeft(Map.empty[String, String]) { (acc, hook) =>
      acc ++ hook.run(actions)
    }
  }

  def writeWithTransaction(
      writer: String => (TableChanges, Seq[IndexFile], Seq[DeleteFile])): Unit = {
    val oldTransactions = deltaLog.unsafeVolatileSnapshot.setTransactions
    // If the transaction was completed before then no operation
    for (txn <- oldTransactions; version <- deltaOptions.txnVersion;
      appId <- deltaOptions.txnAppId) {
      if (txn.appId == appId && version <= txn.version) {
        val message = s"Transaction $version from application $appId is already completed," +
          " the requested write is ignored"
        logWarning(message)
        return
      }
    }
    deltaLog.withNewTransaction(None, Some(deltaLog.update())) { txn =>
      // Register metrics to use in the Commit Info
      val statsTrackers = createStatsTrackers(txn)
      registerStatsTrackers(statsTrackers)

      // Execute write
      val transactionStartTime = txn.txnStartTimeNs.toString
      val (tableChanges, indexFiles, deleteFiles) = writer(transactionStartTime)
      val addFiles = indexFiles.map(DeltaQbeastFileUtils.toAddFile)
      val removeFiles = deleteFiles.map(DeltaQbeastFileUtils.toRemoveFile)

      // Update Qbeast Metadata, e.g., Revision
      var actions =
        updateMetadata(txn, tableChanges, addFiles, removeFiles, qbeastOptions.extraOptions)
      // Set transaction identifier if specified
      for (txnVersion <- deltaOptions.txnVersion; txnAppId <- deltaOptions.txnAppId) {
        actions +:= SetTransaction(txnAppId, txnVersion, Some(System.currentTimeMillis()))
      }

      // Run pre-commit hooks
      val revision = tableChanges.updatedRevision
      val dimensionCount = revision.transformations.length
      val qbeastActions = actions.map(DeltaQbeastFileUtils.fromAction(dimensionCount))
      val tags = runPreCommitHooks(qbeastActions)

      // Commit the information to the DeltaLog
      val op =
        DeltaOperations.Write(
          saveMode,
          None,
          deltaOptions.replaceWhere,
          deltaOptions.userMetadata)
      txn.commit(actions = actions, op = op, tags = tags)
    }
  }

  def updateMetadataWithTransaction(config: => Configuration, overwrite: Boolean): Unit = {
    deltaLog.withNewTransaction(None, Some(deltaLog.update())) { txn =>
      if (txn.metadata.partitionColumns.nonEmpty) {
        throw AnalysisExceptionFactory.create(partitionedTableExceptionMsg)
      }
      val updatedConfig =
        if (overwrite) config
        else
          config.foldLeft(txn.metadata.configuration) { case (accConf, (k, v)) =>
            accConf.updated(k, v)
          }

      val updatedMetadata = txn.metadata.copy(configuration = updatedConfig)
      val op = DeltaOperations.SetTableProperties(config)
      txn.updateMetadata(updatedMetadata)
      txn.commit(Seq.empty, op)
    }
  }

  /**
   * Writes metadata of the table
   *
   * @param txn
   *   transaction to commit
   * @param tableChanges
   *   changes to apply
   * @param addFiles
   *   files to add
   * @param removeFiles
   *   files to remove
   * @param extraConfiguration
   *   extra configuration to apply
   * @return
   *   the sequence of file actions to save in the commit log(add, remove...)
   */
  protected def updateMetadata(
      txn: OptimisticTransaction,
      tableChanges: TableChanges,
      addFiles: Seq[AddFile],
      removeFiles: Seq[RemoveFile],
      extraConfiguration: Configuration): Seq[Action] = {

    val isNewTable = txn.readVersion == -1

    if (!isNewTable) {
      // This table already exists, check if the insert is valid.
      if (saveMode == SaveMode.ErrorIfExists) {
        throw AnalysisExceptionFactory.create(s"Path '${deltaLog.dataPath}' already exists.'")
      } else if (saveMode == SaveMode.Ignore) {
        return Nil
      } else if (saveMode == SaveMode.Overwrite) {
        DeltaLog.assertRemovable(txn.snapshot)
      }
    }
    val rearrangeOnly = isOptimizeOperation

    val (newConfiguration, hasRevisionUpdate) = updateConfiguration(
      txn.metadata.configuration,
      isNewTable,
      isOverwriteOperation,
      tableChanges,
      qbeastOptions)

    // The Metadata can be updated only once in a single transaction
    // If a new space revision or a new replicated set is detected,
    // we update everything in the same operation
    updateTableMetadata(
      txn,
      schema,
      isOverwriteOperation,
      rearrangeOnly,
      newConfiguration,
      hasRevisionUpdate)

    if (isNewTable) deltaLog.createLogDirectory()

    val deletedFiles = if (isOverwriteOperation && !isNewTable) {
      txn.filterFiles().map(_.remove)
    } else removeFiles

    if (rearrangeOnly) {
      addFiles.map(_.copy(dataChange = false)) ++ deletedFiles.map(_.copy(dataChange = false))
    } else addFiles ++ deletedFiles
  }

}
