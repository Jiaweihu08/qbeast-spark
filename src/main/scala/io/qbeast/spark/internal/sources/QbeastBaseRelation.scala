/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.internal.sources

import io.qbeast.core.model.{QTableID}
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.sources.InsertableRelation

import org.apache.spark.sql.{SQLContext}
import org.apache.spark.sql.types.{StructType, StructField}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import io.qbeast.spark.delta.OTreeIndex
import org.apache.spark.sql.execution.datasources.HadoopFsRelation
import io.qbeast.spark.table.IndexedTable
import io.qbeast.context.QbeastContext
import org.apache.hadoop.fs.{Path}
import org.apache.spark.sql.catalyst.catalog.{BucketSpec}
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat

/**
 * Companion object for QbeastBaseRelation
 */
object QbeastBaseRelation {

  /**
   * Creates a QbeastBaseRelation instance
   * @param tableID the identifier of the table
   * @return the QbeastBaseRelation
   */

  /**
   * Returns a HadoopFsRelation that contains all of the data present
   * in the table. This relation will be continually updated
   * as files are added or removed from the table. However, new HadoopFsRelation
   * must be requested in order to see changes to the schema.
   * @param tableID the identifier of the table
   * @param sqlContext the SQLContext
   * @return the HadoopFsRelation
   */
  def createRelation(sqlContext: SQLContext, table: IndexedTable): BaseRelation = {

    val spark = SparkSession.active
    val tableID = table.tableID
    val snapshot = QbeastContext.metadataManager.loadSnapshot(tableID)
    val schema = QbeastContext.metadataManager.loadCurrentSchema(tableID)
    val revision = snapshot.loadLatestRevision
    val columnsToIndex = revision.columnTransformers.map(row => row.columnName).mkString(",")
    val cubeSize = revision.desiredCubeSize
    val parameters =
      Map[String, String]("columnsToIndex" -> columnsToIndex, "cubeSize" -> cubeSize.toString())

    val path = new Path(tableID.id)
    val fileIndex = OTreeIndex(spark, path)
    val bucketSpec: Option[BucketSpec] = None
    val file = new ParquetFileFormat()

    new HadoopFsRelation(
      fileIndex,
      partitionSchema = StructType(Seq.empty[StructField]),
      dataSchema = schema,
      bucketSpec = bucketSpec,
      file,
      parameters)(spark) with InsertableRelation {
      def insert(data: DataFrame, overwrite: Boolean): Unit = {
        table.save(data, parameters, append = !overwrite)
      }
    }
  }

  /**
   * Function that can be called from a QbeastBaseRelation object to create a
   * new QbeastBaseRelation with a new tableID.
   * @param tableID the identifier of the table
   * @param indexedTable the indexed table
   * @return BaseRelation for the new table in Qbeast format
   */
  def forQbeastTable(tableID: QTableID, indexedTable: IndexedTable): BaseRelation = {

    val spark = SparkSession.active
    createRelation(spark.sqlContext, indexedTable)

  }

}
