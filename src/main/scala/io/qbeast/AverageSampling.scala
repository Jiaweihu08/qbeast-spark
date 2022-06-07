/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast

import io.qbeast.core.model.QTableID
import io.qbeast.spark.delta
import io.qbeast.spark.delta.DeltaQbeastSnapshot
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

// scalastyle:off
object AverageSampling {
  val spark: SparkSession = SparkSession.builder().getOrCreate()
  // PR table indexed on repo_main_language and month_year
  val path = "s3a://qbeast-research/github-archive/pr_twoCols/"

  val targetColumns = Seq(col("added_rows"), col("deleted_rows"), col("changed_files"))

  val tableId = new QTableID(path)
  val deltaLog: DeltaLog = DeltaLog.forTable(spark, tableId.id)

  val qbeastSnapshot: DeltaQbeastSnapshot =
    delta.DeltaQbeastSnapshot(deltaLog.snapshot)

  val cubeStatuses = qbeastSnapshot.loadLatestIndexStatus.cubesStatuses

  // Steps:
  // 1. Read root cube and use its files as the initial sample s
  // 2. For each of target group in s, compute the sample size and the files or cubes to read to satisfy the tolerance
  // 3. Get the union of files S to read from all groups
  // 4. Read data from S and use it as the starting point for the next iter until the tolerance
  // constraint is met for all groups
  def averageWithTolerance(tol: Double): Unit = {
    val initialSample = getInitialSample()
  }

  def averageSampling(): Unit = {}

  def getInitialSample(): DataFrame = {
    val rootStatus = qbeastSnapshot.loadLatestIndexStatus.cubesStatuses.values.head
    val rootPath = rootStatus.files.map(_.path).head
    val initialSample = spark.read.format("parquet").load(path + rootPath)
    initialSample
  }

  def computeStats(df: DataFrame): DataFrameStats = {
//    df.agg(count().alias("count"), avg())
    DataFrameStats(0L, 0.0, 0.0, 0L)
  }

}

case class DataFrameStats(count: Long, mean: Double, std: Double, size: Long)
