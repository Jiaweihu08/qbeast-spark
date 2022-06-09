/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.index.query

import io.qbeast.core.model._
import io.qbeast.spark.delta.DeltaQbeastSnapshot
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.delta.DeltaLog

// scalastyle:off
// Steps:
// 1. Read root cube and use its files as the initial sample s
// 2. For each of target group in s, compute the sample size and the files or cubes to read to satisfy the tolerance
// 3. Get the union of files S to read from all groups
// 4. Read data from S and use it as the starting point for the next iter until the tolerance
// constraint is met for all groups

class FileExtractor(spark: SparkSession) {
  val path = "s3a://qbeast-research/github-archive/pr_twoCols/"

  val deltaLog: DeltaLog = DeltaLog.forTable(spark, path)
  val qbeastSnapshot: DeltaQbeastSnapshot = DeltaQbeastSnapshot(deltaLog.snapshot)
  val querySpecBuilder = new QuerySpecBuilder(Seq.empty)

  val queryExecutor =
    new QueryExecutor(querySpecBuilder, qbeastSnapshot)

  val revision: Revision = qbeastSnapshot.loadAllRevisions.head
  val indexStatus: IndexStatus = qbeastSnapshot.loadIndexStatus(revision.revisionID)

  val querySpace: AllSpace = AllSpace()

  def getMatchingFiles(fraction: Double): Seq[String] = {
    val weightRange = WeightRange(Weight.MinValue, Weight(fraction))
    val querySpec = QuerySpec(weightRange, querySpace)
    queryExecutor.executeRevision(querySpec, indexStatus).map(_.path)
  }

}

//  def readRoot(qbeastSnapshot: QbeastSnapshot, path: String): DataFrame = {
//    val rootStatus = qbeastSnapshot.loadLatestIndexStatus.cubesStatuses.values.head
//    val rootPath = rootStatus.files.map(_.path).head
//    val initialSample = spark.read.format("parquet").load(path + rootPath)
//    initialSample
//  }
