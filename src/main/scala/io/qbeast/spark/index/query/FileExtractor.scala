/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.index.query

import io.qbeast.IISeq
import io.qbeast.core.model._
import io.qbeast.spark.delta.DeltaQbeastSnapshot
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.functions.col

import scala.collection.mutable

class FileExtractor(spark: SparkSession, path: String) {

  val qbeastSnapshot: DeltaQbeastSnapshot = DeltaQbeastSnapshot(
    DeltaLog.forTable(spark, path).snapshot)

  val revision: Revision = qbeastSnapshot.loadLatestRevision
  val indexStatus: IndexStatus = qbeastSnapshot.loadLatestIndexStatus

  def getSamplingBlocks(fraction: Double, expressions: Seq[Expression]): IISeq[QbeastBlock] = {
    val querySpecBuilder = new QuerySpecBuilder(expressions)
    var querySpec: QuerySpec = querySpecBuilder.build(revision)
    val queryExecutor =
      new QueryExecutor(querySpecBuilder, qbeastSnapshot)

    if (fraction < 1.0) {
      val weightRange = WeightRange(Weight.MinValue, Weight(fraction))
      querySpec = querySpec.copy(weightRange = weightRange)
    }

    queryExecutor.executeRevision(querySpec, indexStatus)
  }

  def adaptiveSampling(
      groups: Seq[String],
      targetGroupNumber: Int): (DataFrame, Seq[QbeastBlock], Double) = {
    val tree = indexStatus.cubesStatuses

    val byWeight = Ordering.by[CubeStatus, NormalizedWeight](_.normalizedWeight)
    val queue = new mutable.PriorityQueue[CubeStatus]()(byWeight)

    queue.enqueue(indexStatus.cubesStatuses.head._2)
    var blocks = Seq.empty[QbeastBlock]

    var df = spark.emptyDataFrame
    var fraction = 0d

    var continue = true
    while (!continue || queue.nonEmpty) {
      val nextCube = queue.dequeue()
      blocks ++= nextCube.files
      nextCube.cubeId.children
        .filter(tree.contains)
        .map(c => tree(c))
        .foreach(cs => queue.enqueue(cs))

      df = spark.read.parquet(blocks.map(path + _.path): _*)
      fraction = nextCube.normalizedWeight
      val numGroups = df.groupBy(groups.map(col): _*).count.collect().head.getInt(0)

      // scalastyle:off
      println(s"Current fraction: $fraction, number of groups found: $numGroups")

      if (numGroups == targetGroupNumber) {
        continue = false
      }
    }
    (df, blocks, fraction)
  }

}
