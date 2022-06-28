/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.index.query

import io.qbeast.IISeq
import io.qbeast.core.model._
import io.qbeast.spark.delta.DeltaQbeastSnapshot
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.delta.DeltaLog

class FileExtractor(spark: SparkSession, path: String) {

  val qbeastSnapshot: DeltaQbeastSnapshot = DeltaQbeastSnapshot(
    DeltaLog.forTable(spark, path).snapshot)

  val revision: Revision = qbeastSnapshot.loadLatestRevision
  val indexStatus: IndexStatus = qbeastSnapshot.loadLatestIndexStatus

  def getSamplingBlocks(
      fraction: Double,
      expressions: Seq[Expression],
      threshold: Double = 1.0): IISeq[QbeastBlock] = {
    val querySpecBuilder = new QuerySpecBuilder(expressions)
    var querySpec: QuerySpec = querySpecBuilder.build(revision)
    val queryExecutor =
      new QueryExecutor(querySpecBuilder, qbeastSnapshot)

    if (fraction < threshold) {
      val weightRange = WeightRange(Weight.MinValue, Weight(fraction))
      querySpec = querySpec.copy(weightRange = weightRange)
    }

    queryExecutor.executeRevision(querySpec, indexStatus)
  }

}
