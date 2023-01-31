/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.index

import io.qbeast.core.model.{Point, Revision}
import org.apache.spark.sql.{AnalysisExceptionFactory, Row}

/**
 * Utility functions for working with Spark Rows
 */
object RowUtils {

  /**
   * Converts the row values to a Point in the space
   * @param row the row values
   * @param revision the revision of the space
   * @return the point
   */
  def rowValuesToPoint(row: Row, revision: Revision): Point = Point {
    if (revision.transformations.isEmpty) {
      throw AnalysisExceptionFactory.create("Trying to index on a not initialized Revision")

    }
    val coordinates = Vector.newBuilder[Double]
    coordinates.sizeHint(revision.columnTransformers.length)
    var i = 0
    for (t <- revision.transformations) {
      val v = row.get(i)
      coordinates += t.transform(v)
      i += 1

    }
    coordinates.result()

  }

  def rowValuesToPercentilePoint(
      row: Row,
      revision: Revision,
      columnPercentiles: Seq[Seq[Any]]): Point = Point {
    if (revision.transformations.isEmpty) {
      throw AnalysisExceptionFactory.create("Trying to index on a not initialized Revision")
    }

    val coordinates = Vector.newBuilder[Double]
    coordinates.sizeHint(revision.columnTransformers.length)
    for (i <- revision.transformations.indices) {
      val v = row.get(i)
      val percentiles = columnPercentiles(i)
      val t = revision.transformations(i)
      coordinates += t.transformWithPercentiles(v, percentiles.toIndexedSeq)
    }
    coordinates.result()
  }

}
