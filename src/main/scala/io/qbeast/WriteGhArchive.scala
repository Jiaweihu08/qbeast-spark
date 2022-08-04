/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast

import io.qbeast.spark.QbeastTable
import org.apache.spark.sql.SparkSession

object WriteGhArchive {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().getOrCreate()

    val sourcePath = "s3a://qbeast-research/github-archive/prWithYear/"
    val targetPath =
      "s3a://qbeast-research/github-archive/qbeast-pr/piecewiseSeq-noWeightCut/500k/"

    val source = spark.read.parquet(sourcePath).where("changed_files <= 50")
    spark.time(
      source.write
        .format("qbeast")
        .option("cubeSize", 500000)
        .option("columnsToIndex", "repo_main_language,year")
        .save(targetPath))

    val metrics = QbeastTable.forPath(spark, targetPath).getIndexMetrics()

    // scalastyle:off println
    println(metrics)
  }

}
