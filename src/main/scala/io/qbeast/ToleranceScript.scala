/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, to_date, year}

class ToleranceScript(spark: SparkSession) {
  val sourcePrPath = "s3a://qbeast-research/github-archive/pr/"
  val pr: DataFrame = spark.read.format("parquet").load(sourcePrPath)
  val prWithYear: DataFrame = pr.withColumn("year", year(to_date(col("month_year"), "MM-yyyy")))
  val columnsToIndex = "repo_main_language,year"

  pr.createOrReplaceTempView("pr")

  // Replace pr with prWithYear
  // Index prWithYear using the Double Pass Implementation

//  prWithYear.write
//    .format("qbeast")
//    .option("columnsToIndex", columnsToIndex)
//    .save("s3a://qbeast-research/github-archive/qbeast-pr/double/original/")

  def writeChangedFiles(): Unit = {
    val doubleLimitedPath =
      "s3a://qbeast-research/github-archive/qbeast-pr/double/changed_files/withLimit50/"

    prWithYear
      .where("changed_files <= 50")
      .write
      .format("qbeast")
      .option("columnsToIndex", columnsToIndex)
      .save(doubleLimitedPath)
  }

  def writeWithLimit(imp: String, dcs: String, targetColumn: String, upperBound: Int): Unit = {
    val isValidImp = Seq("double", "piecewiseSeq", "single").contains(imp)
    val isValidTargetColumn =
      Seq("changed_files", "added_rows", "deleted_rows").contains(targetColumn)

    assert(isValidImp && isValidTargetColumn, "Input not valid, try again")

    val targetPath =
      s"s3a://qbeast-research/github-archive/qbeast-pr/$imp/$targetColumn/withLimit$upperBound/dcs/"

    pr.where(s"$targetColumn <= $upperBound")
      .write
      .format("qbeast")
      .option("analyzerImp", imp)
      .option("columnsToIndex", columnsToIndex)
      .option("cubeSize", dcs)
      .save(targetPath)
  }

}

//
// val path = "s3a://qbeast-research/github-archive/pr_twoCols/"
//
// def groupSampleSize(cnt: Long, avg: Double, std: Double): Double = {
//  val tolerance = 0.1
//  val z = 1.96 // 95% confident
//  val central = z * std / avg
//  val frac = (1.0 / (tolerance * tolerance)) * central * central / cnt
//  frac
//  }
//
//  val samplingFraction =
//  udf((cnt: Long, avg: Double, std: Double) => groupSampleSize(cnt, avg, std))
//
//  spark.udf.register("samplingFraction", samplingFraction)
//
//  def getTargetColumnValues(columnName: String): Unit = {
//  val df = spark.read.format("qbeast").load(path)
//  df.createOrReplaceTempView("pr")
//
//  //    val columnName = "changed_files"
//  val statsDf_limit50 = spark.sql(s"""SELECT
//        |repo_main_language AS language,
//        |YEAR(to_date(month_year, 'MM-yyyy')) AS year,
//        |COUNT($columnName) AS cnt,
//        |AVG($columnName) AS avg,
//        |STD($columnName) AS std
//        |FROM
//        |pr
//        |WHERE
//        |$columnName <= 50 AND YEAR(to_date(month_year, 'MM-yyyy')) IS NOT NUll
//        |GROUP BY
//        |repo_main_language, YEAR(to_date(month_year, 'MM-yyyy'))
//        |HAVING cnt > 30
//        |""".stripMargin)
//
//  statsDf_limit50.createOrReplaceTempView("stats")
//
//  val groupSamplingFraction = spark.sql(s"""
//        |SELECT
//        |language,
//        |year,
//        |samplingFraction(cnt, avg, std) AS fraction,
//        |avg, std, cnt, '$columnName'
//        |FROM
//        |stats
//        |""".stripMargin)
//
//  groupSamplingFraction.show()
//  //    groupSamplingFraction
//  //      .agg(
//  //        count("*").alias("cnt"),
//  //        avg("fraction").alias("avg"),
//  //        percentile_approx(col("fraction"), lit(0.5), lit(10000000)).alias("median"))
//  //      .show()
//  }
