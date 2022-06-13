/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast

import io.qbeast.spark.index.query.FileExtractor
import org.apache.spark.sql.functions.{expr, udf}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

// scalastyle:off
class ToleranceScript(spark: SparkSession) {
  val columnsToIndex = "repo_main_language,year"
  val targetColumns = Seq("added_rows", "deleted_rows", "changed_files")

  def groupSampleSize(cnt: Long, avg: Double, std: Double, tol: Double): Double = {
    if (cnt <= 30d) {
      1.0
    } else {
      val z = 1.96 // 95% confident
      val central = z * std / avg
      val frac = (1.0 / (tol * tol)) * central * central / cnt
      frac
    }
  }

  val samplingFraction =
    udf((cnt: Long, avg: Double, std: Double, tol: Double) => groupSampleSize(cnt, avg, std, tol))

  spark.udf.register("samplingFraction", samplingFraction)

  def writeWithLimit(
      pr: DataFrame,
      imp: String,
      dcs: String,
      targetColumn: String,
      upperBound: Int): Unit = {
    val isValidImp = Seq("double", "piecewiseSeq", "single").contains(imp)
    val isValidTargetColumn = targetColumns.contains(targetColumn)

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

  def computeGroupFractions(
      sourcePath: String,
      fractionPath: String,
      columnName: String): Unit = {
    val pr = spark.read.format("qbeast").load(sourcePath)

    pr.createOrReplaceTempView("pr")

    val statsDf_limit50 = spark.sql(s"""
         |SELECT
         |repo_main_language AS language,
         |year,
         |COUNT($columnName) AS cnt,
         |AVG($columnName) AS avg,
         |STDDEV_POP($columnName) AS std,
         |'$columnName'
         |FROM
         |pr
         |GROUP BY
         |repo_main_language, year
         |""".stripMargin)

    statsDf_limit50.createOrReplaceTempView("stats")

    val groupSamplingFraction = spark.sql(s"""
          |SELECT
          |language,
          |year,
          |avg, std, cnt,
          |samplingFraction(cnt, avg, std, 0.1) AS 10p_fraction,
          |samplingFraction(cnt, avg, std, 0.01) AS 1p_fraction,
          |'$columnName'
          |FROM
          |stats
          |""".stripMargin)

    groupSamplingFraction.show()

    groupSamplingFraction.write.option("header", "true").csv(fractionPath)

  }

//  val sourcePath = "s3a://qbeast-research/github-archive/qbeast-pr/double/changed_files_upperBound50/5000000/"
//  val fractionPath = "s3a://qbeast-research/github-archive/qbeast-pr/double/changed_files_upperBound50/groupFractions/"

  def computeFractionFiles(sourcePath: String, fractionPath: String): Seq[RowWithPaths] = {
    val extractor = new FileExtractor(spark, sourcePath)
    val groupFractions = (spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(fractionPath))

    val rows = groupFractions.collect()
    var numMBs = 0L
    rows.map {
      case Row(
            language: String,
            year: Int,
            avg: Double,
            std: Double,
            cnt: Int,
            fraction10p: Double,
            fraction1p: Double,
            col: String) =>
        val filterGroup = s"repo_main_language == '$language' AND year == $year"
        val (paths10p, numMBs10p) = getFilePaths(fraction10p, extractor, filterGroup, sourcePath)
        val (paths1p, numMBs1p) = getFilePaths(fraction1p, extractor, filterGroup, sourcePath)

        numMBs += numMBs10p + numMBs1p

        RowWithPaths(
          language,
          year,
          avg,
          std,
          cnt,
          fraction10p,
          fraction1p,
          col,
          paths10p,
          paths1p)
    }
  }

  def getFilePaths(
      fraction: Double,
      extractor: FileExtractor,
      filterGroup: String,
      sourcePath: String): (Seq[String], Long) = {
    val blocks = extractor.getSamplingBlocks(fraction, Seq(expr(filterGroup).expr))
    val filePaths = blocks.map(sourcePath + _.path)
    val numMBs = blocks.map(_.size / 1024 / 1024).sum
    (filePaths, numMBs)
  }

//    val df = spark.read.parquet(filePaths: _*).where(filterGroup)
//    val sampleAvg = df
//      .agg(avg("changed_files"))
//      .collect()
//      .head
//      .getDouble(0)
//    val error = Math.abs(sampleAvg - totalAvg) / totalAvg
//    (sampleAvg, error, sizeInMB)
}

case class RowWithPaths(
    language: String,
    year: Int,
    avg: Double,
    std: Double,
    cnt: Int,
    fraction10p: Double,
    fraction1p: Double,
    changed_files: String,
    paths10p: Seq[String],
    paths1p: Seq[String])
