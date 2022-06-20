/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast

import io.qbeast.core.model.QbeastBlock
import io.qbeast.spark.index.query.FileExtractor
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.mutable

// scalastyle:off

object Script {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().getOrCreate()

    val sourcePath =
      "s3a://qbeast-research/github-archive/qbeast-pr/double/changed_files_upperBound50/5000000/"
    val fractionPath =
      "s3a://qbeast-research/github-archive/qbeast-pr/double/changed_files_upperBound50/groupFractions/"

    val script = new ToleranceScript(spark)

    val groupFractions = (spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(fractionPath))

    val (paths10p, paths1p) = script.gatherFractionFiles(sourcePath, groupFractions)

    val withEstimationAndError = script.addApproxAvgAndError(groupFractions, paths10p, paths1p)

    withEstimationAndError.write.parquet(
      "s3a://qbeast-research/github-archive/qbeast-pr/double/changed_files_upperBound50/withError/")
  }

}

class ToleranceScript(spark: SparkSession) {
  val columnsToIndex = "repo_main_language,year"
  val targetColumns = Seq("added_rows", "deleted_rows", "changed_files")
  val implementations = Seq("double", "piecewiseSeq", "single")

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

  val samplingFraction: UserDefinedFunction =
    udf((cnt: Long, avg: Double, std: Double, tol: Double) => groupSampleSize(cnt, avg, std, tol))

  spark.udf.register("samplingFraction", samplingFraction)

  def writeWithLimit(
      sourcePath: String,
      imp: String,
      dcs: String,
      targetColumn: String,
      upperBound: Int): Unit = {
    val isValidImp = implementations.contains(imp)
    val isValidTargetColumn = targetColumns.contains(targetColumn)

    assert(isValidImp && isValidTargetColumn, "Input not valid, try again")
    val pr =
      spark.read
        .format("parquet")
        .load(sourcePath)

    val targetPath =
      s"s3a://qbeast-research/github-archive/qbeast-pr/$imp/$targetColumn/withLimit$upperBound/$dcs/"

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

  def gatherFractionFiles(
      sourcePath: String,
      groupFractions: DataFrame): (Seq[String], Seq[String]) = {
    val extractor = new FileExtractor(spark, sourcePath)

    val rows = groupFractions.collect()
    val allBlocks10p = mutable.ArrayBuffer.empty[QbeastBlock]
    val allBlocks1p = mutable.ArrayBuffer.empty[QbeastBlock]

    rows.foreach {
      case Row(language, year, _, _, _, fraction10p: Double, fraction1p: Double, _) =>
        val groupFilter = (language, year) match {
          case (l: String, y: Int) => s"""repo_main_language == "$l" AND year == $y"""
          case (null, y: Int) => s"repo_main_language IS NULL AND year == $y"
          case (l: String, null) => s"""repo_main_language == "$l" AND year IS NULL"""
          case (null, null) => s"repo_main_language IS NULL AND year IS NULL"
          case _ =>
            println(">>> Shit happening")
            "Shit happening"
        }

        val expressions = Seq(expr(groupFilter).expr)
        val blocks10p = extractor.getSamplingBlocks(fraction10p, expressions)
        val blocks1p = extractor.getSamplingBlocks(fraction1p, expressions)

        allBlocks10p ++= blocks10p
        allBlocks1p ++= blocks1p
    }

    val unique10pBlocks = allBlocks10p.toSet.toSeq
    val size10p = (unique10pBlocks.foldLeft(0d) { case (acc, b) =>
      acc + b.size / 1024d / 1024d
    } / 1024d).toInt

    val unique1pBlocks = allBlocks1p.toSet.toSeq
    val size1p = (unique1pBlocks.foldLeft(0d) { case (acc, b) =>
      acc + b.size / 1024d / 1024d
    } / 1024d).toInt

    println(s"Number of files read for t=10%: ${unique10pBlocks.size}, total size: $size10p")
    println(s"Number of files read for t=1%: ${unique1pBlocks.size}, total size: $size1p")

    (unique10pBlocks.map(sourcePath + _.path), unique1pBlocks.map(sourcePath + _.path))
  }

  def addApproxAvgAndError(
      groupFractions: DataFrame,
      uniqueFiles10p: Seq[String],
      uniqueFiles1p: Seq[String]): DataFrame = {

    val estimation10p = (
      spark.read
        .format("parquet")
        .load(uniqueFiles10p: _*)
        .groupBy("repo_main_language", "year")
        .agg(avg("changed_files").alias("estAvg10p"))
        .withColumnRenamed("year", "year10p")
    )

    val estimation1p = (
      spark.read
        .format("parquet")
        .load(uniqueFiles1p: _*)
        .groupBy("repo_main_language", "year")
        .agg(avg("changed_files").alias("estAvg1p"))
        .withColumnRenamed("year", "year1p")
    )

    val columns = groupFractions.columns ++ Seq("estAvg10p", "estAvg1p")

    (
      groupFractions
        .join(
          estimation10p,
          groupFractions("language") === estimation10p("repo_main_language") && groupFractions(
            "year") === estimation10p("year10p"),
          "left")
        .join(
          estimation1p,
          groupFractions("language") === estimation1p("repo_main_language") && groupFractions(
            "year") === estimation1p("year1p"),
          "left")
        .select(columns.map(c => col(c)): _*)
        .withColumn("avgError10p", abs(col("avg") - col("estAvg10p")) / col("avg"))
        .withColumn("avgError1p", abs(col("avg") - col("estAvg1p")) / col("avg"))
    )

  }

}
