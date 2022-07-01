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
    val script = new ToleranceScript(spark)

    val sourcePath =
      "s3a://qbeast-research/github-archive/qbeast-pr/double/changed_files_upperBound50/nullAsInt/treeCols/5000000/"
    val pr = spark.read.parquet(sourcePath)

//    val fractionPath =
//      "s3a://qbeast-research/github-archive/qbeast-pr/double/changed_files_upperBound50/nullAsInt/groupFractions/"

//    val groupFractions = (spark.read
//      .format("csv")
//      .option("header", "true")
//      .option("inferSchema", "true")
//      .load(fractionPath))

    val tol = 0.1
    val groupsWithFraction = script.computeGroupFraction(pr, tol)
    val groups = groupsWithFraction.collect()

    val uniqueFiles = script.gatherFractionFiles(sourcePath, groups, tol)
    val withEstimationAndError =
      script.addApproxAvgAndError(groupsWithFraction, uniqueFiles, tol)
    withEstimationAndError.groupBy("isErrorBounded").count.show
  }

}

class ToleranceScript(spark: SparkSession) extends Serializable {
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

  val samplingFraction: UserDefinedFunction =
    udf((cnt: Long, avg: Double, std: Double, tol: Double) => groupSampleSize(cnt, avg, std, tol))

  def computeGroupFraction(
      pr: DataFrame,
      tol: Double,
      columnName: String = "changed_files"): DataFrame = {
    assert(tol < 1.0, s"Invalid tol value: $tol >= 1.0")
    assert(targetColumns.contains(columnName), s"Invalid target column: $columnName")

    val groupsWithFraction = (
      pr
        .groupBy("repo_main_language", "year")
        .agg(
          count(columnName).alias("cnt"),
          avg(columnName).alias("avg"),
          stddev_pop(columnName).alias("std"))
        .withColumn("fraction", samplingFraction(col("cnt"), col("avg"), col("std"), lit(tol)))
        .withColumnRenamed("repo_main_language", "language")
    )

    groupsWithFraction
  }

  def gatherFractionFiles(sourcePath: String, groups: Seq[Row], tol: Double): Seq[String] = {
    assert(tol < 1.0, s"Invalid tolerance: $tol >= 1.0")

    val extractor = new FileExtractor(spark, sourcePath)
    val allBlocks = mutable.ArrayBuffer.empty[IISeq[QbeastBlock]]

    groups.foreach { case Row(language, year, _, _, _, fraction: Double) =>
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
      val blocks = extractor.getSamplingBlocks(fraction, expressions)

      allBlocks += blocks
    }

    val uniqueBlocks = allBlocks.flatten.toSet.toSeq

    val size = ((uniqueBlocks.foldLeft(0d) { case (acc, b) =>
      acc + b.size / 1024d / 1024d
    } / 1024d) * 10).toInt / 10d

    var isValid = 0
    var notValid = 0
    groups.foreach(r => if (r.getDouble(5) < 1.0) isValid += 1 else notValid += 1)

    println(s"Valid fractions: $isValid, invalid fractions: $notValid")
    println(s"Number of files read for t=$tol: ${uniqueBlocks.size}, total size: ${size}GB")
    println(s"Max number of blocks: ${allBlocks.map(_.size).max}")

    uniqueBlocks.map(sourcePath + _.path)
  }

  def addApproxAvgAndError(
      groupsWithFraction: DataFrame,
      uniqueFiles: Seq[String],
      tol: Double): DataFrame = {

    val estimation = (
      spark.read
        .format("parquet")
        .load(uniqueFiles: _*)
        .groupBy("repo_main_language", "year")
        .agg(avg("changed_files").alias("estAvg"))
        .withColumnRenamed("year", "estYear")
    )

    val columns = groupsWithFraction.columns :+ "estAvg" map col

    (
      groupsWithFraction
        .join(
          estimation,
          groupsWithFraction("language") <=> estimation("repo_main_language") &&
            groupsWithFraction("year") <=> estimation("estYear"),
          "left")
        .select(columns: _*)
        .withColumn("avgError", abs(col("avg") - col("estAvg")) / col("avg"))
        .orderBy(desc("avgError"))
        .withColumn("isErrorBounded", col("avgError") < lit(tol))
    )

  }

}

object WriteWithLimit {
  val columnsToIndex = "repo_main_language,year,pr_id"
  val targetColumns = Seq("added_rows", "deleted_rows", "changed_files")
  val implementations = Seq("double", "piecewiseSeq", "single")
  val spark: SparkSession = SparkSession.builder().getOrCreate()

  def writeWithLimit(
      sourcePath: String,
      imp: String = "double",
      dcs: String = "5000000",
      targetColumn: String = "changed_files",
      upperBound: Int = 50): Unit = {
    assert(
      implementations.contains(imp) && targetColumns.contains(targetColumn),
      "Input not valid, try again")

    val targetPath =
      s"s3a://qbeast-research/github-archive/qbeast-pr/$imp/${targetColumn}withLimit$upperBound/${columnsToIndex.size}cols/$dcs/"
    println(s"Saving to $targetPath")

    spark.read
      .format("parquet")
      .load(sourcePath)
      .where(s"$targetColumn <= $upperBound")
      .write
      .format("qbeast")
      .option("analyzerImp", imp)
      .option("columnsToIndex", columnsToIndex)
      .option("cubeSize", dcs)
      .save(targetPath)
  }

}
