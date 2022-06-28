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
// import io.qbeast.ToleranceScript
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

    val tol = 0.2
    val groupFractions = script.computeGroupFraction(pr, tol)

    groupFractions.withColumn("worthIt", col("fraction") < 1.0).groupBy("worthIt").count.show

    val groups = groupFractions.collect()

    val filePaths = script.gatherFractionFiles(sourcePath, groups, tol)

    val withEstimationAndError = script.addApproxAvgAndError(groupFractions, filePaths, tol)

    withEstimationAndError.show

//    withEstimationAndError.write.parquet(
//      "s3a://qbeast-research/github-archive/qbeast-pr/double/changed_files_upperBound50/nullAsInt/treeCols/errors/")
//
//    spark.read.parquet(
//      "s3a://qbeast-research/github-archive/qbeast-pr/double/changed_files_upperBound50/nullAsInt/treeCols/errors/")
  }

}

class ToleranceScript(spark: SparkSession) {
  val columnsToIndex = "repo_main_language,year,repo_id"
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

  def computeGroupFraction(
      pr: DataFrame,
      tol: Double,
      columnName: String = "changed_files"): DataFrame = {
    assert(tol < 1.0, s"Invalid tol value: $tol >= 1.0")

    pr.createOrReplaceTempView("pr")

    val stats = spark.sql(s"""
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

    stats.createOrReplaceTempView("stats")

    val groupSamplingFraction = spark.sql(s"""
          |SELECT
          |language,
          |year,
          |avg, std, cnt,
          |samplingFraction(cnt, avg, std, $tol) AS fraction,
          |'$columnName'
          |FROM
          |stats
          |""".stripMargin)

    groupSamplingFraction
  }

  def gatherFractionFiles(sourcePath: String, groups: Seq[Row], tol: Double): Seq[String] = {
    assert(tol < 1.0, s"Invalid tolerance: $tol >= 1.0")

    val extractor = new FileExtractor(spark, sourcePath)
    val allBlocks = mutable.ArrayBuffer.empty[QbeastBlock]

    var maxSize = 0
    groups.foreach { case Row(language, year, _, _, _, fraction: Double, _) =>
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
      val f = if (fraction < 0.5) fraction else 0.5
      val blocks = extractor.getSamplingBlocks(f, expressions)
      allBlocks ++= blocks
      if (blocks.size > 30) {
        if (fraction >= 1.0) println(">>>")
        println(s"Row($language, $year, $fraction), ${blocks.size}")
      }

      if (blocks.size > maxSize) {
        maxSize = blocks.size
      }
    }
    println(s"Max number of blocks: $maxSize")

    val uniqueBlocks = allBlocks.toSet.toSeq
    val size = (uniqueBlocks.foldLeft(0d) { case (acc, b) =>
      acc + b.size / 1024d / 1024d
    } / 1024d).toInt

    println(s"Number of files read for t=$tol: ${uniqueBlocks.size}, total size: $size GB")

    uniqueBlocks.map(sourcePath + _.path)
  }

  def addApproxAvgAndError(
      groupFractions: DataFrame,
      uniqueFiles: Seq[String],
      tol: Double): DataFrame = {
    val estColName = s"estAvg($tol)"
    val errorColName = s"avgError($tol)"

    val estimation = (
      spark.read
        .format("parquet")
        .load(uniqueFiles: _*)
        .groupBy("repo_main_language", "year")
        .agg(avg("changed_files").alias(estColName))
        .withColumnRenamed("year", "estYear")
    )

    val columns = groupFractions.columns ++ Seq("estAvg10p") map col

    (
      groupFractions
        .join(
          estimation,
          groupFractions("language") <=> estimation("repo_main_language") && groupFractions(
            "year") <=> estimation("estYear"),
          "left")
        .select(columns: _*)
        .withColumn(errorColName, abs(col("avg") - col(estColName)) / col("avg"))
    )

  }

}
