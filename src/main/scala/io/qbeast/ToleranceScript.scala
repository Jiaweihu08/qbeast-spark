/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast

import io.qbeast.core.model.QbeastBlock
import io.qbeast.spark.index.query.FileExtractor
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{avg, expr, udf}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.mutable

// scalastyle:off

object Script {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val script = new ToleranceScript(spark)

    val sourcePath =
      "s3a://qbeast-research/github-archive/qbeast-pr/double/changed_files_upperBound50/5000000/"
    val fractionPath =
      "s3a://qbeast-research/github-archive/qbeast-pr/double/changed_files_upperBound50/groupFractions/"

    val fractionFiles = script.computeFractionFiles(sourcePath, fractionPath)

    val withEstimationError = script.addApproximationError(fractionFiles)

    val outputDf: DataFrame = withEstimationError
      .map(RowWithPaths.unapply(_).get)
      .toDF(
        "language",
        "year",
        "avg",
        "std",
        "cnt",
        "fraction10p",
        "fraction1p",
        "changed_files",
        "paths10p",
        "paths1p",
        "groupFilter",
        "estAvg10p",
        "estAvg1p",
        "avgError10p",
        "avgError1p")

    outputDf.write.parquet(
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

  def computeFractionFiles(sourcePath: String, fractionPath: String): Seq[RowWithPaths] = {
    val extractor = new FileExtractor(spark, sourcePath)
    val groupFractions = (spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(fractionPath))

    val rows = groupFractions.collect()
    val allBlocks10p = mutable.ArrayBuffer.empty[QbeastBlock]
    val allBlocks1p = mutable.ArrayBuffer.empty[QbeastBlock]
    val fractionFiles = rows.map {
      case Row(
            language,
            year,
            avg: Double,
            std: Double,
            cnt: Int,
            fraction10p: Double,
            fraction1p: Double,
            col: String) =>
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

        RowWithPaths(
          language,
          year,
          avg,
          std,
          cnt,
          fraction10p,
          fraction1p,
          col,
          blocks10p.map(sourcePath + _.path),
          blocks1p.map(sourcePath + _.path),
          groupFilter)
    }

    val files10p = fractionFiles.flatMap(r => r.paths10p).toSet.toSeq
    val unique10pBlocks = allBlocks10p.toSet.toSeq
    val size10p = (unique10pBlocks.foldLeft(0d) { case (acc, b) =>
      acc + b.size / 1024d / 1024d
    } / 1024d).toInt

    val files1p = fractionFiles.flatMap(r => r.paths1p).toSet.toSeq
    val unique1pBlocks = allBlocks1p.toSet.toSeq
    val size1p = (unique1pBlocks.foldLeft(0d) { case (acc, b) =>
      acc + b.size / 1024d / 1024d
    } / 1024d).toInt

    val areTheSame =
      unique10pBlocks.size == unique1pBlocks.size && unique10pBlocks.forall(
        unique1pBlocks.contains)

    print(s"Are the same blocks? $areTheSame")

    println(s"Number of files read for t=10%: ${files10p.size}, total size: $size10p")

    println(s"Number of files read for t=1%: ${files1p.size}, total size: $size1p")

    fractionFiles
  }

  def addApproximationError(fractionFiles: Seq[RowWithPaths]): Seq[RowWithPaths] = {
    val numRows = fractionFiles.size
    var cnt = 0
    fractionFiles.map { r =>
      cnt += 1
      println(s"""row number: $cnt / $numRows,
           |fraction10p: ${r.fraction10p}, fraction1p: ${r.fraction1p},
           |${r.groupFilter}""".stripMargin)
      val (est10p, error10p) = computeAvgError(r.paths10p, r.groupFilter, r.avg)
      val (est1p, error1p) = computeAvgError(r.paths1p, r.groupFilter, r.avg)
      r.copy(estAvg10p = est10p, estAvg1p = est1p, avgError10p = error10p, avgError1p = error1p)
    }
  }

  def computeAvgError(
      sampleFiles: Seq[String],
      groupFilter: String,
      average: Double): (Double, Double) = {
    val outputRow = spark.read
      .format("parquet")
      .load(sampleFiles: _*)
      .where(groupFilter)
      .agg(avg("changed_files"))
      .collect()
      .head

    if (outputRow.isNullAt(0)) {
      println(outputRow)
      (-1, -1)
    } else {
      val sampleAvg = outputRow.getDouble(0)
      val error = if (average == 0.0) {
        0.0
      } else {
        Math.abs(average - sampleAvg) / average
      }
      (sampleAvg, error)
    }
  }

}

case class RowWithPaths(
    language: Any,
    year: Any,
    avg: Double,
    std: Double,
    cnt: Int,
    fraction10p: Double,
    fraction1p: Double,
    changed_files: String,
    paths10p: Seq[String],
    paths1p: Seq[String],
    groupFilter: String,
    estAvg10p: Double = 0.0,
    estAvg1p: Double = 0.0,
    avgError10p: Double = 0.0,
    avgError1p: Double = 0.0)

object RowWithPaths {

  def unapply(r: RowWithPaths): Option[Tuple15[
    String,
    Int,
    Double,
    Double,
    Int,
    Double,
    Double,
    String,
    Seq[String],
    Seq[String],
    String,
    Double,
    Double,
    Double,
    Double]] = {
    Some(
      (
        r.language.asInstanceOf[String],
        if (r.year == null) -1 else r.year.asInstanceOf[Int],
        r.avg,
        r.std,
        r.cnt,
        r.fraction10p,
        r.fraction1p,
        r.changed_files,
        r.paths10p,
        r.paths1p,
        r.groupFilter,
        r.estAvg10p,
        r.estAvg1p,
        r.avgError10p,
        r.avgError1p))
  }

}
