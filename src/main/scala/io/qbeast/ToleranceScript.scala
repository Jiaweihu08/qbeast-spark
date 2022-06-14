/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast

import io.qbeast.core.model.QbeastBlock
import io.qbeast.spark.index.query.FileExtractor
import org.apache.spark.sql.functions.{avg, expr, udf}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.mutable

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

//  def getFilePaths(
//      fraction: Double,
//      extractor: FileExtractor,
//      filterGroup: String): Seq[QbeastBlock] = {
//    val blocks = extractor.getSamplingBlocks(fraction, Seq(expr(filterGroup).expr))
//    blocks
//  }

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
          case _ => {
            println(">>> Shit happening")
            "Shit happening"
          }
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
    fractionFiles.map {
      case r @ RowWithPaths(_, _, average, _, _, _, _, _, paths10p, paths1p, groupFilter, _, _) =>
        r.copy(
          avgError10p = computeAvgError(paths10p, groupFilter, average),
          avgError1p = computeAvgError(paths1p, groupFilter, average))
    }
  }

  def computeAvgError(sampleFiles: Seq[String], groupFilter: String, average: Double): Double = {
    println(s"Number of files: ${sampleFiles.length}")
    val sample = spark.read.format("parquet").load(sampleFiles: _*)
    val averageDf = sample.where(groupFilter).agg(avg("changed_files"))
    val outputRow = averageDf.collect().head
    if (outputRow.isNullAt(0)) {
      averageDf.show()
      -1
    } else {
      val sampleAvg = outputRow.getDouble(0)
      Math.abs(average - sampleAvg) / average
    }
  }

  def main(args: Set[String]): Unit = {
    import spark.implicits._
    val sourcePath =
      "s3a://qbeast-research/github-archive/qbeast-pr/double/changed_files_upperBound50/5000000/"
    val fractionPath =
      "s3a://qbeast-research/github-archive/qbeast-pr/double/changed_files_upperBound50/groupFractions/"

    val fractionFiles = computeFractionFiles(sourcePath, fractionPath)
    val withEstimationError = addApproximationError(fractionFiles)
    withEstimationError.toDS
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
    avgError10p: Double = 0,
    avgError1p: Double = 0)
