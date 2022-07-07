/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast

import io.qbeast.core.model.QbeastBlock
import io.qbeast.spark.QbeastTable
import io.qbeast.spark.index.query.FileExtractor
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.mutable

// scalastyle:off

object Script {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().getOrCreate()
    val script = new ToleranceScript(spark)

    val sourcePath =
      "s3a://qbeast-research/github-archive/qbeast-pr/double/changed_files-bounded-50/2cols/500000/"

    showMetrics(sourcePath)

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
    groupsWithFraction.cache()
    val groups = groupsWithFraction.collect()

    val (fileCount, uniqueFiles) = script.gatherFractionFiles(sourcePath, groups, tol)
    fileCount.cache()

    val groupWithFileCount = groupsWithFraction
      .join(
        fileCount,
        groupsWithFraction("language") <=> fileCount("_language") && groupsWithFraction(
          "year") <=> fileCount("_year"))
      .select((groupsWithFraction.columns :+ "fileCount").map(col): _*)
    groupWithFileCount.show

    val withEstimationAndError =
      script.addApproxAvgAndError(groupsWithFraction, uniqueFiles, tol)

    withEstimationAndError.cache()
    withEstimationAndError.groupBy("isErrorBounded").count.show
  }

  def showMetrics(path: String, isInner: Boolean = true): Unit = {
    val spark: SparkSession = SparkSession.builder().getOrCreate()
    val metrics = QbeastTable.forPath(spark, path).getIndexMetrics()
    println(metrics)

    val cubeStatuses = metrics.cubeStatuses
    val targetCubesStatuses =
      if (isInner) cubeStatuses.filter(_._1.children.exists(cubeStatuses.contains))
      else cubeStatuses

    targetCubesStatuses
      .groupBy(cw => cw._1.depth)
      .mapValues { m =>
        val weights = m.values.map(_.normalizedWeight)
        val elementCounts = m.values.map(_.files.map(_.elementCount).sum)
        (weights.sum / weights.size, elementCounts.sum / elementCounts.size)
      }
      .toSeq
      .sortBy(_._1)
      .foreach { cw => println(s"$cw\n") }
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

  def gatherFractionFiles(
      sourcePath: String,
      groups: Seq[Row],
      tol: Double): (DataFrame, Seq[String]) = {
    assert(tol < 1.0, s"Invalid tolerance: $tol >= 1.0")

    val fileCountSchema =
      StructType(
        StructField("_language", StringType, nullable = true) ::
          StructField("_year", IntegerType, nullable = true) ::
          StructField("fileCount", IntegerType, nullable = false) :: Nil)

    val extractor = new FileExtractor(spark, sourcePath)
    val allBlocks = mutable.ArrayBuffer.empty[IISeq[QbeastBlock]]

    val groupFileCount = groups.map { case Row(language, year, _, _, _, fraction: Double) =>
      val groupFilter = (language, year) match {
        case (l: String, y: Int) => s"""repo_main_language == "$l" AND year == $y"""
        case (null, y: Int) => s"repo_main_language IS NULL AND year == $y"
        case (l: String, null) => s"""repo_main_language == "$l" AND year IS NULL"""
        case (null, null) => s"repo_main_language IS NULL AND year IS NULL"
        case _ =>
          println(">>> Shit happening")
          "Shit happening"
      }

      val blocks = extractor.getSamplingBlocks(fraction, Seq(expr(groupFilter).expr))
      allBlocks += blocks

      Row(language, year, blocks.size)
    }

    val rdd = spark.sparkContext.parallelize(groupFileCount)
    val groupFileCountDF = spark.createDataFrame(rdd, fileCountSchema)

    val uniqueBlocks = allBlocks.flatten.toSet.toSeq
    showGroupStats(groups, uniqueBlocks, allBlocks.map(_.size).max, tol)

    (groupFileCountDF, uniqueBlocks.map(sourcePath + _.path))
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

  def showGroupStats(
      groups: Seq[Row],
      uniqueBlocks: Seq[QbeastBlock],
      maxBlock: Long,
      tol: Double): Unit = {
    val size = ((uniqueBlocks.foldLeft(0d) { case (acc, b) =>
      acc + b.size / 1024d / 1024d
    } / 1024d) * 10).toInt / 10d

    var isValid = 0
    var notValid = 0
    groups.foreach(r => if (r.getDouble(5) < 1.0) isValid += 1 else notValid += 1)
    println(s"Valid fractions: $isValid, invalid fractions: $notValid")

    println(s"Number of files to read for t=$tol: ${uniqueBlocks.size}, total size: ${size}GB")
    println(s"Max number of cubes read for a given group: $maxBlock")

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

    val sourceDf = spark.read
      .format("parquet")
      .load(sourcePath)

    spark.time(
      sourceDf
        .where(s"$targetColumn <= $upperBound")
        .write
        .format("qbeast")
        .option("analyzerImp", imp)
        .option("columnsToIndex", columnsToIndex)
        .option("cubeSize", dcs)
        .save(targetPath))

    Script.showMetrics(targetPath)
  }

}
