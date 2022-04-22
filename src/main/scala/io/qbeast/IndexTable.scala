/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast

import io.qbeast.spark.QbeastTable
import org.apache.spark.sql.SparkSession

// scalastyle:off
object IndexTable {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().getOrCreate()

    val sourcePath =
      "s3a://qbeast-benchmarking-us-east-1/datasets/1000gb/delta/original-1tb-delta/store_sales/"

    val cubeSize = 5000000
    val imp = "piecewiseSeq"
    val targetPath: String =
      s"""
         |s3a://qbeast-benchmarking-us-east-1/jiaweiTmp/tpc-ds/1tb/no-repartition/
         |$imp/50gbDMFilterAll/$cubeSize
         |""".stripMargin.replaceAll("\n", "")
    val columnsToIndex = "ss_sold_date_sk,ss_item_sk,ss_store_sk"

    val df = spark.read.format("delta").load(sourcePath).na.drop()

    spark.time(
      df.write
        .format("qbeast")
        .option("columnsToIndex", columnsToIndex)
        .option("cubeSize", cubeSize)
        .option("analyzerImp", imp)
        .save(targetPath))

    showMetrics(targetPath)
  }

  def showMetrics(path: String): Unit = {
    val spark = SparkSession.active
    val metrics = QbeastTable.forPath(spark, path).getIndexMetrics()
    println(metrics)
    println("Average maxWeight per level.")
    metrics.cubeStatuses
      .groupBy(cw => cw._1.depth)
      .mapValues(m => m.values.map(st => st.normalizedWeight).sum / m.size)
      .toSeq
      .sortBy(_._1)
      .foreach(println)
  }

}
