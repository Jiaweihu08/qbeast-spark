/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast

import io.qbeast.spark.QbeastTable
import org.apache.spark.sql.{DataFrame, SparkSession}

// scalastyle:off
object IndexTable {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().getOrCreate()

    val sourcePath =
      "s3a://qbeast-benchmarking-us-east-1/datasets/1000gb/delta/original-1tb-delta/store_sales/"
    val df = spark.read.format("delta").load(sourcePath).na.drop()

    val desiredCubeSizes = 5000000 :: 3000000 :: 500000 :: Nil
    val implementations = "single" :: Nil // :: "double"

    implementations.foreach { imp =>
      desiredCubeSizes.foreach { dcs =>
        val path = index(imp, dcs, df)
        showMetrics(path)
      }
    }

  }

  def showMetrics(path: String): Unit = {
    val spark = SparkSession.active
    val metrics = QbeastTable.forPath(spark, path).getIndexMetrics()
    println(metrics)
    println("Average maxWeight per level.")
    metrics.cubeStatuses
      .groupBy(cw => cw._1.depth)
      .mapValues(m => (m.values.map(st => st.normalizedWeight).sum / m.size, m.size))
      .toSeq
      .sortBy(_._1)
      .foreach(println)
  }

  def index(imp: String, desiredCubeSize: Int, df: DataFrame): String = {
    val spark: SparkSession = SparkSession.builder().getOrCreate()
    val columnsToIndex = "ss_sold_date_sk,ss_item_sk,ss_store_sk"
    val targetPath: String =
      s"""
         |s3://qbeast-benchmarking-us-east-1/jiaweiTmp/tpc-ds/1tb/no-repartition/
         |estimated-used-cubes-ratio-harcodedElemCount/$imp/$desiredCubeSize
         |""".stripMargin.replaceAll("\n", "")

    // scalastyle: off
    println(s">>> Indexing table using $imp, desiredCubeSize=$desiredCubeSize")
    spark.time(
      df.write
        .format("qbeast")
        .option("columnsToIndex", columnsToIndex)
        .option("cubeSize", desiredCubeSize)
        .option("analyzerImp", imp)
        .save(targetPath))

    targetPath
  }

}
