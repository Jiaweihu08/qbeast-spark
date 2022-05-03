package io.qbeast.spark.index

import io.qbeast.spark.{QbeastIntegrationTestSpec, QbeastTable}
import org.apache.spark.sql.SparkSession

class SequentialAnalyzerTest extends QbeastIntegrationTestSpec {

  def showMetrics(spark: SparkSession, path: String): Unit = {
    // scalastyle:off println

    val metrics = QbeastTable.forPath(spark, path).getIndexMetrics()

    println(metrics)
    println("Average maxWeight per level.")

    metrics.cubeStatuses
      .groupBy(cw => cw._1.depth)
      .mapValues(m => m.values.map(st => st.maxWeight.fraction).sum / m.size)
      .toSeq
      .sortBy(_._1)
      .foreach(println)
  }

  it should "write data correctly using Sequential analyzer and show metrics" in
    withSparkAndTmpDir { (spark, tmpDir) =>
      val path = "./src/test/resources/ecommerce100k_2019_Oct.csv"
      val df =
        spark.read
          .format("csv")
          .option("header", true)
          .option("inferSchema", true)
          .load(path)

      df.write
        .format("qbeast")
        .option("columnsToIndex", "event_time,user_id,price")
        .option("cubeSize", 50000)
        .option("analyzerImp", "sequential")
        .save(tmpDir)

      showMetrics(spark, tmpDir)

      df.count() shouldBe spark.read.format("qbeast").load(tmpDir).count()
    }

  it should "write data correctly using piecewise Sequential imp and show metrics" in
    withSparkAndTmpDir { (spark, tmpDir) =>
      val path = "./src/test/resources/ecommerce300k_2019_Nov.csv"
      val df =
        spark.read
          .format("csv")
          .option("header", true)
          .option("inferSchema", true)
          .load(path)

      spark.time(
        df.write
          .format("qbeast")
          .option("columnsToIndex", "event_time,user_id,price")
          .option("cubeSize", 50000)
          .option("analyzerImp", "piecewiseSeq")
          .save(tmpDir))

      showMetrics(spark, tmpDir)
      df.count() shouldBe spark.read.format("qbeast").load(tmpDir).count()
    }
}
