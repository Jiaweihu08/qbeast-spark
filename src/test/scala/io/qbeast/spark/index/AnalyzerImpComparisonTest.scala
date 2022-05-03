package io.qbeast.spark.index

import io.qbeast.spark.{QbeastIntegrationTestSpec, QbeastTable}
import org.apache.spark.sql.SparkSession

class AnalyzerImpComparisonTest extends QbeastIntegrationTestSpec {

  private def showMetrics(spark: SparkSession, path: String): Unit = {
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

  val dataSource = "./src/test/resources/ecommerce300k_2019_Nov.csv"

  it should "compare metrics between Single and DoublePass" in
    withQbeastContextSparkAndTmpDir { (spark, tmpDir) =>
      {
        val singlePath = tmpDir + "/single"
        val doublePath = tmpDir + "/double"

        val cs = 5000
        val columnsToIndex = "event_time,user_id,price,product_id"

        val df = spark.read
          .format("csv")
          .option("header", true)
          .option("inferSchema", true)
          .load(dataSource)

        spark.time(
          df.write
            .format("qbeast")
            .option("columnsToIndex", columnsToIndex)
            .option("analyzerImp", "single")
            .option("cubeSize", cs)
            .save(singlePath))

        spark.time(
          df.write
            .format("qbeast")
            .option("columnsToIndex", columnsToIndex)
            .option("cubeSize", cs)
            .save(doublePath))

        Seq(singlePath, doublePath).foreach(path => {
          showMetrics(spark, path)
        })

        val qdf = spark.read.format("qbeast").load(singlePath)
        df.count() shouldBe qdf.count()
      }
    }

  it should "compare metrics between piecewiseSeq and DoublePass" in
    withQbeastContextSparkAndTmpDir { (spark, tmpDir) =>
      {
        val sequentialPath = tmpDir + "/sequential"
        val doublePath = tmpDir + "/double"

        val cs = 5000
        val columnsToIndex = "event_time,user_id,price,product_id"

        val df = spark.read
          .format("csv")
          .option("header", true)
          .option("inferSchema", true)
          .load(dataSource)

        spark.time(
          df.write
            .format("qbeast")
            .option("columnsToIndex", columnsToIndex)
            .option("analyzerImp", "piecewiseSeq")
            .option("cubeSize", cs)
            .save(sequentialPath))

        spark.time(
          df.write
            .format("qbeast")
            .option("columnsToIndex", columnsToIndex)
            .option("analyzerImp", "double")
            .option("cubeSize", cs)
            .save(doublePath))

        Seq(sequentialPath, doublePath).foreach(path => {
          showMetrics(spark, path)
        })

        val qdf = spark.read.format("qbeast").load(sequentialPath)
        df.count() shouldBe qdf.count()
      }
    }
}
