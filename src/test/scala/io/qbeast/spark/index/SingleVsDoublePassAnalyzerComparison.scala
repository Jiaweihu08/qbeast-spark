package io.qbeast.spark.index

import io.qbeast.spark.{QbeastIntegrationTestSpec, QbeastTable}

class SingleVsDoublePassAnalyzerComparison extends QbeastIntegrationTestSpec {
  val dataSource = "./src/test/resources/ecommerce100k_2019_Oct.csv"

  it should "compare metrics(Single vs. DoublePass)" in withQbeastContextSparkAndTmpDir {
    (spark, tmpDir) =>
      {
        val singlePath = tmpDir + "/single"
        val doublePath = tmpDir + "/double"

        val df = spark.read
          .format("csv")
          .option("header", true)
          .option("inferSchema", true)
          .load(dataSource)

        df.write
          .format("qbeast")
          .option("columnsToIndex", "event_time,user_id,price")
          .option("analyzerImp", "single")
          .option("cubeSize", 50000)
          .save(singlePath)

        df.write
          .format("qbeast")
          .option("columnsToIndex", "event_time,user_id,price")
          .option("cubeSize", 50000)
          .save(doublePath)

        // scalastyle:off println
        val metricsSingle = QbeastTable.forPath(spark, singlePath).getIndexMetrics()
        println(metricsSingle)
        metricsSingle.cubeStatuses.values.toList
          .map(c => (c.cubeId, c.normalizedWeight, c.files.map(f => f.elementCount).sum))
          .foreach(println)

        val metricsDouble = QbeastTable.forPath(spark, singlePath).getIndexMetrics()
        println(metricsDouble)
        metricsSingle.cubeStatuses.values.toList
          .map(c => (c.cubeId, c.normalizedWeight, c.files.map(f => f.elementCount).sum))
          .foreach(println)

        val qdf = spark.read.format("qbeast").load(singlePath)
        df.count() shouldBe qdf.count()
      }
  }
}
