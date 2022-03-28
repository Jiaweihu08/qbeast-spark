package io.qbeast.spark.index

import io.qbeast.spark.{QbeastIntegrationTestSpec, QbeastTable}

class SingleVsDoublePassAnalyzerComparison extends QbeastIntegrationTestSpec {
  val dataSource = "./src/test/resources/ecommerce100k_2019_Oct.csv"

  it should "index the data correctly" in withQbeastContextSparkAndTmpDir { (spark, tmpDir) =>
    {
      val df = spark.read
        .format("csv")
        .option("header", true)
        .option("inferSchema", true)
        .load(dataSource)

      df.write
        .format("qbeast")
        .option("columnsToIndex", "event_time,user_id,price")
        .option("analyzerImp", "single")
        .option("cubeSize", 5000)
        .save(tmpDir)

      // scalastyle:off println
//      val metrics = QbeastTable.forPath(spark, tmpDir).getIndexMetrics()
//      println(metrics)
//      metrics.cubeStatuses.values.toList
//        .map(c => (c.cubeId, c.normalizedWeight, c.files.map(f => f.elementCount).sum))
//        .foreach(println)

      val qdf = spark.read.format("qbeast").load(tmpDir)
      df.count() shouldBe qdf.count()
    }
  }
}
