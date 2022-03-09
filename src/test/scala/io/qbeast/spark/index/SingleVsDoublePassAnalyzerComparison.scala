package io.qbeast.spark.index

import io.qbeast.spark.QbeastIntegrationTestSpec

class SingleVsDoublePassAnalyzerComparison extends QbeastIntegrationTestSpec {
  val dataSource = "./src/test/resources/ecommerce100k_2019_Oct.csv"

  // scalastyle:off println
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
        .option("cubeSize", 5000)
        .save(tmpDir)

      val qdf = spark.read.format("qbeast").load(tmpDir)
      df.count() shouldBe qdf.count()
    }
  }
}
