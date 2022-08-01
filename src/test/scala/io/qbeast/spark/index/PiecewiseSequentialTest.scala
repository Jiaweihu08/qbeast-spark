package io.qbeast.spark.index

import io.qbeast.spark.{QbeastIntegrationTestSpec, QbeastTable}

class PiecewiseSequentialTest extends QbeastIntegrationTestSpec {
  val dataSource = "./src/test/resources/ecommerce300k_2019_Nov.csv"

  "Piecewise write" should "index correctly" in withQbeastContextSparkAndTmpDir {
    (spark, tmpDir) =>
      {
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
            .option("cubeSize", cs)
            .save(tmpDir))
      }
      val metrics = QbeastTable.forPath(spark, tmpDir).getIndexMetrics()

      // scalastyle:off println
      println(metrics)
  }
}
