package io.qbeast.spark.index

import io.qbeast.spark.{QbeastIntegrationTestSpec, QbeastTable}

class SequentialAnalyzerTest extends QbeastIntegrationTestSpec {

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

      // scalastyle:off println
      val metrics = QbeastTable
        .forPath(spark, tmpDir)
        .getIndexMetrics()

      println(metrics)

      metrics.cubeStatuses.values.toList
        .sortBy(_.cubeId)
        .map(status => (status.cubeId, status.normalizedWeight, status.files.size))
        .foreach(println)
    }

  it should "write data correctly using piecewise Sequential imp and show metrics" in
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
        .option("analyzerImp", "piecewiseSeq")
        .save(tmpDir)

      // scalastyle:off println
      val metrics = QbeastTable
        .forPath(spark, tmpDir)
        .getIndexMetrics()

      println(metrics)

      metrics.cubeStatuses.values.toList
        .sortBy(_.cubeId)
        .map(status => (status.cubeId, status.normalizedWeight, status.files.size))
        .foreach(println)

      df.count() shouldBe spark.read.format("qbeast").load(tmpDir).count()
    }
}
