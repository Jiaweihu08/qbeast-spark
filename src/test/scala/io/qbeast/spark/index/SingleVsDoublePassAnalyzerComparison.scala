package io.qbeast.spark.index

import io.qbeast.spark.{QbeastIntegrationTestSpec}

class SingleVsDoublePassAnalyzerComparison extends QbeastIntegrationTestSpec {
  val dataSource = "./src/test/resources/ecommerce100k_2019_Oct.csv"

  it should "compare metrics(Single vs. DoublePass)" in withQbeastContextSparkAndTmpDir {
    (spark, tmpDir) =>
      {
        val singlePath = tmpDir + "/single"
        val doublePath = tmpDir + "/double"
        val cs = 2000

        val df = spark.read
          .format("csv")
          .option("header", true)
          .option("inferSchema", true)
          .load(dataSource)

        df.write
          .format("qbeast")
          .option("columnsToIndex", "event_time,user_id,price,product_id")
          .option("analyzerImp", "single")
          .option("cubeSize", cs)
          .save(singlePath)

        df.write
          .format("qbeast")
          .option("columnsToIndex", "event_time,user_id,price,product_id")
          .option("cubeSize", cs)
          .save(doublePath)

        // scalastyle:off println
//        Seq(singlePath, doublePath).foreach(path => {
//          val metrics = QbeastTable.forPath(spark, path).getIndexMetrics()
//          println(metrics)
//          val targetMetric = metrics.cubeStatuses.values.toList
//            .filter(c => {
//              val isInnerCube = c.cubeId.children.exists(metrics.cubeStatuses.contains)
//              val exceedsCs = c.files.map(_.elementCount).sum >= cs
//              val largeWeight = c.normalizedWeight >= 1.0
//              !isInnerCube && (exceedsCs || !largeWeight)
//            })
//            .map(c => (c.cubeId, c.normalizedWeight, c.files.map(f => f.elementCount).sum))
//          println(s"Mean leaf weight: ${targetMetric.map(_._2).sum / targetMetric.size}")
//          println(s"Leaf count: ${targetMetric.size}")
//        })

        val qdf = spark.read.format("qbeast").load(singlePath)
        df.count() shouldBe qdf.count()
      }
  }
}
