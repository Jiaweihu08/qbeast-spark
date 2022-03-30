package io.qbeast.spark.index

import io.qbeast.spark.{QbeastIntegrationTestSpec, QbeastTable}

class SequentialAnalyzerTest extends QbeastIntegrationTestSpec {

//  private def createDF(size: Int, spark: SparkSession): Dataset[T3] = {
//    import spark.implicits._
//
//    0.to(size)
//      .map(i => T3(i, i.toDouble, i.toString, i.toFloat))
//      .toDF()
//      .as[T3]
//
//  }
//
//  private def createTransformers(columnsSchema: Seq[StructField]): IISeq[Transformer] = {
//    columnsSchema
//      .map(field => Transformer(field.name, SparkToQTypesUtils.convertDataTypes(field.dataType)))
//      .toIndexedSeq
//  }

  "Sequential analyzer" should "write data correctly and show metrics" in
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

      val metrics = QbeastTable
        .forPath(spark, tmpDir)
        .getIndexMetrics()

      // scalastyle:off println
      println(metrics)

      metrics.cubeStatuses.values.toList
        .sortBy(_.cubeId)
        .foreach(status => (status.cubeId, status.normalizedWeight, status.files.size))
    }
}
