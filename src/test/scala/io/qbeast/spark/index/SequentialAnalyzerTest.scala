package io.qbeast.spark.index

import io.qbeast.spark.QbeastIntegrationTestSpec

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

  "Sequential analyzer" should "write 100k ecommerce data using sequential implementation" in
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
    }

//  "generateOTreeIndex" should "index data correctly" in withSpark { spark =>
//    val data = createDF(10000, spark)
//    val columnTransformers = createTransformers(data.schema)
//
//    val df = data.toDF()
//
//    val emptyRevision =
//      Revision(0, 1000, QTableID("test"), 1000, columnTransformers, Seq.empty.toIndexedSeq)
//
//    val dataFrameStats = getDataFrameStats(df, columnTransformers)
//    val revision =
//      calculateRevisionChanges(dataFrameStats, emptyRevision).get.createNewRevision
//
//    val numElements = dataFrameStats.getAs[Long]("count")
//    val dimensionCount = revision.columnTransformers.size
//    val maxOTreeHeight = calculateTreeDepth(numElements, revision.desiredCubeSize, dimensionCount)
//
//    val dataWithWeightAndCube = df
//      .transform(addRandomWeight(revision))
//      .transform(addCubeId(revision, maxOTreeHeight))
//
//    val dfSchema = df.schema.add(StructField(cubeColumnName, BinaryType, nullable = true))
//    val indexedData = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], dfSchema)
//
//    val (qbeastData, cubeWeightsDS) = generateOTreeIndex(
//      dataWithWeightAndCube,
//      revision.desiredCubeSize,
//      dimensionCount,
//      maxOTreeHeight,
//      0,
//      indexedData)
//
//    val cubeIdsFromData = qbeastData
//      .collect()
//      .map { row =>
//        val bytes = row.getAs[Array[Byte]](cubeColumnName)
//        CubeId(dimensionCount, bytes)
//      }
//      .toSet
//
//    val cubeWeights = cubeWeightsDS.collect()
//
//    val cubeWeightsMap = cubeWeights.toMap
//
//    cubeIdsFromData.size shouldBe cubeWeightsMap.size
//    cubeWeights.length shouldBe cubeWeightsMap.size
//    cubeIdsFromData.forall(c => cubeWeightsMap.contains(c))
//  }

}