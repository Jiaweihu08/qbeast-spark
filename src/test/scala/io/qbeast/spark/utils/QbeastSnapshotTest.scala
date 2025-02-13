/*
 * Copyright 2021 Qbeast Analytics, S.L.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.qbeast.spark.utils

import io.qbeast.core.model.QTableID
import io.qbeast.core.model.QbeastOptions
import io.qbeast.spark.index.SparkRevisionFactory
import io.qbeast.QbeastIntegrationTestSpec
import io.qbeast.TestClasses.Client3
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.functions.input_file_name
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SparkSession

class QbeastSnapshotTest extends QbeastIntegrationTestSpec {

  def createDF(size: Int): Dataset[Client3] = {
    val spark = SparkSession.active
    import spark.implicits._

    spark
      .range(size)
      .map(i => Client3(i * i, s"student-$i", i.intValue(), i * 2, i * i))

  }

  "QbeastSnapshot" should
    "load last index status correctly" in withQbeastContextSparkAndTmpDir { (spark, tmpDir) =>
      {
        val df = createDF(1000)
        val names = List("age", "val2")
        val cubeSize = 10
        df.write
          .format("qbeast")
          .mode("overwrite")
          .options(Map("columnsToIndex" -> names.mkString(","), "cubeSize" -> cubeSize.toString))
          .save(tmpDir)

        val qbeastSnapshot = getQbeastSnapshot(tmpDir)
        val indexStatus = qbeastSnapshot.loadLatestIndexStatus
        val revision = indexStatus.revision

        revision.revisionID shouldBe 1L
        qbeastSnapshot.loadIndexStatus(revision.revisionID) shouldBe indexStatus
        val latestRevisionID = qbeastSnapshot.loadLatestRevision.revisionID
        qbeastSnapshot.loadIndexStatus(latestRevisionID) shouldBe indexStatus
      }
    }

  it should "load last revision correctly" in
    withQbeastContextSparkAndTmpDir { (spark, tmpDir) =>
      {
        val df = createDF(1000)
        val names = List("age", "val2")
        val cubeSize = 10
        val options =
          Map("columnsToIndex" -> names.mkString(","), "cubeSize" -> cubeSize.toString)
        df.write
          .format("qbeast")
          .mode("overwrite")
          .options(options)
          .save(tmpDir)

        val qbeastSnapshot = getQbeastSnapshot(tmpDir)
        val columnTransformers = SparkRevisionFactory
          .createNewRevision(QTableID(tmpDir), df.schema, QbeastOptions(options))
          .columnTransformers

        val revision = qbeastSnapshot.loadLatestRevision
        revision.revisionID shouldBe 1L
        revision.tableID shouldBe QTableID(tmpDir)
        revision.columnTransformers.map(_.columnName) shouldBe names
        revision.desiredCubeSize shouldBe cubeSize
        revision.columnTransformers shouldBe columnTransformers

      }
    }

  it should "load revision at certain timestamp" in
    withQbeastContextSparkAndTmpDir { (spark, tmpDir) =>
      {
        val df = createDF(1000)
        val names = List("age", "val2")
        val cubeSize = 10
        val options =
          Map("columnsToIndex" -> names.mkString(","), "cubeSize" -> cubeSize.toString)
        df.write
          .format("qbeast")
          .mode("overwrite")
          .options(options)
          .save(tmpDir)

        val qbeastSnapshot = getQbeastSnapshot(tmpDir)
        val timestamp = System.currentTimeMillis()
        qbeastSnapshot.loadRevisionAt(timestamp) shouldBe qbeastSnapshot.loadLatestRevision

      }
    }

  it should "throw an exception when no revision satisfy timestamp requirement" in
    withQbeastContextSparkAndTmpDir { (spark, tmpDir) =>
      {
        val invalidRevisionTimestamp = System.currentTimeMillis()

        val df = createDF(1000)
        val names = List("age", "val2")
        val cubeSize = 10
        val options =
          Map("columnsToIndex" -> names.mkString(","), "cubeSize" -> cubeSize.toString)

        df.write
          .format("qbeast")
          .mode("overwrite")
          .options(options)
          .save(tmpDir)

        val qbeastSnapshot = getQbeastSnapshot(tmpDir)
        an[AnalysisException] shouldBe thrownBy(
          qbeastSnapshot.loadRevisionAt(invalidRevisionTimestamp))

      }
    }

  "loadDataframeFromData" should " correct the a table from a list of files" in {
    withSparkAndTmpDir { (spark, tmpDir) =>
      val df = createDF(10000)
      val names = List("age", "val2")
      val cubeSize = 100
      val options =
        Map("columnsToIndex" -> names.mkString(","), "cubeSize" -> cubeSize.toString)
      df.write
        .format("qbeast")
        .mode("overwrite")
        .options(options)
        .save(tmpDir)

      import spark.implicits._

      val qbeastSnapshot = getQbeastSnapshot(tmpDir)
      val lastRev = qbeastSnapshot.loadLatestRevision
      val filesToOptimize = qbeastSnapshot.loadIndexFiles(lastRev.revisionID)
      val cubeStatus = qbeastSnapshot.loadIndexStatus(lastRev.revisionID).cubesStatuses
      cubeStatus.size should be > 50
      val allData = qbeastSnapshot.loadDataframeFromIndexFiles(filesToOptimize)
      allData.count.toInt shouldBe 10000

      val data = spark.read.format("qbeast").load(tmpDir)
      val (filePath, fileSize) =
        data.groupBy(input_file_name()).count().as[(String, Long)].first()
      val fileName = new Path(filePath).getName
      val subSet =
        qbeastSnapshot.loadDataframeFromIndexFiles(filesToOptimize.filter(_.path == fileName))

      subSet.count shouldBe fileSize

    }
  }

}
