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

import io.qbeast.spark.index.SparkColumnsToIndexSelector
import io.qbeast.table.QbeastTable
import io.qbeast.QbeastIntegrationTestSpec
import io.qbeast.TestClasses.Student
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession

import scala.util.Random

class QbeastSQLIntegrationTest extends QbeastIntegrationTestSpec {

  private val students = 1.to(10).map(i => Student(i, i.toString, Random.nextInt()))

  private def createTestData(spark: SparkSession): DataFrame = {
    import spark.implicits._
    students.toDF()
  }

  "QbeastSpark SQL" should "support CREATE TABLE" in withQbeastContextSparkAndTmpWarehouse(
    (spark, _) => {
      val data = createTestData(spark)
      data.createOrReplaceTempView("data")

      spark.sql(
        "CREATE TABLE student (id INT, name STRING, age INT) USING qbeast " +
          "OPTIONS ('columnsToIndex'='id')")

      val nonTemporaryTables = spark.sql("SHOW TABLES FROM default")
      nonTemporaryTables.count() shouldBe 2 // data table and student table

      val table = spark.sql("DESCRIBE TABLE EXTENDED student")
      // TODO Check the metadata of the table
      // Check provider
      table
        .where("col_name == 'Provider'")
        .select("data_type")
        .first()
        .getString(0) shouldBe "qbeast"
      // Check Table Properties
      val tableProperties = table
        .where("col_name == 'Table Properties'")
        .select("data_type")
        .first()
        .getString(0)
      tableProperties should include("columnsToIndex=id")
      tableProperties should include("option.columnsToIndex=id")

    })

  it should "support INSERT INTO" in withQbeastContextSparkAndTmpWarehouse((spark, _) => {
    val data = createTestData(spark)
    data.createOrReplaceTempView("data")

    spark.sql(
      "CREATE TABLE student (id INT, name STRING, age INT) USING qbeast " +
        "OPTIONS ('columnsToIndex'='id')")

    spark.sql("INSERT INTO table student SELECT * FROM data")

    val indexed = spark.sql("SELECT * FROM student")

    indexed.count() shouldBe data.count()

    indexed.columns.toSet shouldBe data.columns.toSet

    assertSmallDatasetEquality(indexed, data, orderedComparison = false, ignoreNullable = true)

  })

  it should "support CREATE TABLE AS SELECT statement" in withQbeastContextSparkAndTmpWarehouse(
    (spark, _) => {

      val data = createTestData(spark)
      data.createOrReplaceTempView("data")

      spark.sql(
        "CREATE OR REPLACE TABLE student USING qbeast " +
          "OPTIONS ('columnsToIndex'='id') " +
          "AS SELECT * FROM data;")

      val indexed = spark.sql("SELECT * FROM student")

      indexed.count() shouldBe data.count()

      indexed.columns.toSet shouldBe data.columns.toSet

      assertSmallDatasetEquality(indexed, data, orderedComparison = false, ignoreNullable = true)

    })

  it should "work with LOCATION" in withQbeastContextSparkAndTmpDir((spark, tmpDir) => {

    val data = createTestData(spark)
    data.createOrReplaceTempView("data")

    spark.sql(
      "CREATE OR REPLACE TABLE student USING qbeast " +
        "OPTIONS ('columnsToIndex'='id') " +
        s"LOCATION '$tmpDir' " +
        "AS SELECT * FROM data;")

    val indexed = spark.sql("SELECT * FROM student")

    indexed.count() shouldBe data.count()

    indexed.columns.toSet shouldBe data.columns.toSet

    assertSmallDatasetEquality(indexed, data, orderedComparison = false, ignoreNullable = true)

  })

  it should "create EXTERNAL table" in withQbeastContextSparkAndTmpDir((spark, tmpDir) => {
    spark.sql(
      "CREATE EXTERNAL TABLE student (id INT, name STRING, age INT) " +
        "USING qbeast " +
        "OPTIONS ('columnsToIndex'='id') " +
        s"LOCATION '$tmpDir'")

    val table = spark.sql("SELECT * from student")

    table.count() shouldBe 0

    table.columns.toSet shouldBe Set("id", "name", "age")

  })

  it should "throw an error when using different path locations" in
    withQbeastContextSparkAndTmpWarehouse((spark, tmpDir) => {

      val data = createTestData(spark)
      data.createOrReplaceTempView("data")

      an[AnalysisException] shouldBe thrownBy(
        spark.sql(
          "CREATE OR REPLACE TABLE student USING qbeast " +
            s"OPTIONS ('columnsToIndex'='id','location'='$tmpDir/new') " +
            s"LOCATION '$tmpDir' "))

    })

  it should "support INSERT INTO on a managed Table" in
    withQbeastContextSparkAndTmpWarehouse { (spark, _) =>
      {
        import spark.implicits._

        val data = createTestData(spark)
        val dataToInsert = Seq(Student(90, "qbeast", 2)).toDF()

        data.write
          .format("qbeast")
          .option("columnsToIndex", "id,name")
          .saveAsTable("students")

        dataToInsert.createOrReplaceTempView("toInsert")

        spark.sql("INSERT INTO students TABLE toInsert")
        spark.sql("SELECT * FROM students").count shouldBe 1 + data.count
        spark.sql("SELECT * FROM students WHERE name == 'qbeast'").count shouldBe 1

      }
    }

  it should "support INSERT OVERWRITE on a managed Table" in
    withQbeastContextSparkAndTmpWarehouse { (spark, _) =>
      {
        import spark.implicits._

        val data = createTestData(spark)
        val dataToInsert = Seq(Student(90, "qbeast", 2)).toDF()

        data.write
          .format("qbeast")
          .option("columnsToIndex", "id,name")
          .saveAsTable("students")

        dataToInsert.createOrReplaceTempView("toInsert")

        spark.sql("INSERT OVERWRITE students TABLE toInsert")
        spark.sql("SELECT * FROM students").count shouldBe 1
        spark.sql("SELECT * FROM students WHERE name == 'qbeast'").count shouldBe 1
      }
    }

  it should "work without providing columnsToIndex" in withExtendedSparkAndTmpDir(
    sparkConfWithSqlAndCatalog.set("spark.qbeast.index.columnsToIndex.auto", "true")) {
    (spark, tmpDir) =>
      {
        val data = createTestData(spark)
        data.createOrReplaceTempView("data")

        spark.sql(
          "CREATE OR REPLACE TABLE student USING qbeast " +
            s"LOCATION '$tmpDir' " +
            "AS SELECT * FROM data;")

        val autoColumnsToIndex = SparkColumnsToIndexSelector.selectColumnsToIndex(data)

        val qbeastTable = QbeastTable.forPath(spark, tmpDir)
        qbeastTable.indexedColumns() shouldBe autoColumnsToIndex
        qbeastTable.latestRevisionID shouldBe 1L

      }
  }

  it should "work with other namespaces" in withQbeastContextSparkAndTmpWarehouse { (spark, _) =>
    {
      spark.sql("CREATE DATABASE IF NOT EXISTS test")
      spark.sql(
        "CREATE TABLE IF NOT EXISTS test.students(id INT, name STRING, age INT) " +
          "USING qbeast OPTIONS ('columnsToIndex'='id')")

      val data = createTestData(spark)
      data.write.format("qbeast").mode("append").insertInto("test.students")

      assertSmallDatasetEquality(
        spark.sql("SELECT * FROM test.students"),
        data,
        ignoreNullable = true)

    }
  }

}
