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
package io.qbeast.catalog

import io.qbeast.QbeastIntegrationTestSpec
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.AnalysisException
import org.apache.spark.SparkConf

import scala.collection.JavaConverters._

class DefaultStagedTableTest extends QbeastIntegrationTestSpec with CatalogTestSuite {

  "A DefaultStagedTable" should "be returned by default" in withQbeastContextSparkAndTmpWarehouse(
    (spark, _) => {
      val qbeastCatalog = createQbeastCatalog(spark)
      val tableIdentifier = Identifier.of(defaultNamespace, "student")

      qbeastCatalog.stageCreate(
        tableIdentifier,
        columns,
        Array.empty[Transform],
        Map.empty[String, String].asJava) shouldBe a[DefaultStagedTable]

      qbeastCatalog.stageReplace(
        tableIdentifier,
        columns,
        Array.empty[Transform],
        Map.empty[String, String].asJava) shouldBe a[DefaultStagedTable]

      qbeastCatalog.stageCreateOrReplace(
        tableIdentifier,
        columns,
        Array.empty[Transform],
        Map.empty[String, String].asJava) shouldBe a[DefaultStagedTable]
    })

  it should "output table properties" in withQbeastContextSparkAndTmpWarehouse((spark, _) => {
    val tableIdentifier = Identifier.of(Array("default"), "students")
    val catalog = sessionCatalog(spark)
    val underlyingTable =
      catalog.createTable(
        tableIdentifier,
        columns,
        Array.empty[Transform],
        Map.empty[String, String].asJava)

    val defaultStagedTable = DefaultStagedTable
      .apply(tableIdentifier, underlyingTable, catalog)

    defaultStagedTable.properties() shouldBe underlyingTable.properties()
  })

  it should "output partitioning" in withQbeastContextSparkAndTmpWarehouse((spark, _) => {
    val tableIdentifier = Identifier.of(Array("default"), "students")
    val catalog = sessionCatalog(spark)
    val underlyingTable =
      catalog.createTable(
        tableIdentifier,
        columns,
        Array.empty[Transform],
        Map.empty[String, String].asJava)

    val defaultStagedTable = DefaultStagedTable
      .apply(tableIdentifier, underlyingTable, catalog)

    defaultStagedTable.partitioning() shouldBe underlyingTable.partitioning()
  })

  it should "output capabilities" in withQbeastContextSparkAndTmpWarehouse((spark, _) => {
    val tableIdentifier = Identifier.of(Array("default"), "students")
    val catalog = sessionCatalog(spark)
    val underlyingTable =
      catalog.createTable(
        tableIdentifier,
        columns,
        Array.empty[Transform],
        Map.empty[String, String].asJava)

    val defaultStagedTable = DefaultStagedTable
      .apply(tableIdentifier, underlyingTable, catalog)

    defaultStagedTable.capabilities() shouldBe underlyingTable.capabilities()
  })

  it should "output schema" in withQbeastContextSparkAndTmpWarehouse((spark, tmpWarehouse) => {
    val tableIdentifier = Identifier.of(Array("default"), "students")
    val catalog = sessionCatalog(spark)
    val underlyingTable =
      catalog.createTable(
        tableIdentifier,
        columns,
        Array.empty[Transform],
        Map.empty[String, String].asJava)

    val defaultStagedTable = DefaultStagedTable
      .apply(tableIdentifier, underlyingTable, catalog)

    defaultStagedTable.schema() shouldBe schema
    defaultStagedTable.columns() shouldBe columns
  })

  it should "output name" in withQbeastContextSparkAndTmpWarehouse((spark, tmpWarehouse) => {
    val tableIdentifier = Identifier.of(Array("default"), "students")
    val catalog = sessionCatalog(spark)
    val underlyingTable =
      catalog.createTable(
        tableIdentifier,
        columns,
        Array.empty[Transform],
        Map.empty[String, String].asJava)

    val defaultStagedTable = DefaultStagedTable
      .apply(tableIdentifier, underlyingTable, catalog)

    defaultStagedTable.name() shouldBe "students"
  })

  it should "drop table on abort" in withQbeastContextSparkAndTmpWarehouse(
    (spark, tmpWarehouse) => {
      val tableIdentifier = Identifier.of(Array("default"), "students")
      val catalog = sessionCatalog(spark)
      val underlyingTable = catalog.createTable(
        tableIdentifier,
        columns,
        Array.empty[Transform],
        Map.empty[String, String].asJava)

      val defaultStagedTable = DefaultStagedTable
        .apply(tableIdentifier, underlyingTable, catalog)

      defaultStagedTable.abortStagedChanges()
      catalog.listTables(defaultNamespace) shouldBe Array()
    })

  it should "commit the metadata on commit" in withQbeastContextSparkAndTmpWarehouse(
    (spark, tmpWarehouse) => {
      val tableIdentifier = Identifier.of(Array("default"), "students")
      val catalog = sessionCatalog(spark)
      val underlyingTable = catalog.createTable(
        tableIdentifier,
        columns,
        Array.empty[Transform],
        Map.empty[String, String].asJava)

      val defaultStagedTable = DefaultStagedTable
        .apply(tableIdentifier, underlyingTable, catalog)

      defaultStagedTable.commitStagedChanges()

      catalog.listTables(defaultNamespace) shouldBe Array(tableIdentifier)
    })

  it should "throw exception when creating default WriteBuilder" in
    withQbeastContextSparkAndTmpWarehouse((spark, tmpWarehouse) => {
      val tableIdentifier = Identifier.of(Array("default"), "students")
      val catalog = sessionCatalog(spark)
      val underlyingTable = catalog.createTable(
        tableIdentifier,
        columns,
        Array.empty[Transform],
        Map.empty[String, String].asJava)

      val defaultStagedTable = DefaultStagedTable
        .apply(tableIdentifier, underlyingTable, catalog)

      an[AnalysisException] shouldBe thrownBy(
        defaultStagedTable.newWriteBuilder(fakeLogicalWriteInfo))
    })

  it should "use the right builder when table SupportsWrites" in withTmpDir(tmpDir =>
    withExtendedSpark(sparkConf = new SparkConf()
      .setMaster("local[8]")
      .set("spark.sql.extensions", "io.qbeast.sql.QbeastSparkSessionExtension")
      .set("spark.sql.warehouse.dir", tmpDir)
      .set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .set("spark.sql.catalog.qbeast_catalog", "io.qbeast.catalog.QbeastCatalog"))(spark => {

      val tableIdentifier = Identifier.of(Array("default"), "students")
      val catalog = sessionCatalog(spark)
      val underlyingTable = catalog.createTable(
        tableIdentifier,
        columns,
        Array.empty[Transform],
        Map("provider" -> "delta").asJava)

      val defaultStagedTable = DefaultStagedTable
        .apply(tableIdentifier, underlyingTable, catalog)

      defaultStagedTable
        .newWriteBuilder(fakeLogicalWriteInfo)
        .build() shouldBe a[Any] // could be any type writeBuilder
    }))

}
