/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.gravitino.spark.connector.iceberg.extensions;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.gravitino.spark.connector.iceberg.GravitinoIcebergCatalog;
import org.apache.gravitino.spark.connector.iceberg.extensions.IcebergExtendedDataSourceV2Strategy.IcebergCatalogAndIdentifier;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.Table;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.spark.SparkCatalog;
import org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedStatic;
import scala.collection.JavaConverters;
import scala.collection.immutable.Seq;

/**
 * Tests {@link IcebergExtendedDataSourceV2Strategy} on an Iceberg catalog that Gravitino does not
 * manage.
 */
public class TestIcebergExtendedDataSourceV2Strategy {

  @TempDir static Path warehouse;

  private static SparkSession spark;

  @BeforeAll
  static void startSpark() {
    // Same extension order as GravitinoDriverPlugin: the Gravitino strategy runs first.
    spark =
        SparkSession.builder()
            .master("local[1]")
            .appName("test-iceberg-extended-strategy")
            .config("spark.ui.enabled", "false")
            .config(
                "spark.sql.extensions",
                GravitinoIcebergSparkSessionExtensions.class.getName()
                    + ","
                    + IcebergSparkSessionExtensions.class.getName())
            .config("spark.sql.catalog.local", SparkCatalog.class.getName())
            .config("spark.sql.catalog.local.type", "hadoop")
            .config("spark.sql.catalog.local.warehouse", warehouse.toUri().toString())
            .getOrCreate();
  }

  @AfterAll
  static void stopSpark() {
    if (spark != null) {
      spark.stop();
    }
  }

  @Test
  void testBranchAndTagDdlOnPlainIcebergCatalog() {
    spark.sql("CREATE TABLE local.db.refs (id INT) USING iceberg");
    spark.sql("INSERT INTO local.db.refs VALUES (1)");

    spark.sql("ALTER TABLE local.db.refs CREATE TAG t1");
    spark.sql("ALTER TABLE local.db.refs CREATE BRANCH b1");
    assertEquals("[[b1], [main], [t1]]", refNames("local.db.refs"));

    spark.sql("ALTER TABLE local.db.refs DROP TAG t1");
    spark.sql("ALTER TABLE local.db.refs DROP BRANCH b1");
    assertEquals("[[main]]", refNames("local.db.refs"));
  }

  @Test
  void testPartitionFieldDdlOnPlainIcebergCatalog() throws Exception {
    spark.sql("CREATE TABLE local.db.spec (id INT, category STRING) USING iceberg");

    spark.sql("ALTER TABLE local.db.spec ADD PARTITION FIELD category");
    assertEquals(Collections.singletonList("category"), partitionFieldNames("local.db.spec"));

    spark.sql(
        "ALTER TABLE local.db.spec REPLACE PARTITION FIELD category WITH bucket(4, id) AS id_bucket");
    assertEquals(Collections.singletonList("id_bucket"), partitionFieldNames("local.db.spec"));

    spark.sql("ALTER TABLE local.db.spec DROP PARTITION FIELD id_bucket");
    assertTrue(partitionFieldNames("local.db.spec").isEmpty());
  }

  @Test
  void testIdentifierFieldsAndWriteOrderDdlOnPlainIcebergCatalog() throws Exception {
    spark.sql("CREATE TABLE local.db.ident (id INT NOT NULL, data STRING) USING iceberg");

    spark.sql("ALTER TABLE local.db.ident SET IDENTIFIER FIELDS id");
    assertEquals(
        Collections.singleton("id"), loadTable("local.db.ident").schema().identifierFieldNames());

    spark.sql("ALTER TABLE local.db.ident DROP IDENTIFIER FIELDS id");
    assertTrue(loadTable("local.db.ident").schema().identifierFieldNames().isEmpty());

    spark.sql("ALTER TABLE local.db.ident WRITE ORDERED BY id");
    assertTrue(loadTable("local.db.ident").sortOrder().isSorted());
  }

  @Test
  void testGravitinoIcebergCatalogTakesGravitinoBranch() {
    Identifier identifier = Identifier.of(new String[] {"db"}, "t");
    try (MockedStatic<Spark3Util> util = mockStatic(Spark3Util.class)) {
      util.when(() -> Spark3Util.catalogAndIdentifier(any(SparkSession.class), anyList()))
          .thenReturn(
              new Spark3Util.CatalogAndIdentifier(mock(GravitinoIcebergCatalog.class), identifier));
      assertTrue(
          IcebergCatalogAndIdentifier.buildCatalogAndIdentifier(spark, seq("cat", "db", "t"))
              .isDefined());

      util.when(() -> Spark3Util.catalogAndIdentifier(any(SparkSession.class), anyList()))
          .thenReturn(new Spark3Util.CatalogAndIdentifier(mock(SparkCatalog.class), identifier));
      assertFalse(
          IcebergCatalogAndIdentifier.buildCatalogAndIdentifier(spark, seq("cat", "db", "t"))
              .isDefined());
    }
  }

  private static String refNames(String table) {
    List<Row> refs = spark.sql("SELECT name FROM " + table + ".refs ORDER BY name").collectAsList();
    return refs.toString();
  }

  private static List<String> partitionFieldNames(String table) throws Exception {
    List<String> names = new ArrayList<>();
    for (PartitionField field : loadTable(table).spec().fields()) {
      names.add(field.name());
    }
    return names;
  }

  private static Table loadTable(String table) throws Exception {
    Table icebergTable = Spark3Util.loadIcebergTable(spark, table);
    icebergTable.refresh();
    return icebergTable;
  }

  private static Seq<String> seq(String... parts) {
    return JavaConverters.asScalaIteratorConverter(Arrays.asList(parts).iterator())
        .asScala()
        .toIndexedSeq();
  }
}
