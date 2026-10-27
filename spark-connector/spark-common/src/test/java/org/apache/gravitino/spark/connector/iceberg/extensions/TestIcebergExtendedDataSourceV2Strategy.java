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

import java.nio.file.Path;
import java.util.List;
import org.apache.iceberg.spark.SparkCatalog;
import org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Tests {@link IcebergExtendedDataSourceV2Strategy} on catalogs that Gravitino does not manage. */
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
  void testIcebergDdlOnPlainIcebergCatalog() {
    spark.sql("CREATE TABLE local.db.t (id INT) USING iceberg");
    spark.sql("INSERT INTO local.db.t VALUES (1)");

    spark.sql("ALTER TABLE local.db.t CREATE TAG t1");
    spark.sql("ALTER TABLE local.db.t CREATE BRANCH b1");

    List<Row> refs = spark.sql("SELECT name FROM local.db.t.refs ORDER BY name").collectAsList();
    assertEquals("[[b1], [main], [t1]]", refs.toString());
  }
}
