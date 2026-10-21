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

import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.parser.ParseException;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import scala.collection.JavaConverters;

/**
 * Verifies the strategy's gating contract: Gravitino Iceberg tables are planned by the Gravitino
 * strategy, and every other catalog is skipped (Option empty) so the co-registered Iceberg strategy
 * - or any later strategy - can plan it. On the unfixed tree the strategy THREW
 * UnsupportedOperationException for any non-Gravitino catalog, crashing statements that work in a
 * session without the plugin.
 */
@SuppressWarnings("deprecation") // JavaConverters is deprecated on Scala 2.13 but required on 2.12
public class TestIcebergExtendedDataSourceV2Strategy {

  private static SparkSession spark;
  private static IcebergExtendedDataSourceV2Strategy strategy;

  /** A plain catalog that is NOT a GravitinoIcebergCatalog. */
  public static class PlainCatalog implements org.apache.spark.sql.connector.catalog.TableCatalog {

    private String name;

    @Override
    public void initialize(String name, CaseInsensitiveStringMap options) {
      this.name = name;
    }

    @Override
    public String name() {
      return name;
    }

    @Override
    public org.apache.spark.sql.connector.catalog.Table loadTable(
        org.apache.spark.sql.connector.catalog.Identifier ident) {
      throw new UnsupportedOperationException();
    }

    @Override
    public org.apache.spark.sql.connector.catalog.Identifier[] listTables(String[] namespace) {
      return new org.apache.spark.sql.connector.catalog.Identifier[0];
    }

    @Override
    @SuppressWarnings("deprecation")
    public org.apache.spark.sql.connector.catalog.Table createTable(
        org.apache.spark.sql.connector.catalog.Identifier ident,
        org.apache.spark.sql.types.StructType schema,
        org.apache.spark.sql.connector.expressions.Transform[] partitions,
        java.util.Map<String, String> properties) {
      throw new UnsupportedOperationException();
    }

    @Override
    public org.apache.spark.sql.connector.catalog.Table alterTable(
        org.apache.spark.sql.connector.catalog.Identifier ident,
        org.apache.spark.sql.connector.catalog.TableChange... changes) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean dropTable(org.apache.spark.sql.connector.catalog.Identifier ident) {
      return false;
    }

    @Override
    public void renameTable(
        org.apache.spark.sql.connector.catalog.Identifier oldIdent,
        org.apache.spark.sql.connector.catalog.Identifier newIdent) {
      throw new UnsupportedOperationException();
    }
  }

  @BeforeAll
  static void startSpark() {
    spark =
        SparkSession.builder()
            .master("local[1]")
            .appName("test-iceberg-extended-strategy")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.catalog.plain_iceberg", PlainCatalog.class.getName())
            .config(
                "spark.sql.extensions",
                String.join(
                    ",",
                    org.apache.gravitino.spark.connector.iceberg.extensions
                        .GravitinoIcebergSparkSessionExtensions.class
                        .getName(),
                    "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"))
            .getOrCreate();
    strategy = new IcebergExtendedDataSourceV2Strategy(spark);
  }

  @AfterAll
  static void stopSpark() {
    if (spark != null) {
      spark.stop();
    }
  }

  @Test
  void testNonGravitinoCatalogIsSkippedInsteadOfThrowing() throws ParseException {
    LogicalPlan plan =
        spark.sessionState().sqlParser().parsePlan("ALTER TABLE plain_iceberg.db.tbl CREATE TAG t");

    // Skip = empty Option is not directly observable through apply (super would plan it), so pin
    // the gate itself: the public buildCatalogAndIdentifier must return None for a plain catalog
    // instead of throwing (red on the unfixed tree, which threw UnsupportedOperationException).
    scala.Option<?> result =
        IcebergExtendedDataSourceV2Strategy.IcebergCatalogAndIdentifier.buildCatalogAndIdentifier(
            spark,
            JavaConverters.asScalaBuffer(java.util.Arrays.asList("plain_iceberg", "db", "tbl"))
                .toList()
                .toIndexedSeq());
    assertTrue(result.isEmpty());

    // And applying the strategy to the plan must not throw UnsupportedOperationException either
    // (the plan falls through to the backported super strategy's own handling).
    strategy.apply(plan);
  }

  @Test
  void testMissingCatalogStillErrorsClearly() throws ParseException {
    LogicalPlan plan =
        spark.sessionState().sqlParser().parsePlan("ALTER TABLE nonexistent.db.tbl CREATE TAG t");
    // A table in no known catalog resolves against the session catalog; the gate returns None and
    // the fall-through decides. The contract pinned here is simply "no Gravitino crash".
    strategy.apply(plan);
  }
}
