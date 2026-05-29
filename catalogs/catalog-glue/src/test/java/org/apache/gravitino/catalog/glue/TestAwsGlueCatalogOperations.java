/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.catalog.glue;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.HashMap;
import java.util.Map;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.TableChange;
import org.apache.gravitino.rel.expressions.distributions.Distributions;
import org.apache.gravitino.rel.expressions.sorts.SortOrders;
import org.apache.gravitino.rel.expressions.transforms.Transforms;
import org.apache.gravitino.rel.indexes.Indexes;
import org.apache.gravitino.rel.types.Types;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

/**
 * Integration tests for {@link GlueCatalogOperations} against a real AWS Glue endpoint.
 *
 * <p>Only runs when {@code AWS_ACCESS_KEY_ID} is set. Required environment variables:
 *
 * <ul>
 *   <li>{@code AWS_ACCESS_KEY_ID}
 *   <li>{@code AWS_SECRET_ACCESS_KEY}
 *   <li>{@code AWS_DEFAULT_REGION} (e.g. {@code ap-northeast-1})
 *   <li>{@code GLUE_CATALOG_ID} (12-digit AWS account ID; optional)
 * </ul>
 */
@EnabledIfEnvironmentVariable(named = "AWS_ACCESS_KEY_ID", matches = ".+")
class TestAwsGlueCatalogOperations {

  private static GlueCatalogOperations ops;
  private static String s3TestBucket;
  private static final String TEST_SCHEMA = "aws_glue_ops_it_" + System.currentTimeMillis();
  private static final Namespace NS = Namespace.of("metalake", "catalog");

  @BeforeAll
  static void setup() {
    s3TestBucket = System.getenv("AWS_S3_TEST_BUCKET");
    if (s3TestBucket == null || s3TestBucket.isEmpty()) {
      throw new IllegalStateException(
          "AWS_S3_TEST_BUCKET is not set. Set it to a writable S3 bucket for Iceberg integration tests.");
    }

    Map<String, String> config = new HashMap<>();
    config.put(
        GlueConstants.AWS_REGION, System.getenv().getOrDefault("AWS_DEFAULT_REGION", "us-east-1"));
    String accessKey = System.getenv("AWS_ACCESS_KEY_ID");
    String secretKey = System.getenv("AWS_SECRET_ACCESS_KEY");
    if (accessKey != null) {
      config.put(GlueConstants.AWS_ACCESS_KEY_ID, accessKey);
    }
    if (secretKey != null) {
      config.put(GlueConstants.AWS_SECRET_ACCESS_KEY, secretKey);
    }
    String catalogId = System.getenv("GLUE_CATALOG_ID");
    if (catalogId != null) {
      config.put(GlueConstants.AWS_GLUE_CATALOG_ID, catalogId);
    }
    config.put(GlueConstants.WAREHOUSE, "s3://" + s3TestBucket + "/warehouse");

    ops = new GlueCatalogOperations();
    ops.initialize(config, null, null);

    ops.createSchema(NameIdentifier.of(NS, TEST_SCHEMA), "IT schema", Map.of());
  }

  @AfterAll
  static void teardown() {
    if (ops == null) {
      return;
    }
    try {
      NameIdentifier[] tables = ops.listTables(Namespace.of(NS.level(0), NS.level(1), TEST_SCHEMA));
      for (NameIdentifier t : tables) {
        ops.dropTable(t);
      }
    } catch (Exception ignored) {
    }
    try {
      ops.dropSchema(NameIdentifier.of(NS, TEST_SCHEMA), true);
    } catch (Exception ignored) {
    }
    ops.glueClient.close();
  }

  @Test
  void testRenameTableIsUnsupported() {
    String tableName = "rename_test_" + System.currentTimeMillis();
    NameIdentifier ident = NameIdentifier.of(NS.level(0), NS.level(1), TEST_SCHEMA, tableName);

    Column col = Column.of("id", Types.IntegerType.get(), "pk");
    ops.createTable(
        ident,
        new Column[] {col},
        "test table",
        Map.of(GlueConstants.FORMAT, "parquet"),
        Transforms.EMPTY_TRANSFORM,
        Distributions.NONE,
        SortOrders.NONE,
        Indexes.EMPTY_INDEXES);

    try {
      assertThrows(
          UnsupportedOperationException.class,
          () -> ops.alterTable(ident, TableChange.rename("new_name")));
    } finally {
      ops.dropTable(ident);
    }
  }

  @Test
  void testCreateIcebergTable() {
    String tableName = "iceberg_create_" + System.currentTimeMillis();
    NameIdentifier ident = NameIdentifier.of(NS.level(0), NS.level(1), TEST_SCHEMA, tableName);

    Column col = Column.of("id", Types.LongType.get(), "pk", false, false, null);
    Map<String, String> props = new HashMap<>();
    props.put(GlueConstants.TABLE_FORMAT, "ICEBERG");
    props.put(GlueConstants.LOCATION, "s3://" + s3TestBucket + "/iceberg/" + tableName);

    try {
      Table created =
          ops.createTable(
              ident,
              new Column[] {col},
              "iceberg table",
              props,
              Transforms.EMPTY_TRANSFORM,
              Distributions.NONE,
              SortOrders.NONE,
              Indexes.EMPTY_INDEXES);

      assertEquals("iceberg table", created.comment());
      assertEquals(1, created.columns().length);
      assertEquals("id", created.columns()[0].name());
      assertEquals(Types.LongType.get(), created.columns()[0].dataType());

      Table loaded = ops.loadTable(ident);
      assertEquals("ICEBERG", loaded.properties().get(GlueConstants.TABLE_FORMAT));
      assertNotNull(loaded.properties().get(GlueConstants.METADATA_LOCATION));
    } finally {
      ops.dropTable(ident);
    }
  }

  @Test
  void testAlterIcebergTableAddColumn() {
    String tableName = "iceberg_alter_col_" + System.currentTimeMillis();
    NameIdentifier ident = NameIdentifier.of(NS.level(0), NS.level(1), TEST_SCHEMA, tableName);

    Column col = Column.of("id", Types.LongType.get(), "pk", false, false, null);
    Map<String, String> props = new HashMap<>();
    props.put(GlueConstants.TABLE_FORMAT, "ICEBERG");
    props.put(GlueConstants.LOCATION, "s3://" + s3TestBucket + "/iceberg/" + tableName);

    try {
      ops.createTable(
          ident,
          new Column[] {col},
          "iceberg table",
          props,
          Transforms.EMPTY_TRANSFORM,
          Distributions.NONE,
          SortOrders.NONE,
          Indexes.EMPTY_INDEXES);

      ops.alterTable(
          ident, TableChange.addColumn(new String[] {"score"}, Types.DoubleType.get(), true));

      Table loaded = ops.loadTable(ident);
      assertEquals(2, loaded.columns().length);
      assertEquals("score", loaded.columns()[1].name());
      assertEquals(Types.DoubleType.get(), loaded.columns()[1].dataType());
    } finally {
      ops.dropTable(ident);
    }
  }

  @Test
  void testAlterIcebergTableSetProperty() {
    String tableName = "iceberg_alter_prop_" + System.currentTimeMillis();
    NameIdentifier ident = NameIdentifier.of(NS.level(0), NS.level(1), TEST_SCHEMA, tableName);

    Column col = Column.of("id", Types.LongType.get(), "pk", false, false, null);
    Map<String, String> props = new HashMap<>();
    props.put(GlueConstants.TABLE_FORMAT, "ICEBERG");
    props.put(GlueConstants.LOCATION, "s3://" + s3TestBucket + "/iceberg/" + tableName);

    try {
      ops.createTable(
          ident,
          new Column[] {col},
          "iceberg table",
          props,
          Transforms.EMPTY_TRANSFORM,
          Distributions.NONE,
          SortOrders.NONE,
          Indexes.EMPTY_INDEXES);

      ops.alterTable(ident, TableChange.setProperty("write.format.default", "parquet"));

      Table loaded = ops.loadTable(ident);
      assertEquals("parquet", loaded.properties().get("write.format.default"));
    } finally {
      ops.dropTable(ident);
    }
  }
}
