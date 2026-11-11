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
package org.apache.gravitino.catalog.glue.integration.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.base.Preconditions;
import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.catalog.glue.GlueConstants;
import org.apache.gravitino.exceptions.NoSuchTableException;
import org.apache.gravitino.exceptions.NotFoundException;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.TableChange;
import org.apache.gravitino.rel.expressions.distributions.Distributions;
import org.apache.gravitino.rel.expressions.sorts.SortOrder;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.indexes.Index;
import org.apache.gravitino.rel.types.Types;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

/**
 * Runs {@link AbstractGlueCatalogIT} scenarios against a real AWS Glue endpoint.
 *
 * <p>This test is <b>skipped by default</b>. Required environment variables:
 *
 * <ul>
 *   <li>{@code AWS_ACCESS_KEY_ID}
 *   <li>{@code AWS_SECRET_ACCESS_KEY}
 *   <li>{@code AWS_S3_TEST_BUCKET}
 *   <li>{@code AWS_DEFAULT_REGION} (optional, defaults to {@code us-east-1})
 *   <li>{@code GLUE_CATALOG_ID} (optional, 12-digit AWS account ID)
 * </ul>
 */
@EnabledIfEnvironmentVariable(named = "AWS_ACCESS_KEY_ID", matches = ".+")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class AwsGlueCatalogIT extends AbstractGlueCatalogIT {

  private static final Logger LOG = LoggerFactory.getLogger(AwsGlueCatalogIT.class);

  @BeforeAll
  void checkAwsEnvironment() {
    Preconditions.checkState(
        System.getenv("AWS_SECRET_ACCESS_KEY") != null, "AWS_SECRET_ACCESS_KEY must be set");
    Preconditions.checkState(
        System.getenv("AWS_S3_TEST_BUCKET") != null, "AWS_S3_TEST_BUCKET must be set");
    if (System.getenv("AWS_DEFAULT_REGION") == null) {
      LOG.info("AWS_DEFAULT_REGION is not set, using default us-east-1");
    }
  }

  @Override
  protected Map<String, String> catalogConfig() {
    Map<String, String> config = new HashMap<>();
    config.put(
        GlueConstants.AWS_REGION, System.getenv().getOrDefault("AWS_DEFAULT_REGION", "us-east-1"));
    config.put(GlueConstants.AWS_ACCESS_KEY_ID, System.getenv("AWS_ACCESS_KEY_ID"));
    config.put(GlueConstants.AWS_SECRET_ACCESS_KEY, System.getenv("AWS_SECRET_ACCESS_KEY"));
    String catalogId = System.getenv("GLUE_CATALOG_ID");
    if (catalogId != null) {
      config.put(GlueConstants.AWS_GLUE_CATALOG_ID, catalogId);
    }
    String bucket = System.getenv("AWS_S3_TEST_BUCKET");
    if (bucket != null) {
      config.put(GlueConstants.WAREHOUSE, "s3://" + bucket + "/warehouse");
    }
    return config;
  }

  @Test
  void testCreateIcebergTableWithComplexTypes() {
    String bucket = System.getenv("AWS_S3_TEST_BUCKET");
    String schema = "glue_it_" + System.nanoTime();
    ops.createSchema(NameIdentifier.of("ml", "cat", schema), null, Collections.emptyMap());

    Map<String, String> props = new HashMap<>();
    props.put(GlueConstants.TABLE_FORMAT, "ICEBERG");
    props.put(GlueConstants.LOCATION, "s3://" + bucket + "/" + schema + "/");

    Column[] cols = {
      Column.of("tags", Types.ListType.nullable(Types.StringType.get()), "list col"),
      Column.of(
          "scores",
          Types.MapType.of(Types.StringType.get(), Types.DoubleType.get(), false),
          "map col"),
      Column.of(
          "info",
          Types.StructType.of(
              Types.StructType.Field.of("name", Types.StringType.get(), true, null),
              Types.StructType.Field.of("age", Types.IntegerType.get(), false, null)),
          "struct col"),
    };

    ops.createTable(
        NameIdentifier.of("ml", "cat", schema, "ice_complex"),
        cols,
        null,
        props,
        new Transform[0],
        Distributions.NONE,
        new SortOrder[0],
        new Index[0]);

    try {
      Table loaded = ops.loadTable(NameIdentifier.of("ml", "cat", schema, "ice_complex"));
      assertEquals(3, loaded.columns().length);

      assertEquals("list col", loaded.columns()[0].comment());
      Types.ListType listType = (Types.ListType) loaded.columns()[0].dataType();
      assertEquals(Types.StringType.get(), listType.elementType());
      assertTrue(listType.elementNullable());

      assertEquals("map col", loaded.columns()[1].comment());
      Types.MapType mapType = (Types.MapType) loaded.columns()[1].dataType();
      assertEquals(Types.StringType.get(), mapType.keyType());
      assertEquals(Types.DoubleType.get(), mapType.valueType());
      assertFalse(mapType.valueNullable());

      assertEquals("struct col", loaded.columns()[2].comment());
      Types.StructType structType = (Types.StructType) loaded.columns()[2].dataType();
      assertEquals(2, structType.fields().length);
      assertEquals("name", structType.fields()[0].name());
      assertEquals(Types.StringType.get(), structType.fields()[0].type());
      assertEquals("age", structType.fields()[1].name());
      assertEquals(Types.IntegerType.get(), structType.fields()[1].type());
    } finally {
      ops.dropTable(NameIdentifier.of("ml", "cat", schema, "ice_complex"));
      ops.dropSchema(NameIdentifier.of("ml", "cat", schema), false);
    }
  }

  @Test
  void testAlterIcebergMetadata() {
    String bucket = System.getenv("AWS_S3_TEST_BUCKET");
    String schema = "glue_it_" + System.nanoTime();
    ops.createSchema(NameIdentifier.of("ml", "cat", schema), null, Collections.emptyMap());

    Map<String, String> props = new HashMap<>();
    props.put(GlueConstants.TABLE_FORMAT, "ICEBERG");
    props.put(GlueConstants.LOCATION, "s3://" + bucket + "/" + schema + "/");
    ops.createTable(
        NameIdentifier.of("ml", "cat", schema, "iceberg_alter"),
        new Column[] {Column.of("id", Types.LongType.get(), null)},
        null,
        props,
        new Transform[0],
        Distributions.NONE,
        new SortOrder[0],
        new Index[0]);

    try {
      ops.alterTable(
          NameIdentifier.of("ml", "cat", schema, "iceberg_alter"),
          TableChange.setProperty("comment", "updated comment"));

      Table loaded = ops.loadTable(NameIdentifier.of("ml", "cat", schema, "iceberg_alter"));
      assertEquals("updated comment", loaded.properties().get("comment"));
    } finally {
      ops.dropTable(NameIdentifier.of("ml", "cat", schema, "iceberg_alter"));
      ops.dropSchema(NameIdentifier.of("ml", "cat", schema), false);
    }
  }

  @Test
  void testRegisteredIcebergTableTakesMetadataAndOnlyChangesGlue() {
    String bucket = System.getenv("AWS_S3_TEST_BUCKET");
    String schema = "glue_it_" + System.nanoTime();
    NameIdentifier src = NameIdentifier.of("ml", "cat", schema, "ice_src");
    NameIdentifier reg = NameIdentifier.of("ml", "cat", schema, "ice_reg");
    NameIdentifier missing = NameIdentifier.of("ml", "cat", schema, "ice_missing");
    String location = "s3://" + bucket + "/" + schema + "/ice_src";
    ops.createSchema(NameIdentifier.of("ml", "cat", schema), null, Collections.emptyMap());
    try {
      createIcebergTable(src, location);
      String metadataLocation =
          ops.loadTable(src).properties().get(GlueConstants.METADATA_LOCATION);

      Map<String, String> props = new HashMap<>();
      props.put(GlueConstants.TABLE_FORMAT, "ICEBERG");
      props.put(GlueConstants.METADATA_LOCATION, metadataLocation);
      Table registered =
          ops.createTable(
              reg,
              new Column[0],
              null,
              props,
              new Transform[0],
              Distributions.NONE,
              new SortOrder[0],
              new Index[0]);
      assertEquals(1, registered.columns().length);
      assertEquals("id", registered.columns()[0].name());
      assertEquals(location, registered.properties().get(GlueConstants.LOCATION));
      assertEquals(
          "true", registered.properties().get(GlueConstants.REGISTERED_ICEBERG_TABLE_PARAM));

      assertThrows(
          IllegalArgumentException.class,
          () ->
              ops.alterTable(
                  reg, TableChange.addColumn(new String[] {"c"}, Types.IntegerType.get())));
      ops.alterTable(reg, TableChange.setProperty("k1", "v1"));
      Table altered = ops.loadTable(reg);
      assertEquals("v1", altered.properties().get("k1"));
      assertEquals(metadataLocation, altered.properties().get(GlueConstants.METADATA_LOCATION));
      assertEquals(
          metadataLocation, ops.loadTable(src).properties().get(GlueConstants.METADATA_LOCATION));
      assertThrows(UnsupportedOperationException.class, () -> ops.purgeTable(reg));

      props.put(
          GlueConstants.METADATA_LOCATION, location + "/metadata/00099-missing.metadata.json");
      assertThrows(
          NotFoundException.class,
          () ->
              ops.createTable(
                  missing,
                  new Column[0],
                  null,
                  props,
                  new Transform[0],
                  Distributions.NONE,
                  new SortOrder[0],
                  new Index[0]));
      assertThrows(NoSuchTableException.class, () -> ops.loadTable(missing));
    } finally {
      ops.dropTable(reg);
      ops.purgeTable(src);
      ops.dropSchema(NameIdentifier.of("ml", "cat", schema), true);
    }
  }

  @Test
  void testIcebergTableWithMissingMetadataFileFailsToLoad() {
    String bucket = System.getenv("AWS_S3_TEST_BUCKET");
    String schema = "glue_it_" + System.nanoTime();
    NameIdentifier ident = NameIdentifier.of("ml", "cat", schema, "ice_lost");
    ops.createSchema(NameIdentifier.of("ml", "cat", schema), null, Collections.emptyMap());
    try (S3Client s3 = s3Client()) {
      createIcebergTable(ident, "s3://" + bucket + "/" + schema + "/ice_lost");
      Table loaded = ops.loadTable(ident);
      assertThrows(UnsupportedOperationException.class, loaded::supportPartitions);

      String metadataLocation = loaded.properties().get(GlueConstants.METADATA_LOCATION);
      URI uri = URI.create(metadataLocation);
      s3.deleteObject(b -> b.bucket(uri.getHost()).key(uri.getPath().substring(1)));

      NotFoundException e = assertThrows(NotFoundException.class, () -> ops.loadTable(ident));
      assertTrue(e.getMessage().contains(metadataLocation), e.getMessage());
      assertTrue(ops.dropTable(ident));
    } finally {
      ops.dropTable(ident);
      ops.dropSchema(NameIdentifier.of("ml", "cat", schema), true);
    }
  }

  @Test
  void testPurgeIcebergTableDeletesItsFiles() {
    String bucket = System.getenv("AWS_S3_TEST_BUCKET");
    String schema = "glue_it_" + System.nanoTime();
    NameIdentifier ident = NameIdentifier.of("ml", "cat", schema, "ice_purge");
    String prefix = schema + "/ice_purge/";
    ops.createSchema(NameIdentifier.of("ml", "cat", schema), null, Collections.emptyMap());
    try (S3Client s3 = s3Client()) {
      createIcebergTable(ident, "s3://" + bucket + "/" + prefix);
      ops.alterTable(ident, TableChange.setProperty("k1", "v1"));
      assertTrue(s3.listObjectsV2(b -> b.bucket(bucket).prefix(prefix)).keyCount() > 0);

      assertTrue(ops.purgeTable(ident));
      assertThrows(NoSuchTableException.class, () -> ops.loadTable(ident));
      assertEquals(0, s3.listObjectsV2(b -> b.bucket(bucket).prefix(prefix)).keyCount());
    } finally {
      ops.dropSchema(NameIdentifier.of("ml", "cat", schema), true);
    }
  }

  @Test
  void testIcebergAlterIsAppliedAsAWhole() {
    String bucket = System.getenv("AWS_S3_TEST_BUCKET");
    String schema = "glue_it_" + System.nanoTime();
    NameIdentifier ident = NameIdentifier.of("ml", "cat", schema, "ice_atomic");
    ops.createSchema(NameIdentifier.of("ml", "cat", schema), null, Collections.emptyMap());
    try {
      createIcebergTable(ident, "s3://" + bucket + "/" + schema + "/ice_atomic");
      String before = ops.loadTable(ident).properties().get(GlueConstants.METADATA_LOCATION);

      assertThrows(
          IllegalArgumentException.class,
          () ->
              ops.alterTable(
                  ident,
                  TableChange.addColumn(new String[] {"partial"}, Types.StringType.get()),
                  TableChange.setProperty("format-version", "1")));
      Table unchanged = ops.loadTable(ident);
      assertEquals(before, unchanged.properties().get(GlueConstants.METADATA_LOCATION));
      assertEquals(1, unchanged.columns().length);

      ops.alterTable(
          ident,
          TableChange.addColumn(
              new String[] {"first_col"},
              Types.IntegerType.get(),
              null,
              TableChange.ColumnPosition.first()));
      assertEquals("first_col", ops.loadTable(ident).columns()[0].name());
    } finally {
      ops.purgeTable(ident);
      ops.dropSchema(NameIdentifier.of("ml", "cat", schema), true);
    }
  }

  @Test
  void testIcebergColumnNullability() {
    String bucket = System.getenv("AWS_S3_TEST_BUCKET");
    String schema = "glue_it_" + System.nanoTime();
    NameIdentifier ident = NameIdentifier.of("ml", "cat", schema, "ice_nullability");
    ops.createSchema(NameIdentifier.of("ml", "cat", schema), null, Collections.emptyMap());
    Map<String, String> props = new HashMap<>();
    props.put(GlueConstants.TABLE_FORMAT, "ICEBERG");
    props.put(GlueConstants.LOCATION, "s3://" + bucket + "/" + schema + "/ice_nullability");
    try {
      ops.createTable(
          ident,
          new Column[] {
            Column.of("id", Types.LongType.get(), null, false, false, Column.DEFAULT_VALUE_NOT_SET),
            Column.of("name", Types.StringType.get(), null)
          },
          null,
          props,
          new Transform[0],
          Distributions.NONE,
          new SortOrder[0],
          new Index[0]);
      assertFalse(ops.loadTable(ident).columns()[0].nullable());

      ops.alterTable(ident, TableChange.updateColumnNullability(new String[] {"id"}, true));
      assertTrue(ops.loadTable(ident).columns()[0].nullable());

      // Iceberg refuses optional -> required as an incompatible change.
      assertThrows(
          IllegalArgumentException.class,
          () ->
              ops.alterTable(
                  ident, TableChange.updateColumnNullability(new String[] {"name"}, false)));
    } finally {
      ops.purgeTable(ident);
      ops.dropSchema(NameIdentifier.of("ml", "cat", schema), true);
    }
  }

  @Test
  void testHiveColumnConstraints() {
    String schema = "glue_it_" + System.nanoTime();
    NameIdentifier ident = NameIdentifier.of("ml", "cat", schema, "hive_constraints");
    ops.createSchema(NameIdentifier.of("ml", "cat", schema), null, Collections.emptyMap());
    try {
      assertThrows(
          IllegalArgumentException.class,
          () -> createHiveTable(ident, Column.of("c", Types.FixedCharType.of(256), null)));
      assertThrows(
          IllegalArgumentException.class,
          () ->
              createHiveTable(
                  ident,
                  Column.of(
                      "c",
                      Types.LongType.get(),
                      null,
                      false,
                      false,
                      Column.DEFAULT_VALUE_NOT_SET)));

      createHiveTable(
          ident,
          Column.of("c", Types.FixedCharType.of(255), null),
          Column.of("v", Types.VarCharType.of(65535), null));
      Table loaded = ops.loadTable(ident);
      assertEquals(Types.FixedCharType.of(255), loaded.columns()[0].dataType());
      assertEquals(Types.VarCharType.of(65535), loaded.columns()[1].dataType());

      assertThrows(
          IllegalArgumentException.class,
          () ->
              ops.alterTable(
                  ident,
                  TableChange.updateColumnType(new String[] {"v"}, Types.VarCharType.of(0))));
      assertThrows(
          IllegalArgumentException.class,
          () ->
              ops.alterTable(
                  ident, TableChange.updateColumnNullability(new String[] {"c"}, false)));
    } finally {
      ops.dropTable(ident);
      ops.dropSchema(NameIdentifier.of("ml", "cat", schema), true);
    }
  }

  private void createHiveTable(NameIdentifier ident, Column... columns) {
    ops.createTable(
        ident,
        columns,
        null,
        Collections.emptyMap(),
        new Transform[0],
        Distributions.NONE,
        new SortOrder[0],
        new Index[0]);
  }

  private void createIcebergTable(NameIdentifier ident, String location) {
    Map<String, String> props = new HashMap<>();
    props.put(GlueConstants.TABLE_FORMAT, "ICEBERG");
    props.put(GlueConstants.LOCATION, location);
    ops.createTable(
        ident,
        new Column[] {Column.of("id", Types.LongType.get(), null)},
        null,
        props,
        new Transform[0],
        Distributions.NONE,
        new SortOrder[0],
        new Index[0]);
  }

  private static S3Client s3Client() {
    return S3Client.builder()
        .region(Region.of(System.getenv().getOrDefault("AWS_DEFAULT_REGION", "us-east-1")))
        .credentialsProvider(
            StaticCredentialsProvider.create(
                AwsBasicCredentials.create(
                    System.getenv("AWS_ACCESS_KEY_ID"), System.getenv("AWS_SECRET_ACCESS_KEY"))))
        .build();
  }
}
