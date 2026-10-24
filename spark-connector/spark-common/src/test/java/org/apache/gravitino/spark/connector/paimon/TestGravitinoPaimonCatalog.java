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

package org.apache.gravitino.spark.connector.paimon;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.catalog.PropertiesMetadataHelpers;
import org.apache.gravitino.catalog.lakehouse.paimon.GravitinoPaimonTable;
import org.apache.gravitino.catalog.lakehouse.paimon.PaimonTablePropertiesMetadata;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.expressions.NamedReference;
import org.apache.gravitino.rel.expressions.distributions.Distribution;
import org.apache.gravitino.rel.expressions.distributions.Strategy;
import org.apache.gravitino.rel.expressions.sorts.SortOrder;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.indexes.Index;
import org.apache.gravitino.spark.connector.SparkTransformConverter;
import org.apache.gravitino.spark.connector.SparkTypeConverter;
import org.apache.gravitino.spark.connector.catalog.GravitinoCatalogManager;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.options.Options;
import org.apache.paimon.spark.SparkTable;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.expressions.Expressions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

/** Tests the Paimon-specific Spark create-table request and native metadata round trip. */
public class TestGravitinoPaimonCatalog {

  private static final StructType SCHEMA =
      new StructType()
          .add("source_namespace", DataTypes.StringType, false)
          .add("source_topic", DataTypes.StringType, false)
          .add("source_partition", DataTypes.IntegerType, false)
          .add("source_offset", DataTypes.LongType, false)
          .add(
              new StructField(
                  "payload",
                  DataTypes.StringType,
                  true,
                  new MetadataBuilder().putString("comment", "event payload").build()))
          .add("dt", DataTypes.StringType)
          .add("hr", DataTypes.StringType);
  private static final String PRIMARY_KEY =
      "source_namespace,source_topic,source_partition,source_offset";

  private org.apache.gravitino.rel.TableCatalog tableCatalog;
  private TestCatalog catalog;
  private Column[] columns;
  private String comment;
  private Map<String, String> properties;
  private Transform[] partitioning;
  private Distribution distribution;
  private SortOrder[] sortOrders;
  private Index[] indexes;

  @BeforeEach
  void setUp() {
    GravitinoClient client = mock(GravitinoClient.class);
    Catalog gravitinoCatalog = mock(Catalog.class);
    tableCatalog = mock(org.apache.gravitino.rel.TableCatalog.class, CALLS_REAL_METHODS);
    when(gravitinoCatalog.type()).thenReturn(Catalog.Type.RELATIONAL);
    when(gravitinoCatalog.provider()).thenReturn("lakehouse-paimon");
    when(gravitinoCatalog.properties()).thenReturn(Map.of());
    when(gravitinoCatalog.supportsSecrets()).thenThrow(new UnsupportedOperationException());
    when(gravitinoCatalog.supportsCredentials()).thenThrow(new UnsupportedOperationException());
    when(gravitinoCatalog.asTableCatalog()).thenReturn(tableCatalog);
    when(client.loadCatalog(any())).thenReturn(gravitinoCatalog);
    GravitinoCatalogManager.create(new SparkConf(false), "user", identity -> client);
    when(tableCatalog.createTable(
            any(),
            any(Column[].class),
            nullable(String.class),
            any(),
            any(Transform[].class),
            nullable(Distribution.class),
            nullable(SortOrder[].class),
            any(Index[].class)))
        .thenAnswer(
            invocation -> {
              columns = invocation.getArgument(1);
              comment = invocation.getArgument(2);
              properties = invocation.getArgument(3);
              partitioning = invocation.getArgument(4);
              distribution = invocation.getArgument(5);
              sortOrders = invocation.getArgument(6);
              indexes = invocation.getArgument(7);
              return mock(Table.class);
            });
    catalog = new TestCatalog();
    catalog.initialize("paimon", new CaseInsensitiveStringMap(Map.of()));
  }

  @AfterEach
  void tearDown() {
    GravitinoCatalogManager.get().close();
  }

  @Test
  void testCreateCompositePrimaryKeyWithDynamicBuckets() {
    Map<String, String> options = new HashMap<>();
    options.put("primary-key", PRIMARY_KEY);
    options.put("bucket", "-1");
    options.put("merge-engine", "deduplicate");
    options.put("snapshot.time-retained", "7 d");
    options.put("comment", "source events");
    Map<String, String> original = new HashMap<>(options);

    catalog.create(
        SCHEMA,
        new org.apache.spark.sql.connector.expressions.Transform[] {
          Expressions.identity("dt"), Expressions.identity("hr")
        },
        options);

    assertEquals(1, indexes.length);
    assertEquals(Index.IndexType.PRIMARY_KEY, indexes[0].type());
    assertArrayEquals(
        new String[][] {
          {"source_namespace"}, {"source_topic"}, {"source_partition"}, {"source_offset"}
        },
        indexes[0].fieldNames());
    assertEquals(Strategy.HASH, distribution.strategy());
    assertEquals(-1, distribution.number());
    assertEquals(0, distribution.expressions().length);
    assertEquals(2, partitioning.length);
    assertEquals("identity", partitioning[0].name());
    assertEquals("dt", partitioning[0].references()[0].toString());
    assertEquals("hr", partitioning[1].references()[0].toString());
    assertEquals("source events", comment);
    assertEquals(
        Map.of("merge-engine", "deduplicate", "snapshot.time-retained", "7 d"), properties);
    assertEquals(SCHEMA.fields().length, columns.length);
    assertFalse(columns[0].nullable());
    assertEquals("event payload", columns[4].comment());
    assertEquals(0, sortOrders.length);
    assertEquals(original, options);
  }

  @Test
  void testCreateFixedBucketsWithExplicitKeys() {
    catalog.create(
        SCHEMA,
        new org.apache.spark.sql.connector.expressions.Transform[0],
        Map.of(
            "primary-key", PRIMARY_KEY,
            "bucket", " 4 ",
            "bucket-key", " source_topic, source_partition "));

    assertEquals(4, distribution.number());
    assertEquals(2, distribution.expressions().length);
    assertArrayEquals(
        new String[] {"source_topic"},
        ((NamedReference.FieldReference) distribution.expressions()[0]).fieldName());
    assertArrayEquals(
        new String[] {"source_partition"},
        ((NamedReference.FieldReference) distribution.expressions()[1]).fieldName());
    assertEquals(Map.of(), properties);
  }

  @Test
  void testCreateClusteredByAppendTableWithPartitions() {
    catalog.create(
        SCHEMA,
        new org.apache.spark.sql.connector.expressions.Transform[] {
          Expressions.identity("dt"), Expressions.bucket(4, "source_topic", "source_partition")
        },
        Map.of("custom.option", "value"));

    assertEquals(0, indexes.length);
    assertEquals(Strategy.HASH, distribution.strategy());
    assertEquals(4, distribution.number());
    assertEquals(2, distribution.expressions().length);
    assertArrayEquals(
        new String[] {"source_topic"},
        ((NamedReference.FieldReference) distribution.expressions()[0]).fieldName());
    assertArrayEquals(
        new String[] {"source_partition"},
        ((NamedReference.FieldReference) distribution.expressions()[1]).fieldName());
    assertEquals(1, partitioning.length);
    assertEquals("identity", partitioning[0].name());
    assertEquals("dt", partitioning[0].references()[0].toString());
    assertEquals(Map.of("custom.option", "value"), properties);
  }

  @Test
  void testCreateClusteredByPrimaryKeyTableWithMatchingProperties() {
    catalog.create(
        SCHEMA,
        new org.apache.spark.sql.connector.expressions.Transform[] {
          Expressions.bucket(4, "source_topic", "source_partition")
        },
        Map.of(
            "primary-key", PRIMARY_KEY,
            "bucket", " 4 ",
            "bucket-key", " source_topic, source_partition "));

    assertEquals(1, indexes.length);
    assertEquals(4, distribution.number());
    assertEquals(2, distribution.expressions().length);
    assertEquals(0, partitioning.length);
    assertEquals(Map.of(), properties);
  }

  @ParameterizedTest
  @ValueSource(strings = {"-1", "8", "0", "invalid"})
  void testRejectConflictingClusteredByBucketCount(String bucket) {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            catalog.create(
                SCHEMA,
                new org.apache.spark.sql.connector.expressions.Transform[] {
                  Expressions.bucket(4, "source_topic")
                },
                Map.of("bucket", bucket)));
    verifyNoInteractions(tableCatalog);
  }

  @Test
  void testRejectConflictingClusteredByBucketKeys() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            catalog.create(
                SCHEMA,
                new org.apache.spark.sql.connector.expressions.Transform[] {
                  Expressions.bucket(4, "source_topic")
                },
                Map.of("bucket-key", "source_partition")));
    verifyNoInteractions(tableCatalog);
  }

  @ParameterizedTest
  @ValueSource(ints = {0, -1})
  void testRejectNonPositiveClusteredByBucketCount(int buckets) {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            catalog.create(
                SCHEMA,
                new org.apache.spark.sql.connector.expressions.Transform[] {
                  Expressions.bucket(buckets, "source_topic")
                },
                Map.of()));
    verifyNoInteractions(tableCatalog);
  }

  @Test
  void testRejectMultipleBucketTransforms() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            catalog.create(
                SCHEMA,
                new org.apache.spark.sql.connector.expressions.Transform[] {
                  Expressions.bucket(4, "source_topic"), Expressions.bucket(4, "source_partition")
                },
                Map.of()));
    verifyNoInteractions(tableCatalog);
  }

  @ParameterizedTest
  @ValueSource(strings = {"missing", "payload.nested"})
  void testRejectInvalidClusteredByColumns(String column) {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            catalog.create(
                SCHEMA,
                new org.apache.spark.sql.connector.expressions.Transform[] {
                  Expressions.bucket(4, column)
                },
                Map.of()));
    verifyNoInteractions(tableCatalog);
  }

  @Test
  void testRejectSortedBuckets() {
    assertThrows(
        UnsupportedOperationException.class,
        () ->
            catalog.create(
                SCHEMA,
                new org.apache.spark.sql.connector.expressions.Transform[] {
                  SparkTransformConverter.createSortBucketTransform(
                      4, new String[] {"source_topic"}, new String[] {"source_partition"})
                },
                Map.of()));
    verifyNoInteractions(tableCatalog);
  }

  @Test
  void testCreateSinglePrimaryKeyWithoutBucketOptions() {
    catalog.create(
        SCHEMA,
        new org.apache.spark.sql.connector.expressions.Transform[0],
        Map.of("primary-key", " source_offset "));

    assertEquals(1, indexes.length);
    assertArrayEquals(new String[][] {{"source_offset"}}, indexes[0].fieldNames());
    assertEquals(Strategy.NONE, distribution.strategy());
    assertEquals(Map.of(), properties);
  }

  @Test
  void testCreateFixedBucketsWithoutExplicitKeys() {
    catalog.create(
        SCHEMA,
        new org.apache.spark.sql.connector.expressions.Transform[0],
        Map.of("primary-key", PRIMARY_KEY, "bucket", "8"));

    assertEquals(8, distribution.number());
    assertEquals(0, distribution.expressions().length);
    assertEquals(Map.of(), properties);
  }

  @Test
  void testRejectBucketKeysWithoutExplicitBucketCount() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            catalog.create(
                SCHEMA,
                new org.apache.spark.sql.connector.expressions.Transform[0],
                Map.of("primary-key", PRIMARY_KEY, "bucket-key", "source_topic")));
    verifyNoInteractions(tableCatalog);
  }

  @Test
  void testCreateWithoutPrimaryKeyOrBuckets() {
    catalog.create(
        SCHEMA,
        new org.apache.spark.sql.connector.expressions.Transform[0],
        Map.of("custom.option", "value", "owner", "spark-user"));

    assertEquals(0, indexes.length);
    assertEquals(Map.of("custom.option", "value"), properties);
  }

  @ParameterizedTest
  @CsvSource({"true,false", "false,false", "false,true"})
  void testPaimonSchemaRoundTrip(boolean primaryKey, boolean clusteredBy, @TempDir Path warehouse)
      throws Exception {
    Map<String, String> options = new HashMap<>();
    options.put("bucket", primaryKey ? "-1" : "4");
    options.put("merge-engine", "deduplicate");
    if (primaryKey) {
      options.put("primary-key", PRIMARY_KEY);
    } else {
      options.put("bucket-key", "source_topic");
    }
    if (clusteredBy) {
      options.remove("bucket");
      options.remove("bucket-key");
    }
    StructType inputSchema =
        primaryKey
            ? new StructType(
                Arrays.stream(SCHEMA.fields())
                    .map(
                        field ->
                            new StructField(field.name(), field.dataType(), true, field.metadata()))
                    .toArray(StructField[]::new))
            : SCHEMA;
    catalog.create(
        inputSchema,
        clusteredBy
            ? new org.apache.spark.sql.connector.expressions.Transform[] {
              Expressions.identity("dt"),
              Expressions.identity("hr"),
              Expressions.bucket(4, "source_topic")
            }
            : new org.apache.spark.sql.connector.expressions.Transform[] {
              Expressions.identity("dt"), Expressions.identity("hr")
            },
        options);
    PropertiesMetadataHelpers.validatePropertyForCreate(
        new PaimonTablePropertiesMetadata(), properties);
    GravitinoPaimonTable table =
        GravitinoPaimonTable.builder()
            .withName("events")
            .withColumns(columns)
            .withComment(comment)
            .withProperties(properties)
            .withPartitioning(partitioning)
            .withDistribution(distribution)
            .withIndexes(indexes)
            .build();
    try (org.apache.paimon.catalog.Catalog nativeCatalog =
        CatalogFactory.createCatalog(
            CatalogContext.create(
                Options.fromMap(Map.of("warehouse", warehouse.toUri().toString()))))) {
      nativeCatalog.createDatabase("default", true);
      org.apache.paimon.catalog.Identifier identifier =
          org.apache.paimon.catalog.Identifier.create("default", "events");
      nativeCatalog.createTable(identifier, table.toPaimonTableSchema(), false);
      org.apache.paimon.table.Table nativeTable = nativeCatalog.getTable(identifier);
      assertEquals(
          primaryKey
              ? List.of("source_namespace", "source_topic", "source_partition", "source_offset")
              : List.of(),
          nativeTable.primaryKeys());
      assertEquals(List.of("dt", "hr"), nativeTable.partitionKeys());
      assertEquals(primaryKey ? "-1" : "4", nativeTable.options().get("bucket"));
      assertEquals(primaryKey ? null : "source_topic", nativeTable.options().get("bucket-key"));
      GravitinoPaimonTable loaded = GravitinoPaimonTable.fromPaimonTable(nativeTable);
      assertEquals(primaryKey ? 1 : 0, loaded.index().length);
      if (primaryKey) {
        assertArrayEquals(indexes[0].fieldNames(), loaded.index()[0].fieldNames());
      }
      assertEquals(primaryKey ? -1 : 4, loaded.distribution().number());
      assertEquals(primaryKey ? 0 : 1, loaded.distribution().expressions().length);
      assertEquals("deduplicate", loaded.properties().get("merge-engine"));
      for (GravitinoPaimonTable metadata : List.of(table, loaded)) {
        SparkPaimonTable sparkTable =
            new SparkPaimonTable(
                Identifier.of(new String[] {"default"}, "events"),
                metadata,
                new SparkTable(nativeTable),
                PaimonPropertiesConverter.getInstance(),
                catalog.getSparkTransformConverter(),
                new SparkTypeConverter());
        assertEquals(SCHEMA, sparkTable.schema());
        assertEquals(primaryKey ? PRIMARY_KEY : null, sparkTable.properties().get("primary-key"));
        assertEquals(primaryKey ? "-1" : "4", sparkTable.properties().get("bucket"));
        assertEquals(primaryKey ? null : "source_topic", sparkTable.properties().get("bucket-key"));
        assertEquals(2, sparkTable.partitioning().length);
      }
    }
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "",
        " ",
        "missing",
        "payload.nested",
        "source_topic,source_topic",
        "source_topic,"
      })
  void testRejectInvalidPrimaryKeys(String primaryKey) {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            catalog.create(
                SCHEMA,
                new org.apache.spark.sql.connector.expressions.Transform[0],
                Map.of("primary-key", primaryKey)));
    verifyNoInteractions(tableCatalog);
  }

  @ParameterizedTest
  @ValueSource(strings = {"", " ", "abc", "0", "-2", "2147483648"})
  void testRejectInvalidBucketNumbers(String bucket) {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            catalog.create(
                SCHEMA,
                new org.apache.spark.sql.connector.expressions.Transform[0],
                Map.of("primary-key", PRIMARY_KEY, "bucket", bucket)));
    verifyNoInteractions(tableCatalog);
  }

  @ParameterizedTest
  @ValueSource(
      strings = {"", "missing", "payload.nested", "source_topic,source_topic", "source_topic,"})
  void testRejectInvalidBucketKeys(String bucketKey) {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            catalog.create(
                SCHEMA,
                new org.apache.spark.sql.connector.expressions.Transform[0],
                Map.of("primary-key", PRIMARY_KEY, "bucket", "4", "bucket-key", bucketKey)));
    verifyNoInteractions(tableCatalog);
  }

  @Test
  void testRejectBucketKeyOutsidePrimaryKey() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            catalog.create(
                SCHEMA,
                new org.apache.spark.sql.connector.expressions.Transform[0],
                Map.of("primary-key", PRIMARY_KEY, "bucket", "4", "bucket-key", "payload")));
    verifyNoInteractions(tableCatalog);
  }

  @Test
  void testRejectDynamicBucketsWithExplicitKeys() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            catalog.create(
                SCHEMA,
                new org.apache.spark.sql.connector.expressions.Transform[0],
                Map.of("primary-key", PRIMARY_KEY, "bucket", "-1", "bucket-key", "source_topic")));
    verifyNoInteractions(tableCatalog);
  }

  @Test
  void testRejectBucketedAppendWithoutKeys() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            catalog.create(
                SCHEMA,
                new org.apache.spark.sql.connector.expressions.Transform[0],
                Map.of("bucket", "4")));
    verifyNoInteractions(tableCatalog);
  }

  @Test
  void testRejectPartitionColumnsAsBucketKeys() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            catalog.create(
                SCHEMA,
                new org.apache.spark.sql.connector.expressions.Transform[] {
                  Expressions.identity("dt")
                },
                Map.of("primary-key", "source_topic,dt", "bucket", "4", "bucket-key", "dt")));
    verifyNoInteractions(tableCatalog);
  }

  @Test
  void testRejectFixedBucketsForCrossPartitionPrimaryKeys() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            catalog.create(
                SCHEMA,
                new org.apache.spark.sql.connector.expressions.Transform[] {
                  Expressions.identity("dt")
                },
                Map.of("primary-key", PRIMARY_KEY, "bucket", "4")));
    verifyNoInteractions(tableCatalog);
  }

  @Test
  void testRejectPrimaryKeyOnlyPartitionColumns() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            catalog.create(
                SCHEMA,
                new org.apache.spark.sql.connector.expressions.Transform[] {
                  Expressions.identity("dt")
                },
                Map.of("primary-key", "dt", "bucket", "-1")));
    verifyNoInteractions(tableCatalog);
  }

  @ParameterizedTest
  @ValueSource(strings = {"missing", "payload.nested"})
  void testRejectInvalidPartitionColumns(String partitionColumn) {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            catalog.create(
                SCHEMA,
                new org.apache.spark.sql.connector.expressions.Transform[] {
                  Expressions.identity(partitionColumn)
                },
                Map.of("primary-key", PRIMARY_KEY, "bucket", "-1")));
    verifyNoInteractions(tableCatalog);
  }

  @Test
  void testRejectDuplicatePartitionColumns() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            catalog.create(
                SCHEMA,
                new org.apache.spark.sql.connector.expressions.Transform[] {
                  Expressions.identity("dt"), Expressions.identity("dt")
                },
                Map.of("primary-key", PRIMARY_KEY, "bucket", "-1")));
    verifyNoInteractions(tableCatalog);
  }

  @Test
  void testPrimaryKeyWithLiteralDotColumn() {
    catalog.create(
        new StructType().add("literal.dot", DataTypes.IntegerType, false),
        new org.apache.spark.sql.connector.expressions.Transform[0],
        Map.of("primary-key", "literal.dot", "bucket", "-1"));
    assertArrayEquals(new String[][] {{"literal.dot"}}, indexes[0].fieldNames());
  }

  @Test
  void testRejectBucketTransformBeforeCreatingTable() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            catalog.create(
                SCHEMA,
                new org.apache.spark.sql.connector.expressions.Transform[] {
                  Expressions.bucket(4, "source_topic")
                },
                Map.of("primary-key", PRIMARY_KEY, "bucket", "-1")));
    verifyNoInteractions(tableCatalog);
  }

  @Test
  void testRejectNonIdentityPartitionBeforeCreatingTable() {
    assertThrows(
        UnsupportedOperationException.class,
        () ->
            catalog.create(
                SCHEMA,
                new org.apache.spark.sql.connector.expressions.Transform[] {Expressions.days("dt")},
                Map.of("primary-key", PRIMARY_KEY, "bucket", "-1")));
    verifyNoInteractions(tableCatalog);
  }

  private static class TestCatalog extends GravitinoPaimonCatalog {

    private void create(
        StructType schema,
        org.apache.spark.sql.connector.expressions.Transform[] transforms,
        Map<String, String> properties) {
      createGravitinoTable(
          Identifier.of(new String[] {"default"}, "events"), schema, transforms, properties);
    }

    @Override
    protected TableCatalog createAndInitSparkCatalog(
        String name, CaseInsensitiveStringMap options, Map<String, String> properties) {
      return mock(TableCatalog.class);
    }
  }
}
