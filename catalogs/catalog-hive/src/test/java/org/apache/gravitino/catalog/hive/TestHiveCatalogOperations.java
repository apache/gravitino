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

package org.apache.gravitino.catalog.hive;

import static org.apache.gravitino.Catalog.AUTHORIZATION_PROVIDER;
import static org.apache.gravitino.Catalog.CLOUD_NAME;
import static org.apache.gravitino.Catalog.CLOUD_REGION_CODE;
import static org.apache.gravitino.Catalog.PROPERTY_IN_USE;
import static org.apache.gravitino.catalog.hive.HiveCatalogPropertiesMetadata.CHECK_INTERVAL_SEC;
import static org.apache.gravitino.catalog.hive.HiveCatalogPropertiesMetadata.CLIENT_POOL_CACHE_EVICTION_INTERVAL_MS;
import static org.apache.gravitino.catalog.hive.HiveCatalogPropertiesMetadata.CLIENT_POOL_SIZE;
import static org.apache.gravitino.catalog.hive.HiveCatalogPropertiesMetadata.DEFAULT_CATALOG;
import static org.apache.gravitino.catalog.hive.HiveCatalogPropertiesMetadata.FETCH_TIMEOUT_SEC;
import static org.apache.gravitino.catalog.hive.HiveCatalogPropertiesMetadata.IMPERSONATION_ENABLE;
import static org.apache.gravitino.catalog.hive.HiveCatalogPropertiesMetadata.KEY_TAB_URI;
import static org.apache.gravitino.catalog.hive.HiveCatalogPropertiesMetadata.LIST_ALL_TABLES;
import static org.apache.gravitino.catalog.hive.HiveCatalogPropertiesMetadata.METASTORE_URIS;
import static org.apache.gravitino.catalog.hive.HiveCatalogPropertiesMetadata.PRINCIPAL;
import static org.apache.gravitino.catalog.hive.TestHiveCatalog.HIVE_PROPERTIES_METADATA;
import static org.apache.gravitino.connector.BaseCatalog.CATALOG_BYPASS_PREFIX;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyShort;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.connector.BaseCatalog;
import org.apache.gravitino.connector.PropertyEntry;
import org.apache.gravitino.exceptions.ConnectionFailedException;
import org.apache.gravitino.exceptions.ViewAlreadyExistsException;
import org.apache.gravitino.hive.CachedClientPool;
import org.apache.gravitino.hive.HiveSchema;
import org.apache.gravitino.hive.HiveTable;
import org.apache.gravitino.hive.client.HiveClient;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Representation;
import org.apache.gravitino.rel.SQLRepresentation;
import org.apache.gravitino.rel.View;
import org.apache.gravitino.rel.ViewChange;
import org.apache.gravitino.rel.expressions.distributions.Distributions;
import org.apache.gravitino.rel.expressions.sorts.SortOrder;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.indexes.Index;
import org.apache.gravitino.rel.types.Types;
import org.apache.gravitino.storage.AzureProperties;
import org.apache.gravitino.utils.ClientPool;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.thrift.TException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class TestHiveCatalogOperations {
  @Test
  void testPropertyMeta() {
    Map<String, PropertyEntry<?>> propertyEntryMap =
        HIVE_PROPERTIES_METADATA.catalogPropertiesMetadata().propertyEntries();

    Assertions.assertEquals(26, propertyEntryMap.size());
    Assertions.assertTrue(propertyEntryMap.containsKey(METASTORE_URIS));
    Assertions.assertTrue(propertyEntryMap.containsKey(Catalog.PROPERTY_PACKAGE));
    Assertions.assertTrue(propertyEntryMap.containsKey(BaseCatalog.CATALOG_OPERATION_IMPL));
    Assertions.assertTrue(propertyEntryMap.containsKey(PROPERTY_IN_USE));
    Assertions.assertTrue(propertyEntryMap.containsKey(AUTHORIZATION_PROVIDER));
    Assertions.assertTrue(propertyEntryMap.containsKey(CLIENT_POOL_SIZE));
    Assertions.assertTrue(propertyEntryMap.containsKey(IMPERSONATION_ENABLE));
    Assertions.assertTrue(propertyEntryMap.containsKey(LIST_ALL_TABLES));
    Assertions.assertTrue(propertyEntryMap.containsKey(DEFAULT_CATALOG));
    Assertions.assertTrue(
        propertyEntryMap.get(AzureProperties.GRAVITINO_AZURE_CLIENT_SECRET).isHidden());
    Assertions.assertTrue(propertyEntryMap.get(METASTORE_URIS).isRequired());
    Assertions.assertFalse(propertyEntryMap.get(Catalog.PROPERTY_PACKAGE).isRequired());
    Assertions.assertFalse(propertyEntryMap.get(CLIENT_POOL_SIZE).isRequired());
    Assertions.assertFalse(
        propertyEntryMap.get(CLIENT_POOL_CACHE_EVICTION_INTERVAL_MS).isRequired());
    Assertions.assertFalse(propertyEntryMap.get(IMPERSONATION_ENABLE).isRequired());
    Assertions.assertFalse(propertyEntryMap.get(KEY_TAB_URI).isRequired());
    Assertions.assertFalse(propertyEntryMap.get(PRINCIPAL).isRequired());
    Assertions.assertFalse(propertyEntryMap.get(CHECK_INTERVAL_SEC).isRequired());
    Assertions.assertFalse(propertyEntryMap.get(FETCH_TIMEOUT_SEC).isRequired());
    Assertions.assertFalse(propertyEntryMap.get(CLOUD_NAME).isRequired());
    Assertions.assertFalse(propertyEntryMap.get(CLOUD_NAME).isImmutable());
    Assertions.assertFalse(propertyEntryMap.get(CLOUD_REGION_CODE).isRequired());
    Assertions.assertFalse(propertyEntryMap.get(CLOUD_REGION_CODE).isImmutable());
  }

  @Test
  void testPropertyOverwrite() {
    Map<String, String> maps = Maps.newHashMap();
    maps.put("a.b", "v1");
    maps.put(CATALOG_BYPASS_PREFIX + "a.b", "v2");

    maps.put("c.d", "v3");
    maps.put(CATALOG_BYPASS_PREFIX + "c.d", "v4");
    maps.put("e.f", "v5");

    maps.put(METASTORE_URIS, "url1");
    maps.put(ConfVars.METASTOREURIS.varname, "url2");
    maps.put(CATALOG_BYPASS_PREFIX + ConfVars.METASTOREURIS.varname, "url3");
    HiveCatalogOperations op = new HiveCatalogOperations();
    op.initialize(maps, null, HIVE_PROPERTIES_METADATA);

    Properties pp = op.mergeProperties(maps);
    Assertions.assertEquals("v2", pp.get("a.b"));
    Assertions.assertEquals("v4", pp.get("c.d"));
  }

  @Test
  void testEmptyBypassKey() {
    Map<String, String> properties = Maps.newHashMap();
    // Add a normal bypass configuration
    properties.put(CATALOG_BYPASS_PREFIX + "mapreduce.job.reduces", "20");
    // Add an empty bypass configuration
    properties.put(CATALOG_BYPASS_PREFIX, "some-value");

    HiveCatalogOperations hiveCatalogOperations = new HiveCatalogOperations();
    hiveCatalogOperations.initialize(properties, null, HIVE_PROPERTIES_METADATA);
    Properties pp = hiveCatalogOperations.mergeProperties(properties);

    // Verify that the normal bypass configuration is correctly applied
    String v = pp.getProperty("mapreduce.job.reduces");
    Assertions.assertEquals("20", v);

    // Verify that the empty bypass configuration is not applied
    // This will fail if the empty key is incorrectly added
    Assertions.assertNull(pp.getProperty(""));
  }

  @Test
  void testTestConnection() throws InterruptedException {
    HiveCatalogOperations op = new HiveCatalogOperations();
    op.clientPool = mock(CachedClientPool.class);
    when(op.clientPool.run(any())).thenThrow(new TException("mock connection exception"));

    ConnectionFailedException exception =
        Assertions.assertThrows(
            ConnectionFailedException.class,
            () ->
                op.testConnection(
                    NameIdentifier.of("metalake", "catalog"),
                    Catalog.Type.RELATIONAL,
                    "hive",
                    "comment",
                    ImmutableMap.of()));
    Assertions.assertEquals(
        "Failed to run getAllDatabases in Hive Metastore: mock connection exception",
        exception.getMessage());
  }

  @Test
  void testCreateGenericTableWithEmptyColumns() throws Exception {
    HiveCatalogOperations op = new HiveCatalogOperations();
    op.initialize(Maps.newHashMap(), null, HIVE_PROPERTIES_METADATA);

    CachedClientPool clientPool = mock(CachedClientPool.class);
    HiveClient hiveClient = mock(HiveClient.class);
    HiveSchema schema = HiveSchema.builder().withCatalogName("hive").withName("db").build();
    when(hiveClient.getDatabase(anyString(), anyString())).thenReturn(schema);

    ArgumentCaptor<HiveTable> hiveTableCaptor = ArgumentCaptor.forClass(HiveTable.class);
    doNothing().when(hiveClient).createTable(hiveTableCaptor.capture());
    when(clientPool.run(any()))
        .thenAnswer(
            invocation -> {
              ClientPool.Action<?, HiveClient, ?> action = invocation.getArgument(0);
              return action.run(hiveClient);
            });
    op.clientPool = clientPool;

    Map<String, String> properties = Maps.newHashMap();
    properties.put("is_generic", "true");

    op.createTable(
        NameIdentifier.of("db", "tbl"),
        new Column[0],
        "comment",
        properties,
        new Transform[0],
        Distributions.NONE,
        new SortOrder[0],
        new Index[0]);

    HiveTable createdTable = hiveTableCaptor.getValue();
    Assertions.assertEquals(0, createdTable.columns().length);
  }

  @Test
  void testCreateViewAcceptsTrinoDialect() throws Exception {
    HiveCatalogOperations op = new HiveCatalogOperations();
    op.initialize(Maps.newHashMap(), null, HIVE_PROPERTIES_METADATA);

    CachedClientPool clientPool = mock(CachedClientPool.class);
    HiveClient hiveClient = mock(HiveClient.class);
    HiveSchema schema = HiveSchema.builder().withCatalogName("hive").withName("db").build();
    when(hiveClient.getDatabase(anyString(), anyString())).thenReturn(schema);

    ArgumentCaptor<HiveTable> hiveTableCaptor = ArgumentCaptor.forClass(HiveTable.class);
    doNothing().when(hiveClient).createTable(hiveTableCaptor.capture());
    when(clientPool.run(any()))
        .thenAnswer(
            invocation -> {
              ClientPool.Action<?, HiveClient, ?> action = invocation.getArgument(0);
              return action.run(hiveClient);
            });
    op.clientPool = clientPool;

    View view =
        op.createView(
            NameIdentifier.of("db", "v_trino"),
            null,
            new Column[] {Column.of("c1", Types.IntegerType.get())},
            new SQLRepresentation[] {
              SQLRepresentation.builder().withDialect("trino").withSql("SELECT 1").build()
            },
            null,
            null,
            Maps.newHashMap());

    Assertions.assertNotNull(view);
    Assertions.assertEquals(1, view.representations().length);
    SQLRepresentation rep = (SQLRepresentation) view.representations()[0];
    Assertions.assertEquals("trino", rep.dialect());
    Assertions.assertEquals("SELECT 1", rep.sql());

    Map<String, String> storedProperties = hiveTableCaptor.getValue().properties();
    Assertions.assertEquals("true", storedProperties.get("presto_view"));
    Assertions.assertEquals("Presto View", storedProperties.get("comment"));
    TrinoNativeViewCodec.ViewDefinition decoded =
        TrinoNativeViewCodec.decode(hiveTableCaptor.getValue().viewOriginalText());
    Assertions.assertEquals("SELECT 1", decoded.originalSql);
  }

  @Test
  void testCreateViewLoadRoundTripPreservesTrinoComment() throws Exception {
    // A Trino dialect view's user-facing comment lives inside the encoded payload, not the HMS
    // "comment" property (which is fixed to the "Presto View" marker), so it must round-trip
    // through create + load.
    HiveCatalogOperations op = new HiveCatalogOperations();
    op.initialize(Maps.newHashMap(), null, HIVE_PROPERTIES_METADATA);

    CachedClientPool clientPool = mock(CachedClientPool.class);
    HiveClient hiveClient = mock(HiveClient.class);
    HiveSchema schema = HiveSchema.builder().withCatalogName("hive").withName("db").build();
    when(hiveClient.getDatabase(anyString(), anyString())).thenReturn(schema);

    ArgumentCaptor<HiveTable> hiveTableCaptor = ArgumentCaptor.forClass(HiveTable.class);
    doNothing().when(hiveClient).createTable(hiveTableCaptor.capture());
    when(clientPool.run(any()))
        .thenAnswer(
            invocation -> {
              ClientPool.Action<?, HiveClient, ?> action = invocation.getArgument(0);
              return action.run(hiveClient);
            });
    op.clientPool = clientPool;

    View created =
        op.createView(
            NameIdentifier.of("db", "v_trino"),
            "a view comment",
            new Column[] {Column.of("c1", Types.IntegerType.get())},
            new SQLRepresentation[] {
              SQLRepresentation.builder().withDialect("trino").withSql("SELECT 1").build()
            },
            null,
            null,
            Maps.newHashMap());
    Assertions.assertEquals("a view comment", created.comment());

    when(hiveClient.getTable(anyString(), anyString(), anyString()))
        .thenReturn(hiveTableCaptor.getValue());
    View loaded = op.loadView(NameIdentifier.of("db", "v_trino"));
    Assertions.assertEquals("a view comment", loaded.comment());
  }

  @Test
  void testCreateViewTrinoDialectStoresDummyHmsColumnAndRealColumnsInPayload() throws Exception {
    // Real Trino stores only a single dummy HMS column for a Presto View (see
    // io.trino.plugin.hive.HiveMetadata#createView); the real columns live in the encoded
    // payload. Gravitino must match this so a native Trino reading the HMS table directly (or
    // Gravitino reloading its own view) resolves the correct column count/types.
    HiveCatalogOperations op = new HiveCatalogOperations();
    op.initialize(Maps.newHashMap(), null, HIVE_PROPERTIES_METADATA);

    CachedClientPool clientPool = mock(CachedClientPool.class);
    HiveClient hiveClient = mock(HiveClient.class);
    HiveSchema schema = HiveSchema.builder().withCatalogName("hive").withName("db").build();
    when(hiveClient.getDatabase(anyString(), anyString())).thenReturn(schema);

    ArgumentCaptor<HiveTable> hiveTableCaptor = ArgumentCaptor.forClass(HiveTable.class);
    doNothing().when(hiveClient).createTable(hiveTableCaptor.capture());
    when(clientPool.run(any()))
        .thenAnswer(
            invocation -> {
              ClientPool.Action<?, HiveClient, ?> action = invocation.getArgument(0);
              return action.run(hiveClient);
            });
    op.clientPool = clientPool;

    Column[] columns = {
      Column.of("id", Types.LongType.get(), "id column"),
      Column.of("name", Types.StringType.get(), null)
    };

    View created =
        op.createView(
            NameIdentifier.of("db", "v_trino"),
            null,
            columns,
            new SQLRepresentation[] {
              SQLRepresentation.builder().withDialect("trino").withSql("SELECT id, name").build()
            },
            null,
            null,
            Maps.newHashMap());

    HiveTable storedTable = hiveTableCaptor.getValue();
    Assertions.assertEquals(1, storedTable.columns().length);
    Assertions.assertEquals("dummy", storedTable.columns()[0].name());

    Assertions.assertEquals(2, created.columns().length);
    Assertions.assertEquals("id", created.columns()[0].name());
    Assertions.assertEquals("name", created.columns()[1].name());

    when(hiveClient.getTable(anyString(), anyString(), anyString())).thenReturn(storedTable);
    View reloaded = op.loadView(NameIdentifier.of("db", "v_trino"));
    Assertions.assertEquals(2, reloaded.columns().length);
    Assertions.assertEquals("id", reloaded.columns()[0].name());
    Assertions.assertEquals("id column", reloaded.columns()[0].comment());
    Assertions.assertEquals("name", reloaded.columns()[1].name());
  }

  @Test
  void testCreateViewPersistsTrinoDefaultCatalogAndSchema() throws Exception {
    HiveCatalogOperations op = new HiveCatalogOperations();
    op.initialize(Maps.newHashMap(), null, HIVE_PROPERTIES_METADATA);

    CachedClientPool clientPool = mock(CachedClientPool.class);
    HiveClient hiveClient = mock(HiveClient.class);
    HiveSchema schema = HiveSchema.builder().withCatalogName("hive").withName("db").build();
    when(hiveClient.getDatabase(anyString(), anyString())).thenReturn(schema);

    ArgumentCaptor<HiveTable> hiveTableCaptor = ArgumentCaptor.forClass(HiveTable.class);
    doNothing().when(hiveClient).createTable(hiveTableCaptor.capture());
    when(clientPool.run(any()))
        .thenAnswer(
            invocation -> {
              ClientPool.Action<?, HiveClient, ?> action = invocation.getArgument(0);
              return action.run(hiveClient);
            });
    op.clientPool = clientPool;

    // Simulates a Trino "USE gt_hive.db; CREATE VIEW v AS SELECT * FROM t" statement, where
    // the unqualified "t" must be resolved against defaultCatalog/defaultSchema on reload.
    op.createView(
        NameIdentifier.of("db", "v_trino"),
        null,
        new Column[] {Column.of("c1", Types.IntegerType.get())},
        new SQLRepresentation[] {
          SQLRepresentation.builder().withDialect("trino").withSql("SELECT * FROM t").build()
        },
        "gt_hive",
        "db",
        Maps.newHashMap());

    TrinoNativeViewCodec.ViewDefinition decoded =
        TrinoNativeViewCodec.decode(hiveTableCaptor.getValue().viewOriginalText());
    Assertions.assertEquals("gt_hive", decoded.catalog);
    Assertions.assertEquals("db", decoded.schema);

    when(hiveClient.getTable(anyString(), anyString(), anyString()))
        .thenReturn(hiveTableCaptor.getValue());
    View loaded = op.loadView(NameIdentifier.of("db", "v_trino"));

    Assertions.assertEquals("gt_hive", loaded.defaultCatalog());
    Assertions.assertEquals("db", loaded.defaultSchema());
  }

  @Test
  void testCreateViewRejectsTrinoSchemaWithoutCatalog() throws Exception {
    // Trino's own ConnectorViewDefinition rejects a schema without a catalog; accepting it here
    // would persist a payload that a native Trino connector cannot decode.
    HiveCatalogOperations op = new HiveCatalogOperations();
    op.initialize(Maps.newHashMap(), null, HIVE_PROPERTIES_METADATA);

    CachedClientPool clientPool = mock(CachedClientPool.class);
    HiveClient hiveClient = mock(HiveClient.class);
    HiveSchema schema = HiveSchema.builder().withCatalogName("hive").withName("db").build();
    when(hiveClient.getDatabase(anyString(), anyString())).thenReturn(schema);
    when(clientPool.run(any()))
        .thenAnswer(
            invocation -> {
              ClientPool.Action<?, HiveClient, ?> action = invocation.getArgument(0);
              return action.run(hiveClient);
            });
    op.clientPool = clientPool;

    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                op.createView(
                    NameIdentifier.of("db", "v_trino"),
                    null,
                    new Column[] {Column.of("c1", Types.IntegerType.get())},
                    new SQLRepresentation[] {
                      SQLRepresentation.builder().withDialect("trino").withSql("SELECT 1").build()
                    },
                    null,
                    "db",
                    Maps.newHashMap()));

    Assertions.assertTrue(
        exception
            .getMessage()
            .contains("does not support a defaultSchema without a defaultCatalog"));
  }

  @Test
  void testCreateViewClearsStaleTrinoMarkerForNonTrinoDialect() throws Exception {
    HiveCatalogOperations op = new HiveCatalogOperations();
    op.initialize(Maps.newHashMap(), null, HIVE_PROPERTIES_METADATA);

    CachedClientPool clientPool = mock(CachedClientPool.class);
    HiveClient hiveClient = mock(HiveClient.class);
    HiveSchema schema = HiveSchema.builder().withCatalogName("hive").withName("db").build();
    when(hiveClient.getDatabase(anyString(), anyString())).thenReturn(schema);

    ArgumentCaptor<HiveTable> hiveTableCaptor = ArgumentCaptor.forClass(HiveTable.class);
    doNothing().when(hiveClient).createTable(hiveTableCaptor.capture());
    when(clientPool.run(any()))
        .thenAnswer(
            invocation -> {
              ClientPool.Action<?, HiveClient, ?> action = invocation.getArgument(0);
              return action.run(hiveClient);
            });
    op.clientPool = clientPool;

    Map<String, String> propertiesWithStaleMarker = Maps.newHashMap();
    propertiesWithStaleMarker.put("presto_view", "true");

    op.createView(
        NameIdentifier.of("db", "v_hive"),
        null,
        new Column[0],
        new SQLRepresentation[] {
          SQLRepresentation.builder().withDialect("hive").withSql("SELECT 1").build()
        },
        null,
        null,
        propertiesWithStaleMarker);

    Assertions.assertNull(hiveTableCaptor.getValue().properties().get("presto_view"));
  }

  @Test
  void testCreateViewPassesColumnsToHiveMetastore() throws Exception {
    HiveCatalogOperations op = new HiveCatalogOperations();
    op.initialize(Maps.newHashMap(), null, HIVE_PROPERTIES_METADATA);

    CachedClientPool clientPool = mock(CachedClientPool.class);
    HiveClient hiveClient = mock(HiveClient.class);
    HiveSchema schema = HiveSchema.builder().withCatalogName("hive").withName("db").build();
    Column[] columns = {
      Column.of("id", Types.LongType.get(), "id column"),
      Column.of("name", Types.StringType.get(), "name column")
    };
    when(hiveClient.getDatabase(anyString(), anyString())).thenReturn(schema);

    ArgumentCaptor<HiveTable> hiveTableCaptor = ArgumentCaptor.forClass(HiveTable.class);
    doNothing().when(hiveClient).createTable(hiveTableCaptor.capture());
    when(clientPool.run(any()))
        .thenAnswer(
            invocation -> {
              ClientPool.Action<?, HiveClient, ?> action = invocation.getArgument(0);
              return action.run(hiveClient);
            });
    op.clientPool = clientPool;

    View created =
        op.createView(
            NameIdentifier.of("db", "v_hive"),
            null,
            columns,
            new SQLRepresentation[] {
              SQLRepresentation.builder()
                  .withDialect("hive")
                  .withSql("SELECT id, name FROM t")
                  .build()
            },
            null,
            null,
            Maps.newHashMap());

    Assertions.assertEquals(2, hiveTableCaptor.getValue().columns().length);
    Assertions.assertEquals("id", hiveTableCaptor.getValue().columns()[0].name());
    Assertions.assertEquals("name", hiveTableCaptor.getValue().columns()[1].name());
    Assertions.assertEquals(
        "SELECT id, name FROM t", hiveTableCaptor.getValue().viewOriginalText());
    Assertions.assertEquals(2, created.columns().length);
  }

  @Test
  void testCreateViewWithUppercaseHiveDialectUnderTurkishLocale() throws Exception {
    Locale originalDefault = Locale.getDefault();
    Locale.setDefault(Locale.forLanguageTag("tr-TR"));
    try {
      HiveCatalogOperations op = new HiveCatalogOperations();
      op.initialize(Maps.newHashMap(), null, HIVE_PROPERTIES_METADATA);

      CachedClientPool clientPool = mock(CachedClientPool.class);
      HiveClient hiveClient = mock(HiveClient.class);
      HiveSchema schema = HiveSchema.builder().withCatalogName("hive").withName("db").build();
      when(hiveClient.getDatabase(anyString(), anyString())).thenReturn(schema);
      when(clientPool.run(any()))
          .thenAnswer(
              invocation -> {
                ClientPool.Action<?, HiveClient, ?> action = invocation.getArgument(0);
                return action.run(hiveClient);
              });
      op.clientPool = clientPool;

      View created =
          Assertions.assertDoesNotThrow(
              () ->
                  op.createView(
                      NameIdentifier.of("db", "v_hive_upper"),
                      null,
                      new Column[0],
                      new SQLRepresentation[] {
                        SQLRepresentation.builder().withDialect("HIVE").withSql("SELECT 1").build()
                      },
                      null,
                      null,
                      Maps.newHashMap()));

      SQLRepresentation representation = (SQLRepresentation) created.representations()[0];
      Assertions.assertEquals("SELECT 1", representation.sql());
    } finally {
      Locale.setDefault(originalDefault);
    }
  }

  @Test
  void testCreateViewRejectsNonNullDefaultCatalogAndSchema() throws Exception {
    HiveCatalogOperations op = new HiveCatalogOperations();
    op.initialize(Maps.newHashMap(), null, HIVE_PROPERTIES_METADATA);

    CachedClientPool clientPool = mock(CachedClientPool.class);
    HiveClient hiveClient = mock(HiveClient.class);
    HiveSchema schema = HiveSchema.builder().withCatalogName("hive").withName("db").build();
    when(hiveClient.getDatabase(anyString(), anyString())).thenReturn(schema);
    when(clientPool.run(any()))
        .thenAnswer(
            invocation -> {
              ClientPool.Action<?, HiveClient, ?> action = invocation.getArgument(0);
              return action.run(hiveClient);
            });
    op.clientPool = clientPool;

    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                op.createView(
                    NameIdentifier.of("db", "v_hive"),
                    null,
                    new Column[0],
                    new SQLRepresentation[] {
                      SQLRepresentation.builder().withDialect("hive").withSql("SELECT 1").build()
                    },
                    "analytics",
                    "mart",
                    Maps.newHashMap(ImmutableMap.of("created_by", "test"))));

    Assertions.assertTrue(
        exception.getMessage().contains("does not support non-null defaultCatalog/defaultSchema"));
  }

  @Test
  void testCreateViewAcceptsSparkDialect() throws Exception {
    HiveCatalogOperations op = new HiveCatalogOperations();
    op.initialize(Maps.newHashMap(), null, HIVE_PROPERTIES_METADATA);

    CachedClientPool clientPool = mock(CachedClientPool.class);
    HiveClient hiveClient = mock(HiveClient.class);
    HiveSchema schema = HiveSchema.builder().withCatalogName("hive").withName("db").build();
    when(hiveClient.getDatabase(anyString(), anyString())).thenReturn(schema);
    doNothing().when(hiveClient).createTable(any());
    when(clientPool.run(any()))
        .thenAnswer(
            invocation -> {
              ClientPool.Action<?, HiveClient, ?> action = invocation.getArgument(0);
              return action.run(hiveClient);
            });
    op.clientPool = clientPool;

    // Callers must supply spark.sql.create.version so HMS detectDialect identifies the view as
    // Spark
    View view =
        op.createView(
            NameIdentifier.of("db", "v_spark"),
            null,
            new Column[0],
            new SQLRepresentation[] {
              SQLRepresentation.builder().withDialect("spark").withSql("SELECT 1").build()
            },
            null,
            null,
            Maps.newHashMap(ImmutableMap.of("spark.sql.create.version", "3.5.3")));

    Assertions.assertNotNull(view);
    Assertions.assertEquals(1, view.representations().length);
    SQLRepresentation rep = (SQLRepresentation) view.representations()[0];
    Assertions.assertEquals("spark", rep.dialect());
    Assertions.assertEquals("SELECT 1", rep.sql());
  }

  @Test
  void testCreateViewRejectsNonSqlRepresentation() throws Exception {
    HiveCatalogOperations op = new HiveCatalogOperations();
    op.initialize(Maps.newHashMap(), null, HIVE_PROPERTIES_METADATA);

    CachedClientPool clientPool = mock(CachedClientPool.class);
    HiveClient hiveClient = mock(HiveClient.class);
    HiveSchema schema = HiveSchema.builder().withCatalogName("hive").withName("db").build();
    when(hiveClient.getDatabase(anyString(), anyString())).thenReturn(schema);
    when(clientPool.run(any()))
        .thenAnswer(
            invocation -> {
              ClientPool.Action<?, HiveClient, ?> action = invocation.getArgument(0);
              return action.run(hiveClient);
            });
    op.clientPool = clientPool;

    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                op.createView(
                    NameIdentifier.of("db", "v_non_sql"),
                    null,
                    new Column[0],
                    new Representation[] {() -> "custom"},
                    null,
                    null,
                    Maps.newHashMap()));

    Assertions.assertTrue(exception.getMessage().contains("exactly one SQL representation"));
  }

  @Test
  void testCreateViewRejectsMultipleRepresentations() throws Exception {
    HiveCatalogOperations op = new HiveCatalogOperations();
    op.initialize(Maps.newHashMap(), null, HIVE_PROPERTIES_METADATA);

    CachedClientPool clientPool = mock(CachedClientPool.class);
    HiveClient hiveClient = mock(HiveClient.class);
    HiveSchema schema = HiveSchema.builder().withCatalogName("hive").withName("db").build();
    when(hiveClient.getDatabase(anyString(), anyString())).thenReturn(schema);
    when(clientPool.run(any()))
        .thenAnswer(
            invocation -> {
              ClientPool.Action<?, HiveClient, ?> action = invocation.getArgument(0);
              return action.run(hiveClient);
            });
    op.clientPool = clientPool;

    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                op.createView(
                    NameIdentifier.of("db", "v_multi_reps"),
                    null,
                    new Column[0],
                    new Representation[] {
                      SQLRepresentation.builder().withDialect("hive").withSql("SELECT 1").build(),
                      SQLRepresentation.builder().withDialect("hive").withSql("SELECT 2").build()
                    },
                    null,
                    null,
                    Maps.newHashMap()));

    Assertions.assertTrue(exception.getMessage().contains("exactly one SQL representation"));
  }

  @Test
  void testLoadViewAcceptsTrinoDialect() throws Exception {
    HiveCatalogOperations op = new HiveCatalogOperations();
    op.initialize(Maps.newHashMap(), null, HIVE_PROPERTIES_METADATA);

    CachedClientPool clientPool = mock(CachedClientPool.class);
    HiveClient hiveClient = mock(HiveClient.class);
    String encoded =
        TrinoNativeViewCodec.encode(
            new TrinoNativeViewCodec.ViewDefinition(
                "SELECT 1",
                null,
                null,
                List.of(new TrinoNativeViewCodec.ViewColumn("_col0", "integer", null)),
                null,
                null,
                true,
                List.of()));
    when(hiveClient.getTable(anyString(), anyString(), anyString()))
        .thenReturn(
            HiveTable.builder()
                .withName("v_trino")
                .withCatalogName("hive")
                .withDatabaseName("db")
                .withColumns(new Column[0])
                .withComment("Presto View")
                .withProperties(
                    Maps.newHashMap(
                        ImmutableMap.of(
                            HiveConstants.TABLE_TYPE,
                            TableType.VIRTUAL_VIEW.name(),
                            "presto_view",
                            "true")))
                .withViewOriginalText(encoded)
                .build());
    when(clientPool.run(any()))
        .thenAnswer(
            invocation -> {
              ClientPool.Action<?, HiveClient, ?> action = invocation.getArgument(0);
              return action.run(hiveClient);
            });
    op.clientPool = clientPool;

    View loaded = op.loadView(NameIdentifier.of("db", "v_trino"));

    SQLRepresentation representation = (SQLRepresentation) loaded.representations()[0];
    Assertions.assertEquals("trino", representation.dialect());
    Assertions.assertEquals("SELECT 1", representation.sql());
  }

  @Test
  void testLoadViewAcceptsNativeTrinoView() throws Exception {
    HiveCatalogOperations op = new HiveCatalogOperations();
    op.initialize(Maps.newHashMap(), null, HIVE_PROPERTIES_METADATA);

    CachedClientPool clientPool = mock(CachedClientPool.class);
    HiveClient hiveClient = mock(HiveClient.class);
    // A native Presto/Trino view (created outside Gravitino, e.g. by a native Trino Hive
    // connector pointed at the same Hive Metastore) is encoded using Trino's own native format;
    // Gravitino must be able to read it directly, not just views it created itself.
    String encoded =
        TrinoNativeViewCodec.encode(
            new TrinoNativeViewCodec.ViewDefinition(
                "SELECT 1",
                "native_catalog",
                "native_db",
                List.of(new TrinoNativeViewCodec.ViewColumn("id", "integer", null)),
                "a comment",
                null,
                true,
                List.of()));
    when(hiveClient.getTable(anyString(), anyString(), anyString()))
        .thenReturn(
            HiveTable.builder()
                .withName("v_native_trino")
                .withCatalogName("hive")
                .withDatabaseName("db")
                .withColumns(new Column[0])
                .withComment("Presto View")
                .withProperties(
                    Maps.newHashMap(
                        ImmutableMap.of(
                            HiveConstants.TABLE_TYPE,
                            TableType.VIRTUAL_VIEW.name(),
                            "presto_view",
                            "true")))
                .withViewOriginalText(encoded)
                .build());
    when(clientPool.run(any()))
        .thenAnswer(
            invocation -> {
              ClientPool.Action<?, HiveClient, ?> action = invocation.getArgument(0);
              return action.run(hiveClient);
            });
    op.clientPool = clientPool;

    View loaded = op.loadView(NameIdentifier.of("db", "v_native_trino"));

    SQLRepresentation representation = (SQLRepresentation) loaded.representations()[0];
    Assertions.assertEquals("trino", representation.dialect());
    Assertions.assertEquals("SELECT 1", representation.sql());
    Assertions.assertEquals("a comment", loaded.comment());
    Assertions.assertEquals("native_catalog", loaded.defaultCatalog());
    Assertions.assertEquals("native_db", loaded.defaultSchema());
  }

  @Test
  void testLoadViewRejectsPrestoViewThatIsNotAPlainView() throws Exception {
    HiveCatalogOperations op = new HiveCatalogOperations();
    op.initialize(Maps.newHashMap(), null, HIVE_PROPERTIES_METADATA);

    CachedClientPool clientPool = mock(CachedClientPool.class);
    HiveClient hiveClient = mock(HiveClient.class);
    // A Trino materialized view also carries presto_view=true, but with a different comment
    // marker ("Presto Materialized View"); it must be rejected rather than misread as a plain
    // Trino view.
    when(hiveClient.getTable(anyString(), anyString(), anyString()))
        .thenReturn(
            HiveTable.builder()
                .withName("v_materialized")
                .withCatalogName("hive")
                .withDatabaseName("db")
                .withColumns(new Column[0])
                .withComment("Presto Materialized View")
                .withProperties(
                    Maps.newHashMap(
                        ImmutableMap.of(
                            HiveConstants.TABLE_TYPE,
                            TableType.VIRTUAL_VIEW.name(),
                            "presto_view",
                            "true")))
                .withViewOriginalText("/* Presto View: base64encodedpayload */")
                .build());
    when(clientPool.run(any()))
        .thenAnswer(
            invocation -> {
              ClientPool.Action<?, HiveClient, ?> action = invocation.getArgument(0);
              return action.run(hiveClient);
            });
    op.clientPool = clientPool;

    Assertions.assertThrows(
        UnsupportedOperationException.class,
        () -> op.loadView(NameIdentifier.of("db", "v_materialized")));
  }

  @Test
  void testLoadViewReturnsColumnsFromHiveMetastore() throws Exception {
    HiveCatalogOperations op = new HiveCatalogOperations();
    op.initialize(Maps.newHashMap(), null, HIVE_PROPERTIES_METADATA);

    CachedClientPool clientPool = mock(CachedClientPool.class);
    HiveClient hiveClient = mock(HiveClient.class);
    Column[] columns = {
      Column.of("id", Types.LongType.get(), "id column"),
      Column.of("name", Types.StringType.get(), "name column")
    };
    when(hiveClient.getTable(anyString(), anyString(), anyString()))
        .thenReturn(
            HiveTable.builder()
                .withName("v_hive")
                .withCatalogName("hive")
                .withDatabaseName("db")
                .withColumns(columns)
                .withProperties(
                    Maps.newHashMap(
                        ImmutableMap.of(HiveConstants.TABLE_TYPE, TableType.VIRTUAL_VIEW.name())))
                .withViewOriginalText("SELECT id, name FROM t")
                .build());
    when(clientPool.run(any()))
        .thenAnswer(
            invocation -> {
              ClientPool.Action<?, HiveClient, ?> action = invocation.getArgument(0);
              return action.run(hiveClient);
            });
    op.clientPool = clientPool;

    View loaded = op.loadView(NameIdentifier.of("db", "v_hive"));

    Assertions.assertEquals(2, loaded.columns().length);
    Assertions.assertEquals("id", loaded.columns()[0].name());
    Assertions.assertEquals("name", loaded.columns()[1].name());
  }

  @Test
  void testLoadViewUsesOriginalText() throws Exception {
    HiveCatalogOperations op = new HiveCatalogOperations();
    op.initialize(Maps.newHashMap(), null, HIVE_PROPERTIES_METADATA);

    CachedClientPool clientPool = mock(CachedClientPool.class);
    HiveClient hiveClient = mock(HiveClient.class);
    when(hiveClient.getTable(anyString(), anyString(), anyString()))
        .thenReturn(
            HiveTable.builder()
                .withName("v_hive")
                .withCatalogName("hive")
                .withDatabaseName("db")
                .withColumns(new Column[0])
                .withProperties(
                    Maps.newHashMap(
                        ImmutableMap.of(HiveConstants.TABLE_TYPE, TableType.VIRTUAL_VIEW.name())))
                .withViewOriginalText("SELECT id, name FROM t")
                .build());
    when(clientPool.run(any()))
        .thenAnswer(
            invocation -> {
              ClientPool.Action<?, HiveClient, ?> action = invocation.getArgument(0);
              return action.run(hiveClient);
            });
    op.clientPool = clientPool;

    View loaded = op.loadView(NameIdentifier.of("db", "v_hive"));

    SQLRepresentation representation = (SQLRepresentation) loaded.representations()[0];
    Assertions.assertEquals("SELECT id, name FROM t", representation.sql());
  }

  @Test
  void testAlterViewReplaceAcceptsTrinoDialect() throws Exception {
    HiveCatalogOperations op = new HiveCatalogOperations();
    op.initialize(Maps.newHashMap(), null, HIVE_PROPERTIES_METADATA);

    CachedClientPool clientPool = mock(CachedClientPool.class);
    HiveClient hiveClient = mock(HiveClient.class);
    String encoded =
        TrinoNativeViewCodec.encode(
            new TrinoNativeViewCodec.ViewDefinition(
                "SELECT 1",
                null,
                null,
                List.of(new TrinoNativeViewCodec.ViewColumn("c1", "integer", null)),
                null,
                null,
                true,
                List.of()));
    HiveTable currentTable =
        HiveTable.builder()
            .withName("v_hive")
            .withCatalogName("hive")
            .withDatabaseName("db")
            .withColumns(new Column[0])
            .withComment("Presto View")
            .withProperties(
                Maps.newHashMap(
                    ImmutableMap.of(
                        HiveConstants.TABLE_TYPE,
                        TableType.VIRTUAL_VIEW.name(),
                        "presto_view",
                        "true")))
            .withViewOriginalText(encoded)
            .build();
    when(hiveClient.getTable(anyString(), anyString(), anyString())).thenReturn(currentTable);

    ArgumentCaptor<HiveTable> hiveTableCaptor = ArgumentCaptor.forClass(HiveTable.class);
    doNothing()
        .when(hiveClient)
        .alterTable(anyString(), anyString(), anyString(), hiveTableCaptor.capture());
    when(clientPool.run(any()))
        .thenAnswer(
            invocation -> {
              ClientPool.Action<?, HiveClient, ?> action = invocation.getArgument(0);
              return action.run(hiveClient);
            });
    op.clientPool = clientPool;

    View updated =
        op.alterView(
            NameIdentifier.of("db", "v_hive"),
            ViewChange.replaceView(
                new Column[] {Column.of("c1", Types.IntegerType.get())},
                new SQLRepresentation[] {
                  SQLRepresentation.builder().withDialect("trino").withSql("SELECT 2").build()
                },
                null,
                null,
                null));

    Assertions.assertEquals("true", hiveTableCaptor.getValue().properties().get("presto_view"));
    Assertions.assertEquals("Presto View", hiveTableCaptor.getValue().properties().get("comment"));
    TrinoNativeViewCodec.ViewDefinition decoded =
        TrinoNativeViewCodec.decode(hiveTableCaptor.getValue().viewOriginalText());
    Assertions.assertEquals("SELECT 2", decoded.originalSql);
    SQLRepresentation representation = (SQLRepresentation) updated.representations()[0];
    Assertions.assertEquals("trino", representation.dialect());
    Assertions.assertEquals("SELECT 2", representation.sql());
  }

  @Test
  void testAlterViewReplaceRejectsExistingNonDefaultOwner() throws Exception {
    // Gravitino's view model has no owner concept; replacing a native Trino view that has a
    // non-null owner (a SECURITY DEFINER view) would silently turn it into an ownerless view.
    assertReplaceRejectsUnrepresentableNativeView(
        TrinoNativeViewCodec.encode(
            new TrinoNativeViewCodec.ViewDefinition(
                "SELECT 1",
                null,
                null,
                List.of(new TrinoNativeViewCodec.ViewColumn("c1", "integer", null)),
                null,
                "alice",
                true,
                List.of())));
  }

  @Test
  void testAlterViewReplaceRejectsExistingRunAsInvokerFalse() throws Exception {
    // runAsInvoker=false means SECURITY DEFINER; Gravitino always writes runAsInvoker=true, so
    // replacing such a view would silently downgrade it to SECURITY INVOKER.
    assertReplaceRejectsUnrepresentableNativeView(
        TrinoNativeViewCodec.encode(
            new TrinoNativeViewCodec.ViewDefinition(
                "SELECT 1",
                null,
                null,
                List.of(new TrinoNativeViewCodec.ViewColumn("c1", "integer", null)),
                null,
                null,
                false,
                List.of())));
  }

  @Test
  void testAlterViewReplaceRejectsExistingNonEmptyPath() throws Exception {
    // Gravitino's view model has no SQL path concept; replacing a native Trino view that has a
    // non-empty path would silently discard it. TrinoNativeViewCodec.encode() always writes an
    // empty path (Gravitino itself never produces one), so this payload is built by hand to
    // simulate a view created directly by a native Trino connector with a non-empty path.
    String encoded =
        "/* Presto View: "
            + Base64.getEncoder()
                .encodeToString(
                    ("{\"originalSql\":\"SELECT 1\",\"catalog\":null,\"schema\":null,"
                            + "\"columns\":[{\"name\":\"c1\",\"type\":\"integer\",\"comment\":null}],"
                            + "\"comment\":null,\"owner\":null,\"runAsInvoker\":true,"
                            + "\"path\":[{\"catalog\":\"c\",\"schema\":\"s\"}]}")
                        .getBytes(StandardCharsets.UTF_8))
            + " */";
    assertReplaceRejectsUnrepresentableNativeView(encoded);
  }

  private void assertReplaceRejectsUnrepresentableNativeView(String encoded) throws Exception {
    HiveCatalogOperations op = new HiveCatalogOperations();
    op.initialize(Maps.newHashMap(), null, HIVE_PROPERTIES_METADATA);

    CachedClientPool clientPool = mock(CachedClientPool.class);
    HiveClient hiveClient = mock(HiveClient.class);
    HiveTable currentTable =
        HiveTable.builder()
            .withName("v_trino")
            .withCatalogName("hive")
            .withDatabaseName("db")
            .withColumns(new Column[0])
            .withComment("Presto View")
            .withProperties(
                Maps.newHashMap(
                    ImmutableMap.of(
                        HiveConstants.TABLE_TYPE,
                        TableType.VIRTUAL_VIEW.name(),
                        "presto_view",
                        "true")))
            .withViewOriginalText(encoded)
            .build();
    when(hiveClient.getTable(anyString(), anyString(), anyString())).thenReturn(currentTable);
    when(clientPool.run(any()))
        .thenAnswer(
            invocation -> {
              ClientPool.Action<?, HiveClient, ?> action = invocation.getArgument(0);
              return action.run(hiveClient);
            });
    op.clientPool = clientPool;

    Assertions.assertThrows(
        UnsupportedOperationException.class,
        () ->
            op.alterView(
                NameIdentifier.of("db", "v_trino"),
                ViewChange.replaceView(
                    new Column[] {Column.of("c1", Types.IntegerType.get())},
                    new SQLRepresentation[] {
                      SQLRepresentation.builder().withDialect("trino").withSql("SELECT 2").build()
                    },
                    null,
                    null,
                    null)));
  }

  @Test
  void testAlterViewRejectsSetPropertyCommentOnTrinoView() throws Exception {
    // A Trino dialect view's HMS "comment" property is fixed to "Presto View" (the marker Trino
    // itself relies on to recognize the view); the real comment lives inside the encoded payload.
    // Setting it directly (bypassing ReplaceView) would desynchronize the two and make the view
    // unloadable on the next read, so it must be rejected instead.
    HiveCatalogOperations op = new HiveCatalogOperations();
    op.initialize(Maps.newHashMap(), null, HIVE_PROPERTIES_METADATA);

    CachedClientPool clientPool = mock(CachedClientPool.class);
    HiveClient hiveClient = mock(HiveClient.class);
    String encoded =
        TrinoNativeViewCodec.encode(
            new TrinoNativeViewCodec.ViewDefinition(
                "SELECT 1",
                null,
                null,
                List.of(new TrinoNativeViewCodec.ViewColumn("c1", "integer", null)),
                null,
                null,
                true,
                List.of()));
    HiveTable currentTable =
        HiveTable.builder()
            .withName("v_trino")
            .withCatalogName("hive")
            .withDatabaseName("db")
            .withColumns(new Column[0])
            .withComment("Presto View")
            .withProperties(
                Maps.newHashMap(
                    ImmutableMap.of(
                        HiveConstants.TABLE_TYPE,
                        TableType.VIRTUAL_VIEW.name(),
                        "presto_view",
                        "true")))
            .withViewOriginalText(encoded)
            .build();
    when(hiveClient.getTable(anyString(), anyString(), anyString())).thenReturn(currentTable);
    when(clientPool.run(any()))
        .thenAnswer(
            invocation -> {
              ClientPool.Action<?, HiveClient, ?> action = invocation.getArgument(0);
              return action.run(hiveClient);
            });
    op.clientPool = clientPool;

    Assertions.assertThrows(
        UnsupportedOperationException.class,
        () ->
            op.alterView(
                NameIdentifier.of("db", "v_trino"), ViewChange.setProperty("comment", "my note")));
    Assertions.assertThrows(
        UnsupportedOperationException.class,
        () ->
            op.alterView(NameIdentifier.of("db", "v_trino"), ViewChange.removeProperty("comment")));
  }

  @Test
  void testAlterViewRejectsRemovingPrestoViewMarkerFromTrinoView() throws Exception {
    // The presto_view HMS property is part of the native Trino view storage contract; removing it
    // directly (bypassing ReplaceView) would leave the encoded payload in viewOriginalText while
    // making the view misclassify as Hive dialect on the next load, exposing the raw payload as
    // SQL.
    HiveCatalogOperations op = new HiveCatalogOperations();
    op.initialize(Maps.newHashMap(), null, HIVE_PROPERTIES_METADATA);

    CachedClientPool clientPool = mock(CachedClientPool.class);
    HiveClient hiveClient = mock(HiveClient.class);
    String encoded =
        TrinoNativeViewCodec.encode(
            new TrinoNativeViewCodec.ViewDefinition(
                "SELECT 1",
                null,
                null,
                List.of(new TrinoNativeViewCodec.ViewColumn("c1", "integer", null)),
                null,
                null,
                true,
                List.of()));
    HiveTable currentTable =
        HiveTable.builder()
            .withName("v_trino")
            .withCatalogName("hive")
            .withDatabaseName("db")
            .withColumns(new Column[0])
            .withComment("Presto View")
            .withProperties(
                Maps.newHashMap(
                    ImmutableMap.of(
                        HiveConstants.TABLE_TYPE,
                        TableType.VIRTUAL_VIEW.name(),
                        "presto_view",
                        "true")))
            .withViewOriginalText(encoded)
            .build();
    when(hiveClient.getTable(anyString(), anyString(), anyString())).thenReturn(currentTable);
    when(clientPool.run(any()))
        .thenAnswer(
            invocation -> {
              ClientPool.Action<?, HiveClient, ?> action = invocation.getArgument(0);
              return action.run(hiveClient);
            });
    op.clientPool = clientPool;

    Assertions.assertThrows(
        UnsupportedOperationException.class,
        () ->
            op.alterView(
                NameIdentifier.of("db", "v_trino"), ViewChange.removeProperty("presto_view")));
  }

  @Test
  void testAlterViewRejectsSettingPrestoViewMarkerOnNonTrinoView() throws Exception {
    // Setting presto_view=true on a plain Hive view directly (bypassing ReplaceView) would make
    // the view misclassify as Trino dialect on the next load, and decoding its plain SQL
    // viewOriginalText as a Trino native payload would fail.
    HiveCatalogOperations op = new HiveCatalogOperations();
    op.initialize(Maps.newHashMap(), null, HIVE_PROPERTIES_METADATA);

    CachedClientPool clientPool = mock(CachedClientPool.class);
    HiveClient hiveClient = mock(HiveClient.class);
    HiveTable currentTable =
        HiveTable.builder()
            .withName("v_hive")
            .withCatalogName("hive")
            .withDatabaseName("db")
            .withColumns(new Column[0])
            .withProperties(
                Maps.newHashMap(
                    ImmutableMap.of(HiveConstants.TABLE_TYPE, TableType.VIRTUAL_VIEW.name())))
            .withViewOriginalText("SELECT 1")
            .build();
    when(hiveClient.getTable(anyString(), anyString(), anyString())).thenReturn(currentTable);
    when(clientPool.run(any()))
        .thenAnswer(
            invocation -> {
              ClientPool.Action<?, HiveClient, ?> action = invocation.getArgument(0);
              return action.run(hiveClient);
            });
    op.clientPool = clientPool;

    Assertions.assertThrows(
        UnsupportedOperationException.class,
        () ->
            op.alterView(
                NameIdentifier.of("db", "v_hive"), ViewChange.setProperty("presto_view", "true")));
  }

  @Test
  void testAlterViewReplaceClearsTrinoMarkerForNonTrinoDialect() throws Exception {
    HiveCatalogOperations op = new HiveCatalogOperations();
    op.initialize(Maps.newHashMap(), null, HIVE_PROPERTIES_METADATA);

    CachedClientPool clientPool = mock(CachedClientPool.class);
    HiveClient hiveClient = mock(HiveClient.class);
    String encoded =
        TrinoNativeViewCodec.encode(
            new TrinoNativeViewCodec.ViewDefinition(
                "SELECT 1",
                null,
                null,
                List.of(new TrinoNativeViewCodec.ViewColumn("c1", "integer", null)),
                null,
                null,
                true,
                List.of()));
    HiveTable currentTable =
        HiveTable.builder()
            .withName("v_trino")
            .withCatalogName("hive")
            .withDatabaseName("db")
            .withColumns(new Column[0])
            .withComment("Presto View")
            .withProperties(
                Maps.newHashMap(
                    ImmutableMap.of(
                        HiveConstants.TABLE_TYPE,
                        TableType.VIRTUAL_VIEW.name(),
                        "presto_view",
                        "true")))
            .withViewOriginalText(encoded)
            .build();
    when(hiveClient.getTable(anyString(), anyString(), anyString())).thenReturn(currentTable);

    ArgumentCaptor<HiveTable> hiveTableCaptor = ArgumentCaptor.forClass(HiveTable.class);
    doNothing()
        .when(hiveClient)
        .alterTable(anyString(), anyString(), anyString(), hiveTableCaptor.capture());
    when(clientPool.run(any()))
        .thenAnswer(
            invocation -> {
              ClientPool.Action<?, HiveClient, ?> action = invocation.getArgument(0);
              return action.run(hiveClient);
            });
    op.clientPool = clientPool;

    op.alterView(
        NameIdentifier.of("db", "v_trino"),
        ViewChange.replaceView(
            new Column[0],
            new SQLRepresentation[] {
              SQLRepresentation.builder().withDialect("hive").withSql("SELECT 2").build()
            },
            null,
            null,
            null));

    Assertions.assertNull(hiveTableCaptor.getValue().properties().get("presto_view"));
  }

  @Test
  void testAlterViewReplacePassesReplacementColumnsToHiveMetastore() throws Exception {
    HiveCatalogOperations op = new HiveCatalogOperations();
    op.initialize(Maps.newHashMap(), null, HIVE_PROPERTIES_METADATA);

    CachedClientPool clientPool = mock(CachedClientPool.class);
    HiveClient hiveClient = mock(HiveClient.class);
    Column[] currentColumns = {Column.of("id", Types.LongType.get(), "id column")};
    Column[] replacementColumns = {
      Column.of("id", Types.LongType.get(), "id column"),
      Column.of("name", Types.StringType.get(), "name column")
    };
    HiveTable currentTable =
        HiveTable.builder()
            .withName("v_hive")
            .withCatalogName("hive")
            .withDatabaseName("db")
            .withColumns(currentColumns)
            .withProperties(
                Maps.newHashMap(
                    ImmutableMap.of(HiveConstants.TABLE_TYPE, TableType.VIRTUAL_VIEW.name())))
            .withViewOriginalText("SELECT id FROM t")
            .build();
    when(hiveClient.getTable(anyString(), anyString(), anyString())).thenReturn(currentTable);

    ArgumentCaptor<HiveTable> hiveTableCaptor = ArgumentCaptor.forClass(HiveTable.class);
    doNothing()
        .when(hiveClient)
        .alterTable(anyString(), anyString(), anyString(), hiveTableCaptor.capture());
    when(clientPool.run(any()))
        .thenAnswer(
            invocation -> {
              ClientPool.Action<?, HiveClient, ?> action = invocation.getArgument(0);
              return action.run(hiveClient);
            });
    op.clientPool = clientPool;

    View updated =
        op.alterView(
            NameIdentifier.of("db", "v_hive"),
            ViewChange.replaceView(
                replacementColumns,
                new SQLRepresentation[] {
                  SQLRepresentation.builder()
                      .withDialect("hive")
                      .withSql("SELECT id, name FROM t")
                      .build()
                },
                null,
                null,
                "new comment"));

    Assertions.assertEquals(2, hiveTableCaptor.getValue().columns().length);
    Assertions.assertEquals("id", hiveTableCaptor.getValue().columns()[0].name());
    Assertions.assertEquals("name", hiveTableCaptor.getValue().columns()[1].name());
    Assertions.assertEquals(
        "SELECT id, name FROM t", hiveTableCaptor.getValue().viewOriginalText());
    Assertions.assertEquals("new comment", hiveTableCaptor.getValue().comment());
    Assertions.assertEquals(2, updated.columns().length);
    Assertions.assertEquals("new comment", updated.comment());
  }

  @Test
  void testAlterViewRenameThrowsWhenTargetViewExists() throws Exception {
    HiveCatalogOperations op = new HiveCatalogOperations();
    op.initialize(Maps.newHashMap(), null, HIVE_PROPERTIES_METADATA);

    CachedClientPool clientPool = mock(CachedClientPool.class);
    HiveClient hiveClient = mock(HiveClient.class);
    HiveTable sourceView =
        HiveTable.builder()
            .withName("v_source")
            .withCatalogName("hive")
            .withDatabaseName("db")
            .withColumns(new Column[0])
            .withProperties(
                Maps.newHashMap(
                    ImmutableMap.of(HiveConstants.TABLE_TYPE, TableType.VIRTUAL_VIEW.name())))
            .withViewOriginalText("SELECT 1")
            .build();
    HiveTable targetView =
        HiveTable.builder()
            .withName("v_target")
            .withCatalogName("hive")
            .withDatabaseName("db")
            .withColumns(new Column[0])
            .withProperties(
                Maps.newHashMap(
                    ImmutableMap.of(HiveConstants.TABLE_TYPE, TableType.VIRTUAL_VIEW.name())))
            .withViewOriginalText("SELECT 2")
            .build();

    when(hiveClient.getTable(anyString(), anyString(), anyString()))
        .thenAnswer(
            invocation -> {
              String tableName = invocation.getArgument(2);
              if ("v_source".equals(tableName)) {
                return sourceView;
              }
              if ("v_target".equals(tableName)) {
                return targetView;
              }
              throw new RuntimeException("NoSuchObjectException: unknown table " + tableName);
            });
    when(clientPool.run(any()))
        .thenAnswer(
            invocation -> {
              ClientPool.Action<?, HiveClient, ?> action = invocation.getArgument(0);
              return action.run(hiveClient);
            });
    op.clientPool = clientPool;

    ViewAlreadyExistsException exception =
        Assertions.assertThrows(
            ViewAlreadyExistsException.class,
            () -> op.alterView(NameIdentifier.of("db", "v_source"), ViewChange.rename("v_target")));
    Assertions.assertTrue(exception.getMessage().contains("already exists"));
  }

  @Test
  void testAlterViewReplaceRejectsNonNullDefaultCatalogAndSchema() throws Exception {
    HiveCatalogOperations op = new HiveCatalogOperations();
    op.initialize(Maps.newHashMap(), null, HIVE_PROPERTIES_METADATA);

    CachedClientPool clientPool = mock(CachedClientPool.class);
    HiveClient hiveClient = mock(HiveClient.class);
    HiveTable currentTable =
        HiveTable.builder()
            .withName("v_hive")
            .withCatalogName("hive")
            .withDatabaseName("db")
            .withColumns(new Column[0])
            .withProperties(
                Maps.newHashMap(
                    ImmutableMap.of(HiveConstants.TABLE_TYPE, TableType.VIRTUAL_VIEW.name())))
            .withViewOriginalText("SELECT 1")
            .build();
    when(hiveClient.getTable(anyString(), anyString(), anyString())).thenReturn(currentTable);
    when(clientPool.run(any()))
        .thenAnswer(
            invocation -> {
              ClientPool.Action<?, HiveClient, ?> action = invocation.getArgument(0);
              return action.run(hiveClient);
            });
    op.clientPool = clientPool;

    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                op.alterView(
                    NameIdentifier.of("db", "v_hive"),
                    ViewChange.replaceView(
                        new Column[0],
                        new SQLRepresentation[] {
                          SQLRepresentation.builder()
                              .withDialect("hive")
                              .withSql("SELECT 2")
                              .build()
                        },
                        "analytics",
                        "mart",
                        null)));

    Assertions.assertTrue(
        exception.getMessage().contains("does not support non-null defaultCatalog/defaultSchema"));
  }

  @Test
  void testDropViewThrowsForUnexpectedException() throws Exception {
    HiveCatalogOperations op = new HiveCatalogOperations();
    op.initialize(Maps.newHashMap(), null, HIVE_PROPERTIES_METADATA);

    CachedClientPool clientPool = mock(CachedClientPool.class);
    HiveClient hiveClient = mock(HiveClient.class);
    HiveTable currentView =
        HiveTable.builder()
            .withName("v1")
            .withCatalogName("hive")
            .withDatabaseName("db")
            .withColumns(new Column[0])
            .withProperties(
                Maps.newHashMap(
                    ImmutableMap.of(HiveConstants.TABLE_TYPE, TableType.VIRTUAL_VIEW.name())))
            .withViewOriginalText("SELECT 1")
            .build();
    when(hiveClient.getTable(anyString(), anyString(), anyString())).thenReturn(currentView);
    doAnswer(
            invocation -> {
              throw new RuntimeException("permission denied");
            })
        .when(hiveClient)
        .dropTable(anyString(), anyString(), anyString(), anyBoolean(), anyBoolean());
    when(clientPool.run(any()))
        .thenAnswer(
            invocation -> {
              ClientPool.Action<?, HiveClient, ?> action = invocation.getArgument(0);
              return action.run(hiveClient);
            });
    op.clientPool = clientPool;

    RuntimeException exception =
        Assertions.assertThrows(
            RuntimeException.class, () -> op.dropView(NameIdentifier.of("db", "v1")));
    Assertions.assertTrue(exception.getMessage().contains("Failed to drop Hive view"));
  }

  @Test
  void testListTablesFiltersViewsWithHiveTableTypeField() throws Exception {
    HiveCatalogOperations op = new HiveCatalogOperations();
    op.initialize(ImmutableMap.of(LIST_ALL_TABLES, "true"), null, HIVE_PROPERTIES_METADATA);

    CachedClientPool clientPool = mock(CachedClientPool.class);
    HiveClient hiveClient = mock(HiveClient.class);
    when(hiveClient.getAllDatabases(anyString())).thenReturn(List.of("db"));
    when(hiveClient.getAllTables(anyString(), anyString()))
        .thenReturn(new ArrayList<>(List.of("v1", "t1")));
    when(hiveClient.listTablesByType(anyString(), anyString(), anyString(), anyString()))
        .thenReturn(List.of("v1"));
    when(clientPool.run(any()))
        .thenAnswer(
            invocation -> {
              ClientPool.Action<?, HiveClient, ?> action = invocation.getArgument(0);
              return action.run(hiveClient);
            });
    op.clientPool = clientPool;

    NameIdentifier[] tables = op.listTables(Namespace.of("metalake", "hive", "db"));

    Assertions.assertEquals(1, tables.length);
    Assertions.assertEquals("t1", tables[0].name());
    verify(hiveClient).getAllTables(eq("hive"), eq("db"));
    verify(hiveClient)
        .listTablesByType(eq("hive"), eq("db"), eq("*"), eq(TableType.VIRTUAL_VIEW.name()));
    verify(hiveClient, never())
        .listTableNamesByFilter(anyString(), anyString(), anyString(), anyShort());
  }

  @Test
  void testListTablesRemovesOnlyHudiBaseAndDerivedTables() throws Exception {
    HiveCatalogOperations op = new HiveCatalogOperations();
    op.initialize(ImmutableMap.of(LIST_ALL_TABLES, "false"), null, HIVE_PROPERTIES_METADATA);

    CachedClientPool clientPool = mock(CachedClientPool.class);
    HiveClient hiveClient = mock(HiveClient.class);
    when(hiveClient.getAllDatabases(anyString())).thenReturn(List.of("db"));
    when(hiveClient.getAllTables(anyString(), anyString()))
        .thenReturn(
            new ArrayList<>(
                List.of(
                    "v1",
                    "hive_tbl",
                    "ice_tbl",
                    "hudi_base",
                    "hudi_base_ro",
                    "hudi_base_rt",
                    "hudi_base_root")));
    when(hiveClient.listTablesByType(anyString(), anyString(), anyString(), anyString()))
        .thenReturn(List.of("v1"));

    String icebergAndPaimonFilter =
        String.format(
            "%stable_type like \"ICEBERG\" or %stable_type like \"PAIMON\"",
            HiveConstants.HIVE_FILTER_FIELD_PARAMS, HiveConstants.HIVE_FILTER_FIELD_PARAMS);
    String hudiBaseFilter =
        String.format("%sprovider like \"hudi\"", HiveConstants.HIVE_FILTER_FIELD_PARAMS);
    when(hiveClient.listTableNamesByFilter(anyString(), anyString(), anyString(), anyShort()))
        .thenAnswer(
            invocation -> {
              String filter = invocation.getArgument(2);
              if (icebergAndPaimonFilter.equals(filter)) {
                return List.of("ice_tbl");
              }

              if (hudiBaseFilter.equals(filter)) {
                return List.of("hudi_base");
              }

              return List.of();
            });
    when(clientPool.run(any()))
        .thenAnswer(
            invocation -> {
              ClientPool.Action<?, HiveClient, ?> action = invocation.getArgument(0);
              return action.run(hiveClient);
            });
    op.clientPool = clientPool;

    NameIdentifier[] tables = op.listTables(Namespace.of("metalake", "hive", "db"));

    Assertions.assertEquals(2, tables.length);
    Assertions.assertEquals("hive_tbl", tables[0].name());
    Assertions.assertEquals("hudi_base_root", tables[1].name());
    verify(hiveClient)
        .listTableNamesByFilter(eq("hive"), eq("db"), eq(icebergAndPaimonFilter), anyShort());
    verify(hiveClient).listTableNamesByFilter(eq("hive"), eq("db"), eq(hudiBaseFilter), anyShort());
  }

  @Test
  void testListViewsUsesTypeListing() throws Exception {
    HiveCatalogOperations op = new HiveCatalogOperations();
    op.initialize(ImmutableMap.of(LIST_ALL_TABLES, "true"), null, HIVE_PROPERTIES_METADATA);

    CachedClientPool clientPool = mock(CachedClientPool.class);
    HiveClient hiveClient = mock(HiveClient.class);
    when(hiveClient.getAllDatabases(anyString())).thenReturn(List.of("db"));
    when(hiveClient.listTablesByType(anyString(), anyString(), anyString(), anyString()))
        .thenReturn(List.of("v1", "v2"));
    when(clientPool.run(any()))
        .thenAnswer(
            invocation -> {
              ClientPool.Action<?, HiveClient, ?> action = invocation.getArgument(0);
              return action.run(hiveClient);
            });
    op.clientPool = clientPool;

    NameIdentifier[] views = op.listViews(Namespace.of("metalake", "hive", "db"));

    Assertions.assertEquals(2, views.length);
    verify(hiveClient)
        .listTablesByType(eq("hive"), eq("db"), eq("*"), eq(TableType.VIRTUAL_VIEW.name()));
    verify(hiveClient, never()).getAllTables(anyString(), anyString());
    verify(hiveClient, never()).getTable(anyString(), anyString(), anyString());
    verify(hiveClient, never())
        .listTableNamesByFilter(anyString(), anyString(), anyString(), anyShort());
  }

  @Test
  void testDropViewReturnsFalseWhenTargetIsTable() throws Exception {
    HiveCatalogOperations op = new HiveCatalogOperations();
    op.initialize(Maps.newHashMap(), null, HIVE_PROPERTIES_METADATA);

    CachedClientPool clientPool = mock(CachedClientPool.class);
    HiveClient hiveClient = mock(HiveClient.class);
    HiveTable table =
        HiveTable.builder()
            .withName("t1")
            .withCatalogName("hive")
            .withDatabaseName("db")
            .withColumns(new Column[0])
            .withProperties(
                Maps.newHashMap(
                    ImmutableMap.of(HiveConstants.TABLE_TYPE, TableType.MANAGED_TABLE.name())))
            .build();
    when(hiveClient.getTable(anyString(), anyString(), anyString())).thenReturn(table);
    doAnswer(
            invocation -> {
              throw new AssertionError("dropTable should not be called for non-view objects");
            })
        .when(hiveClient)
        .dropTable(anyString(), anyString(), anyString(), anyBoolean(), anyBoolean());
    when(clientPool.run(any()))
        .thenAnswer(
            invocation -> {
              ClientPool.Action<?, HiveClient, ?> action = invocation.getArgument(0);
              return action.run(hiveClient);
            });
    op.clientPool = clientPool;

    boolean dropped = op.dropView(NameIdentifier.of("db", "t1"));
    Assertions.assertFalse(dropped);
  }
}
