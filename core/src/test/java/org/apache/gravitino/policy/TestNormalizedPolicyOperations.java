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

package org.apache.gravitino.policy;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.Config;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.cache.NoOpsCache;
import org.apache.gravitino.catalog.CatalogDispatcher;
import org.apache.gravitino.catalog.CatalogManager;
import org.apache.gravitino.catalog.CatalogTestUtils;
import org.apache.gravitino.catalog.SchemaDispatcher;
import org.apache.gravitino.catalog.TableDispatcher;
import org.apache.gravitino.connector.BaseCatalog;
import org.apache.gravitino.connector.capability.Capability;
import org.apache.gravitino.connector.capability.CapabilityResult;
import org.apache.gravitino.exceptions.NoSuchMetadataObjectException;
import org.apache.gravitino.lock.LockManager;
import org.apache.gravitino.meta.ColumnEntity;
import org.apache.gravitino.meta.TableEntity;
import org.apache.gravitino.rel.types.Types;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.apache.gravitino.storage.relational.RelationalEntityStore;
import org.apache.gravitino.storage.relational.TestJDBCBackend;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.mockito.Mockito;

class TestNormalizedPolicyOperations extends TestJDBCBackend {
  private static final String METALAKE = "normalized_policy";
  private static final String POLICY = "MixedPolicy";
  private final Map<String, Object> originalFields = new LinkedHashMap<>();
  private CatalogManager catalogs;
  private BaseCatalog catalog;
  private PolicyManager policies;

  // BackendTestExtension replaces the GravitinoEnv config and ID generator; restore them so later
  // test classes in the same JVM see the original values.
  private static Object originalEnvConfig;
  private static Object originalEnvIdGenerator;

  @BeforeAll
  static void saveEnvConfig() throws IllegalAccessException {
    GravitinoEnv env = GravitinoEnv.getInstance();
    originalEnvConfig = FieldUtils.readField(env, "config", true);
    originalEnvIdGenerator = FieldUtils.readField(env, "idGenerator", true);
  }

  @AfterAll
  static void restoreEnvConfig() throws IllegalAccessException {
    GravitinoEnv env = GravitinoEnv.getInstance();
    FieldUtils.writeField(env, "config", originalEnvConfig, true);
    FieldUtils.writeField(env, "idGenerator", originalEnvIdGenerator, true);
  }

  @BeforeEach
  void setUp() throws Exception {
    GravitinoEnv env = GravitinoEnv.getInstance();
    for (String field :
        new String[] {
          "catalogManager",
          "lockManager",
          "internalCatalogDispatcher",
          "internalSchemaDispatcher",
          "internalTableDispatcher"
        }) {
      originalFields.put(field, FieldUtils.readField(env, field, true));
    }
    Config config = new Config(false) {};
    RelationalEntityStore store = new RelationalEntityStore();
    FieldUtils.writeField(store, "backend", backend, true);
    FieldUtils.writeField(store, "cache", new NoOpsCache(config), true);
    catalogs = mock(CatalogManager.class);
    catalog = mock(BaseCatalog.class);
    CatalogTestUtils.mockDoWithCatalog(catalogs, catalog);
    when(catalog.capability()).thenReturn(foldingCapability());
    FieldUtils.writeField(env, "catalogManager", catalogs, true);
    FieldUtils.writeField(env, "lockManager", new LockManager(config), true);
    CatalogDispatcher catalogDispatcher = mock(CatalogDispatcher.class);
    when(catalogDispatcher.catalogExists(any())).thenReturn(true);
    FieldUtils.writeField(env, "internalCatalogDispatcher", catalogDispatcher, true);
    SchemaDispatcher schemas = mock(SchemaDispatcher.class);
    when(schemas.schemaExists(any())).thenReturn(true);
    FieldUtils.writeField(env, "internalSchemaDispatcher", schemas, true);
    TableDispatcher tables = mock(TableDispatcher.class);
    when(tables.tableExists(any())).thenReturn(true);
    FieldUtils.writeField(env, "internalTableDispatcher", tables, true);
    createAndInsertMakeLake(METALAKE);
    createAndInsertCatalog(METALAKE, "cat");
    createAndInsertSchema(METALAKE, "cat", "analytics");
    createAndInsertTableEntity(Namespace.of(METALAKE, "cat", "analytics"), "orders");
    policies = new PolicyManager(RandomIdGenerator.INSTANCE, store);
    policies.createPolicy(
        METALAKE,
        POLICY,
        Policy.BuiltInType.CUSTOM,
        null,
        true,
        PolicyContents.custom(
            Map.of(),
            Set.of(
                MetadataObject.Type.CATALOG, MetadataObject.Type.SCHEMA, MetadataObject.Type.TABLE),
            null));
  }

  @AfterEach
  void tearDown() throws Exception {
    for (Map.Entry<String, Object> field : originalFields.entrySet()) {
      FieldUtils.writeField(GravitinoEnv.getInstance(), field.getKey(), field.getValue(), true);
    }
  }

  @TestTemplate
  void testEquivalentSchemaAndTableNamesSharePolicyRelations() {
    for (MetadataObject.Type type :
        new MetadataObject.Type[] {MetadataObject.Type.SCHEMA, MetadataObject.Type.TABLE}) {
      String suffix = type == MetadataObject.Type.TABLE ? ".orders" : "";
      MetadataObject lower = MetadataObjects.parse("cat.analytics" + suffix, type);
      MetadataObject upper =
          MetadataObjects.parse("cat.ANALYTICS" + suffix.toUpperCase(Locale.ROOT), type);
      assertArrayEquals(
          new String[] {POLICY},
          policies.associatePoliciesForMetadataObject(
              METALAKE, upper, new String[] {POLICY}, null));
      assertEquals(POLICY, policies.getPolicyForMetadataObject(METALAKE, lower, POLICY).name());
      assertEquals(POLICY, policies.getPolicyForMetadataObject(METALAKE, upper, POLICY).name());
      assertEquals(POLICY, policies.listPolicyInfosForMetadataObject(METALAKE, upper)[0].name());
      policies.associatePoliciesForMetadataObject(METALAKE, upper, null, new String[] {POLICY});
      assertEquals(0, policies.listPolicyInfosForMetadataObject(METALAKE, lower).length);
    }
  }

  @TestTemplate
  void testColumnReadsNormalizeParentsAndKeepColumnCase() throws Exception {
    ColumnEntity column =
        ColumnEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName("MixedColumn")
            .withPosition(0)
            .withDataType(Types.IntegerType.get())
            .withNullable(true)
            .withAutoIncrement(false)
            .withAuditInfo(AUDIT_INFO)
            .build();
    TableEntity table =
        TableEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName("column_table")
            .withNamespace(Namespace.of(METALAKE, "cat", "analytics"))
            .withColumns(List.of(column))
            .withAuditInfo(AUDIT_INFO)
            .build();
    backend.insert(table, false);
    MetadataObject request =
        MetadataObjects.parse("cat.ANALYTICS.COLUMN_TABLE.MixedColumn", MetadataObject.Type.COLUMN);
    assertEquals(0, policies.listPolicyInfosForMetadataObject(METALAKE, request).length);
    MetadataObject wrongColumn =
        MetadataObjects.parse("cat.ANALYTICS.COLUMN_TABLE.MIXEDCOLUMN", MetadataObject.Type.COLUMN);
    assertThrows(
        NoSuchMetadataObjectException.class,
        () -> policies.listPolicyInfosForMetadataObject(METALAKE, wrongColumn));
    // Normalizing reads does not enable policy association on unsupported object types.
    assertThrows(
        IllegalArgumentException.class,
        () ->
            policies.associatePoliciesForMetadataObject(
                METALAKE, request, new String[] {POLICY}, null));
  }

  @TestTemplate
  void testCatalogNamesDoNotLoadCatalogCapabilities() {
    MetadataObject object = MetadataObjects.parse("cat", MetadataObject.Type.CATALOG);
    policies.associatePoliciesForMetadataObject(METALAKE, object, new String[] {POLICY}, null);
    assertEquals(POLICY, policies.getPolicyForMetadataObject(METALAKE, object, POLICY).name());
    assertEquals(1, policies.listPolicyInfosForMetadataObject(METALAKE, object).length);
    verifyNoInteractions(catalogs);
  }

  @TestTemplate
  void testCaseSensitiveCatalogStillRejectsDifferentNames() {
    when(catalog.capability()).thenReturn(new Capability() {});
    MetadataObject upper = MetadataObjects.parse("cat.ANALYTICS.ORDERS", MetadataObject.Type.TABLE);
    assertThrows(
        NoSuchMetadataObjectException.class,
        () ->
            policies.associatePoliciesForMetadataObject(
                METALAKE, upper, new String[] {POLICY}, null));
    MetadataObject lower = MetadataObjects.parse("cat.analytics.orders", MetadataObject.Type.TABLE);
    assertEquals(0, policies.listPolicyInfosForMetadataObject(METALAKE, lower).length);
  }

  @TestTemplate
  void testNormalizationFailureLeavesRelationsUnchanged() {
    MetadataObject object =
        MetadataObjects.parse("cat.analytics.orders", MetadataObject.Type.TABLE);
    policies.associatePoliciesForMetadataObject(METALAKE, object, new String[] {POLICY}, null);
    Capability original = catalog.capability();
    when(catalog.capability()).thenThrow(new IllegalStateException("cannot load catalog rules"));
    assertThrows(
        RuntimeException.class,
        () ->
            policies.associatePoliciesForMetadataObject(
                METALAKE, object, null, new String[] {POLICY}));
    assertThrows(
        RuntimeException.class,
        () ->
            policies.associatePoliciesForMetadataObject(
                METALAKE, object, new String[] {POLICY}, null));
    Mockito.doReturn(original).when(catalog).capability();
    assertEquals(POLICY, policies.getPolicyForMetadataObject(METALAKE, object, POLICY).name());
  }

  private static Capability foldingCapability() {
    return new Capability() {
      @Override
      public CapabilityResult caseSensitiveOnName(Scope scope) {
        return scope == Scope.SCHEMA || scope == Scope.TABLE
            ? CapabilityResult.unsupported("case insensitive")
            : CapabilityResult.SUPPORTED;
      }
    };
  }
}
