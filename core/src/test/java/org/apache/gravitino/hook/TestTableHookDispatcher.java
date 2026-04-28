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
package org.apache.gravitino.hook;

import static org.apache.gravitino.Configs.CATALOG_CACHE_EVICTION_INTERVAL_MS;
import static org.apache.gravitino.Configs.DEFAULT_ENTITY_RELATIONAL_STORE;
import static org.apache.gravitino.Configs.ENTITY_RELATIONAL_JDBC_BACKEND_DRIVER;
import static org.apache.gravitino.Configs.ENTITY_RELATIONAL_JDBC_BACKEND_MAX_CONNECTIONS;
import static org.apache.gravitino.Configs.ENTITY_RELATIONAL_JDBC_BACKEND_URL;
import static org.apache.gravitino.Configs.ENTITY_RELATIONAL_JDBC_BACKEND_WAIT_MILLISECONDS;
import static org.apache.gravitino.Configs.ENTITY_RELATIONAL_STORE;
import static org.apache.gravitino.Configs.ENTITY_STORE;
import static org.apache.gravitino.Configs.RELATIONAL_ENTITY_STORE;
import static org.apache.gravitino.Configs.SERVICE_ADMINS;
import static org.apache.gravitino.Configs.STORE_DELETE_AFTER_TIME;
import static org.apache.gravitino.Configs.STORE_TRANSACTION_MAX_SKEW_TIME;
import static org.apache.gravitino.Configs.TREE_LOCK_CLEAN_INTERVAL;
import static org.apache.gravitino.Configs.TREE_LOCK_MAX_NODE_IN_MEMORY;
import static org.apache.gravitino.Configs.TREE_LOCK_MIN_NODE_IN_MEMORY;
import static org.apache.gravitino.Configs.VERSION_RETENTION_COUNT;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.util.Map;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.Config;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.TestColumn;
import org.apache.gravitino.authorization.AccessControlManager;
import org.apache.gravitino.authorization.Owner;
import org.apache.gravitino.authorization.OwnerDispatcher;
import org.apache.gravitino.catalog.CatalogManager;
import org.apache.gravitino.catalog.TableDispatcher;
import org.apache.gravitino.catalog.TestOperationDispatcher;
import org.apache.gravitino.catalog.TestTableOperationDispatcher;
import org.apache.gravitino.connector.BaseCatalog;
import org.apache.gravitino.connector.authorization.AuthorizationPlugin;
import org.apache.gravitino.connector.capability.Capability;
import org.apache.gravitino.connector.capability.CapabilityResult;
import org.apache.gravitino.lock.LockManager;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.TableChange;
import org.apache.gravitino.rel.expressions.NamedReference;
import org.apache.gravitino.rel.expressions.distributions.Distribution;
import org.apache.gravitino.rel.expressions.distributions.Distributions;
import org.apache.gravitino.rel.expressions.distributions.Strategy;
import org.apache.gravitino.rel.expressions.literals.Literals;
import org.apache.gravitino.rel.expressions.sorts.SortOrder;
import org.apache.gravitino.rel.expressions.sorts.SortOrders;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.expressions.transforms.Transforms;
import org.apache.gravitino.rel.indexes.Index;
import org.apache.gravitino.rel.indexes.Indexes;
import org.apache.gravitino.rel.partitions.Partitions;
import org.apache.gravitino.rel.partitions.RangePartition;
import org.apache.gravitino.rel.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

public class TestTableHookDispatcher extends TestOperationDispatcher {

  private static TableHookDispatcher tableHookDispatcher;
  private static SchemaHookDispatcher schemaHookDispatcher;
  private static AccessControlManager accessControlManager =
      Mockito.mock(AccessControlManager.class);
  private static AuthorizationPlugin authorizationPlugin;

  @BeforeAll
  public static void initialize() throws Exception {
    TestTableOperationDispatcher.initialize();

    tableHookDispatcher =
        new TableHookDispatcher(TestTableOperationDispatcher.getTableOperationDispatcher());
    schemaHookDispatcher =
        new SchemaHookDispatcher(TestTableOperationDispatcher.getSchemaOperationDispatcher());

    FieldUtils.writeField(
        GravitinoEnv.getInstance(), "accessControlDispatcher", accessControlManager, true);
    catalogManager = Mockito.mock(CatalogManager.class);
    FieldUtils.writeField(GravitinoEnv.getInstance(), "catalogManager", catalogManager, true);
    BaseCatalog catalog = Mockito.mock(BaseCatalog.class);
    Mockito.when(catalog.capability()).thenReturn(Capability.DEFAULT);
    CatalogManager.CatalogWrapper catalogWrapper =
        Mockito.mock(CatalogManager.CatalogWrapper.class);
    Mockito.when(catalogWrapper.catalog()).thenReturn(catalog);
    Mockito.when(catalogWrapper.capabilities()).thenReturn(Capability.DEFAULT);

    Mockito.when(catalogManager.loadCatalog(any())).thenReturn(catalog);
    Mockito.when(catalogManager.loadCatalogAndWrap(any())).thenReturn(catalogWrapper);
    authorizationPlugin = Mockito.mock(AuthorizationPlugin.class);
    Mockito.when(catalog.getAuthorizationPlugin()).thenReturn(authorizationPlugin);
  }

  @Test
  public void testDropAuthorizationPrivilege() {
    Namespace tableNs = Namespace.of(metalake, catalog, "schema1123");
    Map<String, String> props = ImmutableMap.of("k1", "v1", "k2", "v2");
    schemaHookDispatcher.createSchema(NameIdentifier.of(tableNs.levels()), "comment", props);

    NameIdentifier tableIdent = NameIdentifier.of(tableNs, "tableNAME");
    Column[] columns =
        new Column[] {
          TestColumn.builder()
              .withName("colNAME1")
              .withPosition(0)
              .withType(Types.StringType.get())
              .build(),
          TestColumn.builder()
              .withName("colNAME2")
              .withPosition(1)
              .withType(Types.StringType.get())
              .build()
        };
    RangePartition assignedPartition =
        Partitions.range(
            "partition_V1",
            Literals.stringLiteral("value1"),
            Literals.stringLiteral("value2"),
            null);
    Transform[] transforms =
        new Transform[] {
          Transforms.range(
              new String[] {columns[0].name()}, new RangePartition[] {assignedPartition})
        };
    Distribution distribution =
        Distributions.fields(Strategy.HASH, 5, new String[] {columns[0].name()});
    SortOrder[] sortOrders =
        new SortOrder[] {SortOrders.ascending(NamedReference.field(columns[0].name()))};
    Index[] indexes = new Index[] {Indexes.primary("index1", new String[][] {{columns[0].name()}})};
    tableHookDispatcher.createTable(
        tableIdent, columns, "comment", props, transforms, distribution, sortOrders, indexes);

    withMockedAuthorizationUtils(
        () -> {
          tableHookDispatcher.dropTable(tableIdent);
          Config config = Mockito.mock(Config.class);
          Mockito.when(config.get(SERVICE_ADMINS))
              .thenReturn(Lists.newArrayList("admin1", "admin2"));
          Mockito.when(config.get(ENTITY_STORE)).thenReturn(RELATIONAL_ENTITY_STORE);
          Mockito.when(config.get(ENTITY_RELATIONAL_STORE))
              .thenReturn(DEFAULT_ENTITY_RELATIONAL_STORE);
          Mockito.when(config.get(ENTITY_RELATIONAL_JDBC_BACKEND_URL))
              .thenReturn(
                  String.format("jdbc:h2:file:%s;DB_CLOSE_DELAY=-1;MODE=MYSQL", "/tmp/testdb"));
          Mockito.when(config.get(ENTITY_RELATIONAL_JDBC_BACKEND_DRIVER))
              .thenReturn("org.h2.Driver");
          Mockito.when(config.get(ENTITY_RELATIONAL_JDBC_BACKEND_MAX_CONNECTIONS)).thenReturn(100);
          Mockito.when(config.get(ENTITY_RELATIONAL_JDBC_BACKEND_WAIT_MILLISECONDS))
              .thenReturn(1000L);
          Mockito.when(config.get(STORE_TRANSACTION_MAX_SKEW_TIME)).thenReturn(1000L);
          Mockito.when(config.get(STORE_DELETE_AFTER_TIME)).thenReturn(20 * 60 * 1000L);
          Mockito.when(config.get(VERSION_RETENTION_COUNT)).thenReturn(1L);
          Mockito.when(config.get(CATALOG_CACHE_EVICTION_INTERVAL_MS)).thenReturn(1000L);
          Mockito.doReturn(100000L).when(config).get(TREE_LOCK_MAX_NODE_IN_MEMORY);
          Mockito.doReturn(1000L).when(config).get(TREE_LOCK_MIN_NODE_IN_MEMORY);
          Mockito.doReturn(36000L).when(config).get(TREE_LOCK_CLEAN_INTERVAL);
          try {
            FieldUtils.writeField(
                GravitinoEnv.getInstance(), "lockManager", new LockManager(config), true);
          } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
          }
          schemaHookDispatcher.dropSchema(NameIdentifier.of(tableNs.levels()), true);
        });
  }

  @Test
  public void testCreateTableSetsOwnerWithNormalizedIdentifier() throws Exception {
    // Self-contained: use a fresh hook with a directly-mocked TableDispatcher and a case-
    // insensitive catalog so we can verify the helper passes a normalized ident to setOwner.
    CatalogManager savedCatalogManager = GravitinoEnv.getInstance().catalogManager();
    OwnerDispatcher savedOwnerDispatcher = GravitinoEnv.getInstance().ownerDispatcher();

    CatalogManager mockCatalogManager = Mockito.mock(CatalogManager.class);
    CatalogManager.CatalogWrapper mockWrapper = Mockito.mock(CatalogManager.CatalogWrapper.class);
    Mockito.when(mockWrapper.capabilities()).thenReturn(new CaseInsensitiveCapability());
    Mockito.when(mockCatalogManager.loadCatalogAndWrap(any())).thenReturn(mockWrapper);

    OwnerDispatcher mockOwnerDispatcher = Mockito.mock(OwnerDispatcher.class);
    TableDispatcher mockTableDispatcher = Mockito.mock(TableDispatcher.class);
    Mockito.when(
            mockTableDispatcher.createTable(any(), any(), any(), any(), any(), any(), any(), any()))
        .thenReturn(Mockito.mock(Table.class));

    FieldUtils.writeField(GravitinoEnv.getInstance(), "catalogManager", mockCatalogManager, true);
    FieldUtils.writeField(GravitinoEnv.getInstance(), "ownerDispatcher", mockOwnerDispatcher, true);

    try {
      TableHookDispatcher localHook = new TableHookDispatcher(mockTableDispatcher);
      NameIdentifier ident = NameIdentifier.of(metalake, catalog, "SCHEMA_NORM", "MY_TABLE");
      localHook.createTable(
          ident,
          new Column[0],
          "comment",
          ImmutableMap.of(),
          new Transform[0],
          Distributions.NONE,
          new SortOrder[0],
          new Index[0]);

      ArgumentCaptor<MetadataObject> captor = ArgumentCaptor.forClass(MetadataObject.class);
      Mockito.verify(mockOwnerDispatcher)
          .setOwner(eq(metalake), captor.capture(), any(), eq(Owner.Type.USER));
      Assertions.assertEquals(
          "my_table",
          captor.getValue().name(),
          "Table name passed to setOwner must be lowercased by Capability.Scope.TABLE normalization");
      Assertions.assertEquals(
          catalog + ".schema_norm",
          captor.getValue().parent(),
          "Table parent (catalog.schema) must have its schema component lowercased by"
              + " Capability.Scope.TABLE namespace normalization");
    } finally {
      FieldUtils.writeField(
          GravitinoEnv.getInstance(), "catalogManager", savedCatalogManager, true);
      FieldUtils.writeField(
          GravitinoEnv.getInstance(), "ownerDispatcher", savedOwnerDispatcher, true);
    }
  }

  @Test
  public void testCreateTableSucceedsEvenIfSetOwnerFails() throws IllegalAccessException {
    OwnerDispatcher mockOwnerDispatcher = Mockito.mock(OwnerDispatcher.class);
    Mockito.doThrow(new RuntimeException("Set owner failed"))
        .when(mockOwnerDispatcher)
        .setOwner(any(), any(), any(), any());
    FieldUtils.writeField(GravitinoEnv.getInstance(), "ownerDispatcher", mockOwnerDispatcher, true);

    try {
      Namespace tableNs = Namespace.of(metalake, catalog, "schema_owner_fail");
      Map<String, String> props = ImmutableMap.of("k1", "v1", "k2", "v2");
      schemaHookDispatcher.createSchema(NameIdentifier.of(tableNs.levels()), "comment", props);

      NameIdentifier tableIdent = NameIdentifier.of(tableNs, "table_owner_fail");
      Column[] columns =
          new Column[] {
            TestColumn.builder()
                .withName("col1")
                .withPosition(0)
                .withType(Types.StringType.get())
                .build()
          };
      tableHookDispatcher.createTable(
          tableIdent,
          columns,
          "comment",
          props,
          new Transform[0],
          Distributions.NONE,
          new SortOrder[0],
          new Index[0]);
    } finally {
      FieldUtils.writeField(GravitinoEnv.getInstance(), "ownerDispatcher", null, true);
    }
  }

  @Test
  public void testRenameAuthorizationPrivilege() {
    Namespace tableNs = Namespace.of(metalake, catalog, "schema1124");
    Map<String, String> props = ImmutableMap.of("k1", "v1", "k2", "v2");
    schemaHookDispatcher.createSchema(NameIdentifier.of(tableNs.levels()), "comment", props);

    NameIdentifier tableIdent = NameIdentifier.of(tableNs, "tableNAME");
    Column[] columns =
        new Column[] {
          TestColumn.builder()
              .withName("colNAME1")
              .withPosition(0)
              .withType(Types.StringType.get())
              .build(),
          TestColumn.builder()
              .withName("colNAME2")
              .withPosition(1)
              .withType(Types.StringType.get())
              .build()
        };
    RangePartition assignedPartition =
        Partitions.range(
            "partition_V1",
            Literals.stringLiteral("value1"),
            Literals.stringLiteral("value2"),
            null);
    Transform[] transforms =
        new Transform[] {
          Transforms.range(
              new String[] {columns[0].name()}, new RangePartition[] {assignedPartition})
        };
    Distribution distribution =
        Distributions.fields(Strategy.HASH, 5, new String[] {columns[0].name()});
    SortOrder[] sortOrders =
        new SortOrder[] {SortOrders.ascending(NamedReference.field(columns[0].name()))};
    Index[] indexes = new Index[] {Indexes.primary("index1", new String[][] {{columns[0].name()}})};
    tableHookDispatcher.createTable(
        tableIdent, columns, "comment", props, transforms, distribution, sortOrders, indexes);

    Mockito.reset(authorizationPlugin);
    TableChange setChange = TableChange.setProperty("k1", "v1");
    tableHookDispatcher.alterTable(tableIdent, setChange);
    Mockito.verify(authorizationPlugin, Mockito.never()).onMetadataUpdated(any());

    Mockito.reset(authorizationPlugin);
    TableChange renameChange = TableChange.rename("newName");
    tableHookDispatcher.alterTable(tableIdent, renameChange);
    Mockito.verify(authorizationPlugin).onMetadataUpdated(any());
  }

  private static class CaseInsensitiveCapability implements Capability {
    @Override
    public CapabilityResult caseSensitiveOnName(Scope scope) {
      return CapabilityResult.unsupported("case-insensitive");
    }
  }
}
