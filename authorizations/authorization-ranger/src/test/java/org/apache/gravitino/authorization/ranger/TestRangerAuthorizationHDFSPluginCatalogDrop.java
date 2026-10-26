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
package org.apache.gravitino.authorization.ranger;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;

import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.Schema;
import org.apache.gravitino.authorization.AccessControlDispatcher;
import org.apache.gravitino.authorization.Privilege;
import org.apache.gravitino.authorization.Privileges;
import org.apache.gravitino.authorization.Role;
import org.apache.gravitino.authorization.SecurableObject;
import org.apache.gravitino.authorization.SecurableObjects;
import org.apache.gravitino.authorization.ranger.reference.RangerDefines.PolicyResource;
import org.apache.gravitino.catalog.CatalogDispatcher;
import org.apache.gravitino.catalog.CatalogManager;
import org.apache.gravitino.catalog.FilesetDispatcher;
import org.apache.gravitino.catalog.SchemaDispatcher;
import org.apache.gravitino.catalog.TableDispatcher;
import org.apache.gravitino.connector.BaseCatalog;
import org.apache.gravitino.exceptions.AuthorizationPluginException;
import org.apache.gravitino.exceptions.NoSuchCatalogException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.file.Fileset;
import org.apache.gravitino.hook.CatalogHookDispatcher;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.utils.ThrowableFunction;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.util.SearchFilter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

/** Drops a Hive catalog through the hook dispatcher and checks the Ranger HDFS policies left. */
public class TestRangerAuthorizationHDFSPluginCatalogDrop {

  private static final Privilege READ = Privileges.ReadFileset.allow();

  private static final String METALAKE = "metalake";
  private static final NameIdentifier CATALOG = NameIdentifier.of(METALAKE, "hive_catalog");
  private static final NameIdentifier OTHER_CATALOG = NameIdentifier.of(METALAKE, "other_catalog");
  private static final NameIdentifier FILESET_CATALOG =
      NameIdentifier.of(METALAKE, "fileset_catalog");
  private static final String WAREHOUSE = "hdfs://ns/user/hive/warehouse";
  private static final String WAREHOUSE_PATH = "/user/hive/warehouse";
  private static final String CATALOG_ID = "1";

  private final List<RangerPolicy> policies = new ArrayList<>();
  private RangerClientExtension rangerClient;
  private CatalogDispatcher innerDispatcher;
  private CatalogDispatcher internalCatalogDispatcher;
  private SchemaDispatcher schemaDispatcher;
  private TableDispatcher tableDispatcher;
  private FilesetDispatcher filesetDispatcher;
  private AccessControlDispatcher accessControlDispatcher;
  private MockedStatic<GravitinoEnv> envStatic;

  @BeforeEach
  public void setUp() throws Exception {
    policies.clear();
    // The constructor connects to Ranger, so only the tested methods run for real.
    RangerAuthorizationHDFSPlugin plugin =
        Mockito.mock(RangerAuthorizationHDFSPlugin.class, Mockito.CALLS_REAL_METHODS);
    rangerClient = Mockito.mock(RangerClientExtension.class);
    // An in-memory Ranger HDFS service: a search returns the policies on the searched path.
    Mockito.when(rangerClient.findPolicies(any()))
        .thenAnswer(invocation -> findPolicies(invocation.getArgument(0)));
    Mockito.when(rangerClient.updatePolicy(anyLong(), any()))
        .thenAnswer(invocation -> invocation.getArgument(1));
    plugin.setRangerClient(rangerClient);
    plugin.setRangerHelper(
        new RangerHelper(
            rangerClient,
            "admin",
            "hdfsService",
            plugin.ownerMappingRule(),
            plugin.policyResourceDefinesRule()));
    FieldUtils.writeField(plugin, "metalake", METALAKE, true);
    FieldUtils.writeField(plugin, "catalogId", CATALOG_ID, true);

    BaseCatalog<?> catalog = Mockito.mock(BaseCatalog.class);
    Mockito.when(catalog.name()).thenReturn(CATALOG.name());
    Mockito.when(catalog.getAuthorizationPlugin()).thenReturn(plugin);
    CatalogManager catalogManager = Mockito.mock(CatalogManager.class);
    Mockito.doAnswer(
            invocation -> {
              ThrowableFunction<BaseCatalog, Object> operation = invocation.getArgument(1);
              return operation.apply(catalog);
            })
        .when(catalogManager)
        .doWithCatalog(Mockito.eq(CATALOG), Mockito.any());

    internalCatalogDispatcher = Mockito.mock(CatalogDispatcher.class);
    Catalog hiveCatalog = Mockito.mock(Catalog.class);
    Mockito.when(hiveCatalog.provider()).thenReturn("hive");
    Mockito.when(hiveCatalog.type()).thenReturn(Catalog.Type.RELATIONAL);
    Mockito.when(internalCatalogDispatcher.loadCatalog(any())).thenReturn(hiveCatalog);
    givenCatalogs(catalogInfo(CATALOG, "hive"));
    Catalog filesetCatalog = Mockito.mock(Catalog.class);
    Mockito.when(filesetCatalog.provider()).thenReturn("fileset");
    Mockito.when(filesetCatalog.type()).thenReturn(Catalog.Type.FILESET);
    Mockito.when(internalCatalogDispatcher.loadCatalog(FILESET_CATALOG)).thenReturn(filesetCatalog);
    schemaDispatcher = Mockito.mock(SchemaDispatcher.class);
    givenDefaultSchemaLocation(CATALOG, WAREHOUSE);
    // By default the catalog has only its default schema, which has no tables.
    tableDispatcher = Mockito.mock(TableDispatcher.class);
    Mockito.when(tableDispatcher.listTables(any())).thenReturn(new NameIdentifier[0]);
    givenSchemas("default");
    filesetDispatcher = Mockito.mock(FilesetDispatcher.class);
    accessControlDispatcher = Mockito.mock(AccessControlDispatcher.class);

    GravitinoEnv env = Mockito.mock(GravitinoEnv.class);
    Mockito.when(env.internalAccessControlDispatcher()).thenReturn(accessControlDispatcher);
    Mockito.when(env.internalFilesetDispatcher()).thenReturn(filesetDispatcher);
    Mockito.when(env.internalCatalogDispatcher()).thenReturn(internalCatalogDispatcher);
    Mockito.when(env.internalSchemaDispatcher()).thenReturn(schemaDispatcher);
    Mockito.when(env.internalTableDispatcher()).thenReturn(tableDispatcher);
    Mockito.when(env.catalogManager()).thenReturn(catalogManager);
    envStatic = Mockito.mockStatic(GravitinoEnv.class);
    envStatic.when(GravitinoEnv::getInstance).thenReturn(env);

    innerDispatcher = Mockito.mock(CatalogDispatcher.class);
    Mockito.when(innerDispatcher.catalogExists(CATALOG)).thenReturn(true);
    Mockito.when(innerDispatcher.dropCatalog(CATALOG, true)).thenReturn(true);
  }

  @AfterEach
  public void tearDown() {
    envStatic.close();
  }

  @Test
  public void testDropCatalogRemovesGravitinoItemsOfWarehousePolicy() throws Exception {
    RangerPolicy policy = addPolicy(1, "/user/hive/warehouse", "GRAVITINO_role", "external_role");

    Assertions.assertTrue(new CatalogHookDispatcher(innerDispatcher).dropCatalog(CATALOG, true));

    // Only the Gravitino-managed item goes; the item Ranger admins added stays.
    Mockito.verify(rangerClient).updatePolicy(Mockito.eq(1L), Mockito.any());
    Assertions.assertEquals(List.of("external_role"), rolesOf(policy));
    Mockito.verify(innerDispatcher).dropCatalog(CATALOG, true);
  }

  @Test
  public void testDropCatalogDeletesPolicyWithOnlyGravitinoItems() throws Exception {
    addPolicy(1, "/user/hive/warehouse", "GRAVITINO_role");

    Assertions.assertTrue(new CatalogHookDispatcher(innerDispatcher).dropCatalog(CATALOG, true));

    Mockito.verify(rangerClient).deletePolicy(1L);
    Mockito.verify(innerDispatcher).dropCatalog(CATALOG, true);
  }

  @Test
  public void testDropCatalogRefusedWhenLocationLookupFails() throws Exception {
    addPolicy(1, "/user/hive/warehouse", "GRAVITINO_role");
    Mockito.when(
            schemaDispatcher.loadSchema(NameIdentifier.of(METALAKE, CATALOG.name(), "default")))
        .thenThrow(new NoSuchSchemaException("metastore unreachable"));

    AuthorizationPluginException e =
        Assertions.assertThrows(
            AuthorizationPluginException.class,
            () -> new CatalogHookDispatcher(innerDispatcher).dropCatalog(CATALOG, true));

    Assertions.assertTrue(e.getMessage().contains(CATALOG.toString()), e.getMessage());
    Mockito.verify(innerDispatcher, Mockito.never())
        .dropCatalog(Mockito.any(), Mockito.anyBoolean());
    Mockito.verify(rangerClient, Mockito.never()).deletePolicy(anyLong());
    Mockito.verify(rangerClient, Mockito.never()).updatePolicy(anyLong(), any());
  }

  @Test
  public void testDropCatalogKeepsOwnersOfPathSharedWithOtherCatalog() throws Exception {
    givenRole("catalog_role", catalogObject(CATALOG));
    RangerPolicy policy = addPolicy(1, WAREHOUSE_PATH, "GRAVITINO_catalog_role");
    addOwnerItem(policy, "alice");
    givenOtherCatalogWithLocation(WAREHOUSE + "/");

    Assertions.assertTrue(new CatalogHookDispatcher(innerDispatcher).dropCatalog(CATALOG, true));

    // The other catalog's owners live on the same policy, so only the dropped catalog's role goes.
    Assertions.assertEquals(List.of(RangerHelper.GRAVITINO_OWNER_ROLE), rolesOf(policy));
    Assertions.assertEquals(List.of("alice"), usersOf(policy));
    Mockito.verify(rangerClient, Mockito.never()).deletePolicy(anyLong());
    Mockito.verify(innerDispatcher).dropCatalog(CATALOG, true);
  }

  @Test
  public void testDropCatalogRemovesPolicyWhenOtherCatalogUsesAnotherPath() throws Exception {
    addPolicy(1, "/user/hive/warehouse", "GRAVITINO_role");
    givenOtherCatalogWithLocation("hdfs://ns/other/warehouse");

    Assertions.assertTrue(new CatalogHookDispatcher(innerDispatcher).dropCatalog(CATALOG, true));

    Mockito.verify(rangerClient).deletePolicy(1L);
  }

  @Test
  public void testDropCatalogRefusedWhenOtherCatalogLocationLookupFails() throws Exception {
    addPolicy(1, "/user/hive/warehouse", "GRAVITINO_role");
    givenOtherCatalogWithLocation(WAREHOUSE);
    Mockito.when(
            schemaDispatcher.loadSchema(
                NameIdentifier.of(METALAKE, OTHER_CATALOG.name(), "default")))
        .thenThrow(new NoSuchSchemaException("metastore unreachable"));

    // Without the other catalog's locations, removing the policy could revoke its grants.
    AuthorizationPluginException e =
        Assertions.assertThrows(
            AuthorizationPluginException.class,
            () -> new CatalogHookDispatcher(innerDispatcher).dropCatalog(CATALOG, true));

    Assertions.assertTrue(e.getMessage().contains(CATALOG.toString()), e.getMessage());

    Mockito.verify(innerDispatcher, Mockito.never())
        .dropCatalog(Mockito.any(), Mockito.anyBoolean());
    Mockito.verify(rangerClient, Mockito.never()).deletePolicy(anyLong());
  }

  @Test
  public void testDropCatalogIgnoresNonHiveCatalogs() throws Exception {
    addPolicy(1, "/user/hive/warehouse", "GRAVITINO_role");
    givenCatalogs(catalogInfo(CATALOG, "hive"), catalogInfo(OTHER_CATALOG, "lakehouse-iceberg"));

    Assertions.assertTrue(new CatalogHookDispatcher(innerDispatcher).dropCatalog(CATALOG, true));

    // A non-Hive catalog has no catalog location, so it is not loaded to check for sharing.
    Mockito.verify(internalCatalogDispatcher, Mockito.never()).loadCatalog(OTHER_CATALOG);
    Mockito.verify(rangerClient).deletePolicy(1L);
  }

  @Test
  public void testDropCatalogWithoutDefaultSchemaLocationSucceeds() throws Exception {
    // A default schema without a location is "no paths", not a failed lookup.
    Schema schema = Mockito.mock(Schema.class);
    Mockito.when(schema.properties()).thenReturn(ImmutableMap.of());
    Mockito.when(
            schemaDispatcher.loadSchema(NameIdentifier.of(METALAKE, CATALOG.name(), "default")))
        .thenReturn(schema);

    Assertions.assertTrue(new CatalogHookDispatcher(innerDispatcher).dropCatalog(CATALOG, true));

    Mockito.verify(rangerClient, Mockito.never()).deletePolicy(anyLong());
    Mockito.verify(innerDispatcher).dropCatalog(CATALOG, true);
  }

  @Test
  public void testForceDropCatalogRemovesGravitinoItemsOfSchemaPolicy() throws Exception {
    addPolicy(1, "/user/hive/warehouse", "GRAVITINO_role");
    RangerPolicy schemaPolicy =
        addPolicy(2, "/user/hive/warehouse/db.db", "GRAVITINO_role", "external_role");
    givenSchemas("default", "db");
    givenSchemaLocation("db", WAREHOUSE + "/db.db");

    Assertions.assertTrue(new CatalogHookDispatcher(innerDispatcher).dropCatalog(CATALOG, true));

    // Only the Gravitino-managed item goes; the item Ranger admins added stays.
    Mockito.verify(rangerClient).updatePolicy(Mockito.eq(2L), Mockito.any());
    Assertions.assertEquals(List.of("external_role"), rolesOf(schemaPolicy));
    Mockito.verify(innerDispatcher).dropCatalog(CATALOG, true);
  }

  @Test
  public void testForceDropCatalogRemovesTablePolicy() throws Exception {
    addPolicy(1, "/user/hive/warehouse", "GRAVITINO_role");
    addPolicy(2, "/user/hive/warehouse/db.db", "GRAVITINO_role");
    addPolicy(3, "/user/hive/warehouse/db.db/t", "GRAVITINO_role");
    givenSchemas("default", "db");
    givenSchemaLocation("db", WAREHOUSE + "/db.db");
    givenTable("db", "t", WAREHOUSE + "/db.db/t");

    Assertions.assertTrue(new CatalogHookDispatcher(innerDispatcher).dropCatalog(CATALOG, true));

    Mockito.verify(rangerClient).deletePolicy(1L);
    Mockito.verify(rangerClient).deletePolicy(2L);
    Mockito.verify(rangerClient).deletePolicy(3L);
    Mockito.verify(innerDispatcher).dropCatalog(CATALOG, true);
  }

  @Test
  public void testForceDropCatalogRemovesPolicyOfSchemaOutsideWarehouse() throws Exception {
    addPolicy(1, "/user/hive/warehouse", "GRAVITINO_role");
    addPolicy(2, "/external/db", "GRAVITINO_role");
    givenSchemas("default", "db");
    givenSchemaLocation("db", "hdfs://ns/external/db");

    Assertions.assertTrue(new CatalogHookDispatcher(innerDispatcher).dropCatalog(CATALOG, true));

    // The schema's policy is found by its own location, not by being under the warehouse.
    Mockito.verify(rangerClient).deletePolicy(2L);
  }

  @Test
  public void testForceDropCatalogRefusedWhenSchemaLocationLookupFails() throws Exception {
    addPolicy(1, "/user/hive/warehouse", "GRAVITINO_role");
    addPolicy(2, "/user/hive/warehouse/db.db", "GRAVITINO_role");
    givenSchemas("default", "db");
    Mockito.when(schemaDispatcher.loadSchema(NameIdentifier.of(METALAKE, CATALOG.name(), "db")))
        .thenThrow(new NoSuchSchemaException("metastore unreachable"));

    AuthorizationPluginException e =
        Assertions.assertThrows(
            AuthorizationPluginException.class,
            () -> new CatalogHookDispatcher(innerDispatcher).dropCatalog(CATALOG, true));

    // Every lookup runs before any change, so the catalog policy is untouched as well.
    Assertions.assertTrue(e.getMessage().contains("db"), e.getMessage());
    assertRefusedWithoutPolicyChanges();
  }

  @Test
  public void testForceDropCatalogRefusedWhenSchemaListingFails() throws Exception {
    addPolicy(1, "/user/hive/warehouse", "GRAVITINO_role");
    Mockito.when(schemaDispatcher.listSchemas(Namespace.of(METALAKE, CATALOG.name())))
        .thenThrow(new NoSuchCatalogException("metastore unreachable"));

    AuthorizationPluginException e =
        Assertions.assertThrows(
            AuthorizationPluginException.class,
            () -> new CatalogHookDispatcher(innerDispatcher).dropCatalog(CATALOG, true));

    Assertions.assertTrue(e.getMessage().contains(CATALOG.toString()), e.getMessage());
    assertRefusedWithoutPolicyChanges();
  }

  @Test
  public void testForceDropCatalogRefusedWhenTableListingFails() throws Exception {
    addPolicy(1, "/user/hive/warehouse", "GRAVITINO_role");
    givenSchemas("default", "db");
    givenSchemaLocation("db", WAREHOUSE + "/db.db");
    Mockito.when(tableDispatcher.listTables(Namespace.of(METALAKE, CATALOG.name(), "db")))
        .thenThrow(new NoSuchSchemaException("metastore unreachable"));

    AuthorizationPluginException e =
        Assertions.assertThrows(
            AuthorizationPluginException.class,
            () -> new CatalogHookDispatcher(innerDispatcher).dropCatalog(CATALOG, true));

    Assertions.assertTrue(e.getMessage().contains(CATALOG.toString()), e.getMessage());
    assertRefusedWithoutPolicyChanges();
  }

  @Test
  public void testForceDropCatalogKeepsSchemaPoliciesOfSharedWarehouse() throws Exception {
    addPolicy(1, "/user/hive/warehouse", "GRAVITINO_role");
    RangerPolicy schemaPolicy = addPolicy(2, "/user/hive/warehouse/db.db", "GRAVITINO_role");
    givenOtherCatalogWithLocation(WAREHOUSE);
    givenSchemas("default", "db");
    givenSchemaLocation("db", WAREHOUSE + "/db.db");

    Assertions.assertTrue(new CatalogHookDispatcher(innerDispatcher).dropCatalog(CATALOG, true));

    // Catalogs on one warehouse share its schemas, so their policies stay with the other catalog.
    Mockito.verify(rangerClient, Mockito.never()).deletePolicy(2L);
    Mockito.verify(rangerClient, Mockito.never()).updatePolicy(Mockito.eq(2L), any());
    Assertions.assertEquals(List.of("GRAVITINO_role"), rolesOf(schemaPolicy));
    Mockito.verify(schemaDispatcher, Mockito.never()).listSchemas(any());
  }

  private void assertRefusedWithoutPolicyChanges() throws Exception {
    Mockito.verify(innerDispatcher, Mockito.never())
        .dropCatalog(Mockito.any(), Mockito.anyBoolean());
    Mockito.verify(rangerClient, Mockito.never()).deletePolicy(anyLong());
    Mockito.verify(rangerClient, Mockito.never()).updatePolicy(anyLong(), any());
  }

  private void givenSchemas(String... schemas) {
    NameIdentifier[] idents = new NameIdentifier[schemas.length];
    for (int i = 0; i < schemas.length; i++) {
      idents[i] = NameIdentifier.of(METALAKE, CATALOG.name(), schemas[i]);
    }
    Mockito.when(schemaDispatcher.listSchemas(Namespace.of(METALAKE, CATALOG.name())))
        .thenReturn(idents);
  }

  private void givenSchemaLocation(String schemaName, String location) {
    Schema schema = Mockito.mock(Schema.class);
    Mockito.when(schema.properties()).thenReturn(ImmutableMap.of("location", location));
    Mockito.when(
            schemaDispatcher.loadSchema(NameIdentifier.of(METALAKE, CATALOG.name(), schemaName)))
        .thenReturn(schema);
  }

  private void givenTable(String schemaName, String tableName, String location) {
    NameIdentifier ident = NameIdentifier.of(METALAKE, CATALOG.name(), schemaName, tableName);
    Mockito.when(tableDispatcher.listTables(Namespace.of(METALAKE, CATALOG.name(), schemaName)))
        .thenReturn(new NameIdentifier[] {ident});
    Table table = Mockito.mock(Table.class);
    Mockito.when(table.properties()).thenReturn(ImmutableMap.of("location", location));
    Mockito.when(tableDispatcher.loadTable(ident)).thenReturn(table);
  }

  @Test
  public void testDropCatalogKeepsGrantOfOtherCatalogSchemaAtWarehouse() throws Exception {
    NameIdentifier schema = NameIdentifier.of(METALAKE, FILESET_CATALOG.name(), "s");
    givenSchemaLocation(schema, WAREHOUSE);
    givenRole("catalog_role", catalogObject(CATALOG));
    givenRole("schema_role", schemaObject(schema));
    RangerPolicy policy =
        addPolicy(
            1, WAREHOUSE_PATH, "GRAVITINO_catalog_role", "GRAVITINO_schema_role", "external_role");

    Assertions.assertTrue(new CatalogHookDispatcher(innerDispatcher).dropCatalog(CATALOG, true));

    Assertions.assertEquals(List.of("GRAVITINO_schema_role", "external_role"), rolesOf(policy));
    Mockito.verify(rangerClient).updatePolicy(Mockito.eq(1L), Mockito.any());
    Mockito.verify(innerDispatcher).dropCatalog(CATALOG, true);
  }

  @Test
  public void testDropCatalogKeepsGrantOfOtherCatalogFilesetAtWarehouse() throws Exception {
    NameIdentifier fileset = NameIdentifier.of(METALAKE, FILESET_CATALOG.name(), "s", "f");
    givenFilesetLocation(fileset, WAREHOUSE + "/");
    givenRole("catalog_role", catalogObject(CATALOG));
    givenRole("fileset_role", filesetObject(fileset));
    RangerPolicy policy =
        addPolicy(1, WAREHOUSE_PATH, "GRAVITINO_catalog_role", "GRAVITINO_fileset_role");

    Assertions.assertTrue(new CatalogHookDispatcher(innerDispatcher).dropCatalog(CATALOG, true));

    Assertions.assertEquals(List.of("GRAVITINO_fileset_role"), rolesOf(policy));
    Mockito.verify(rangerClient, Mockito.never()).deletePolicy(anyLong());
  }

  @Test
  public void testDropCatalogKeepsOwnersWhenOtherCatalogSchemaIsGrantedAtWarehouse()
      throws Exception {
    NameIdentifier schema = NameIdentifier.of(METALAKE, FILESET_CATALOG.name(), "s");
    givenSchemaLocation(schema, WAREHOUSE);
    givenRole("schema_role", catalogObject(CATALOG), schemaObject(schema));
    RangerPolicy policy = addPolicy(1, WAREHOUSE_PATH, "GRAVITINO_schema_role");
    addOwnerItem(policy, "alice");

    Assertions.assertTrue(new CatalogHookDispatcher(innerDispatcher).dropCatalog(CATALOG, true));

    // A role that grants on both catalogs keeps its item, and the schema's owner stays.
    Assertions.assertEquals(
        List.of("GRAVITINO_schema_role", RangerHelper.GRAVITINO_OWNER_ROLE), rolesOf(policy));
    Assertions.assertEquals(List.of("alice"), usersOf(policy));
  }

  @Test
  public void testDropCatalogRemovesGrantOfOtherCatalogSchemaAtAnotherPath() throws Exception {
    NameIdentifier schema = NameIdentifier.of(METALAKE, FILESET_CATALOG.name(), "s");
    givenSchemaLocation(schema, "hdfs://ns/data/s");
    givenRole("schema_role", schemaObject(schema));
    RangerPolicy policy = addPolicy(1, WAREHOUSE_PATH, "GRAVITINO_schema_role");
    addOwnerItem(policy, "alice");

    Assertions.assertTrue(new CatalogHookDispatcher(innerDispatcher).dropCatalog(CATALOG, true));

    Mockito.verify(rangerClient).deletePolicy(1L);
  }

  @Test
  public void testDropCatalogRefusedWhenRoleObjectLocationLookupFails() throws Exception {
    NameIdentifier schema = NameIdentifier.of(METALAKE, FILESET_CATALOG.name(), "s");
    Mockito.when(schemaDispatcher.loadSchema(schema))
        .thenThrow(new NoSuchSchemaException("metastore unreachable"));
    givenRole("schema_role", schemaObject(schema));
    RangerPolicy policy = addPolicy(1, WAREHOUSE_PATH, "GRAVITINO_schema_role");

    AuthorizationPluginException e =
        Assertions.assertThrows(
            AuthorizationPluginException.class,
            () -> new CatalogHookDispatcher(innerDispatcher).dropCatalog(CATALOG, true));

    Assertions.assertTrue(e.getMessage().contains("schema_role"), e.getMessage());
    Assertions.assertEquals(List.of("GRAVITINO_schema_role"), rolesOf(policy));
    Mockito.verify(innerDispatcher, Mockito.never())
        .dropCatalog(Mockito.any(), Mockito.anyBoolean());
    Mockito.verify(rangerClient, Mockito.never()).deletePolicy(anyLong());
    Mockito.verify(rangerClient, Mockito.never()).updatePolicy(anyLong(), any());
  }

  @Test
  public void testDropCatalogKeepsOtherCatalogOwnerRoleAndRemovesItsOwn() throws Exception {
    String ownOwnerRole = RangerHelper.generateCatalogOwnerRoleName(CATALOG_ID);
    String otherOwnerRole = RangerHelper.generateCatalogOwnerRoleName("2");
    RangerPolicy policy = addPolicy(1, WAREHOUSE_PATH, ownOwnerRole, otherOwnerRole);

    Assertions.assertTrue(new CatalogHookDispatcher(innerDispatcher).dropCatalog(CATALOG, true));

    Assertions.assertEquals(List.of(otherOwnerRole), rolesOf(policy));
  }

  @Test
  public void testDropCatalogKeepsMetalakeGrantOnlyWhenPathIsShared() throws Exception {
    givenRole("metalake_role", SecurableObjects.ofMetalake(METALAKE, List.of(READ)));
    RangerPolicy policy = addPolicy(1, WAREHOUSE_PATH, "GRAVITINO_metalake_role");
    givenOtherCatalogWithLocation(WAREHOUSE);

    Assertions.assertTrue(new CatalogHookDispatcher(innerDispatcher).dropCatalog(CATALOG, true));

    Assertions.assertEquals(List.of("GRAVITINO_metalake_role"), rolesOf(policy));
    Mockito.verify(rangerClient, Mockito.never()).deletePolicy(anyLong());
  }

  @Test
  public void testDropCatalogRemovesMetalakeGrantWhenPathIsNotShared() throws Exception {
    givenRole("metalake_role", SecurableObjects.ofMetalake(METALAKE, List.of(READ)));
    addPolicy(1, WAREHOUSE_PATH, "GRAVITINO_metalake_role");

    Assertions.assertTrue(new CatalogHookDispatcher(innerDispatcher).dropCatalog(CATALOG, true));

    Mockito.verify(rangerClient).deletePolicy(1L);
  }

  private void givenOtherCatalogWithLocation(String location) {
    givenCatalogs(catalogInfo(CATALOG, "hive"), catalogInfo(OTHER_CATALOG, "hive"));
    givenDefaultSchemaLocation(OTHER_CATALOG, location);
  }

  private void givenRole(String name, SecurableObject... securableObjects) {
    Role role = Mockito.mock(Role.class);
    Mockito.when(role.name()).thenReturn(name);
    Mockito.when(role.securableObjects()).thenReturn(List.of(securableObjects));
    Mockito.when(accessControlDispatcher.getRole(METALAKE, name)).thenReturn(role);
  }

  private static SecurableObject catalogObject(NameIdentifier catalog) {
    return SecurableObjects.parse(catalog.name(), MetadataObject.Type.CATALOG, List.of(READ));
  }

  private static SecurableObject schemaObject(NameIdentifier schema) {
    return SecurableObjects.parse(
        schema.namespace().level(1) + "." + schema.name(),
        MetadataObject.Type.SCHEMA,
        List.of(READ));
  }

  private static SecurableObject filesetObject(NameIdentifier fileset) {
    return SecurableObjects.parse(
        String.join(
            ".", fileset.namespace().level(1), fileset.namespace().level(2), fileset.name()),
        MetadataObject.Type.FILESET,
        List.of(READ));
  }

  private void givenSchemaLocation(NameIdentifier schema, String location) {
    Schema schemaObj = Mockito.mock(Schema.class);
    Mockito.when(schemaObj.name()).thenReturn(schema.name());
    Mockito.when(schemaObj.properties()).thenReturn(ImmutableMap.of("location", location));
    Mockito.when(schemaDispatcher.loadSchema(schema)).thenReturn(schemaObj);
  }

  private void givenFilesetLocation(NameIdentifier fileset, String location) {
    Fileset filesetObj = Mockito.mock(Fileset.class);
    Mockito.when(filesetObj.storageLocation()).thenReturn(location);
    Mockito.when(filesetDispatcher.loadFileset(fileset)).thenReturn(filesetObj);
  }

  private static void addOwnerItem(RangerPolicy policy, String owner) {
    RangerPolicy.RangerPolicyItem item = new RangerPolicy.RangerPolicyItem();
    item.setUsers(new ArrayList<>(List.of(owner)));
    item.setRoles(new ArrayList<>(List.of(RangerHelper.GRAVITINO_OWNER_ROLE)));
    item.setAccesses(List.of(new RangerPolicy.RangerPolicyItemAccess("read")));
    policy.getPolicyItems().add(item);
  }

  private void givenCatalogs(Catalog... catalogs) {
    Mockito.when(internalCatalogDispatcher.listCatalogsInfo(Namespace.of(METALAKE)))
        .thenReturn(catalogs);
  }

  private static Catalog catalogInfo(NameIdentifier ident, String provider) {
    Catalog catalog = Mockito.mock(Catalog.class);
    Mockito.when(catalog.name()).thenReturn(ident.name());
    Mockito.when(catalog.provider()).thenReturn(provider);
    return catalog;
  }

  private void givenDefaultSchemaLocation(NameIdentifier catalog, String location) {
    Schema schema = Mockito.mock(Schema.class);
    Mockito.when(schema.properties()).thenReturn(ImmutableMap.of("location", location));
    Mockito.when(
            schemaDispatcher.loadSchema(NameIdentifier.of(METALAKE, catalog.name(), "default")))
        .thenReturn(schema);
  }

  private RangerPolicy addPolicy(long id, String path, String... roles) {
    RangerPolicy policy = new RangerPolicy();
    policy.setId(id);
    policy.setName(path);
    policy.setResources(
        ImmutableMap.of(
            PolicyResource.PATH.getName(), new RangerPolicy.RangerPolicyResource(path)));
    for (String role : roles) {
      RangerPolicy.RangerPolicyItem item = new RangerPolicy.RangerPolicyItem();
      item.setRoles(new ArrayList<>(List.of(role)));
      item.setAccesses(List.of(new RangerPolicy.RangerPolicyItemAccess("read")));
      policy.getPolicyItems().add(item);
    }
    policies.add(policy);
    return policy;
  }

  private List<RangerPolicy> findPolicies(Map<String, String> filters) {
    String path = filters.get(SearchFilter.RESOURCE_PREFIX + PolicyResource.PATH.getName());
    return policies.stream()
        .filter(
            policy ->
                Objects.equals(
                    policy.getResources().get(PolicyResource.PATH.getName()).getValues().get(0),
                    path))
        .collect(Collectors.toList());
  }

  private static List<String> usersOf(RangerPolicy policy) {
    return policy.getPolicyItems().stream()
        .flatMap(item -> item.getUsers().stream())
        .collect(Collectors.toList());
  }

  private static List<String> rolesOf(RangerPolicy policy) {
    return policy.getPolicyItems().stream()
        .flatMap(item -> item.getRoles().stream())
        .collect(Collectors.toList());
  }
}
