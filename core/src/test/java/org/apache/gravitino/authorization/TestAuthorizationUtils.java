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
package org.apache.gravitino.authorization;

import static org.apache.gravitino.Catalog.Type.FILESET;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.util.Collections;
import java.util.List;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.Entity;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.Schema;
import org.apache.gravitino.catalog.CatalogDispatcher;
import org.apache.gravitino.catalog.CatalogManager;
import org.apache.gravitino.catalog.CatalogTestUtils;
import org.apache.gravitino.catalog.SchemaDispatcher;
import org.apache.gravitino.catalog.TableDispatcher;
import org.apache.gravitino.connector.BaseCatalog;
import org.apache.gravitino.connector.authorization.AuthorizationPlugin;
import org.apache.gravitino.exceptions.IllegalNameIdentifierException;
import org.apache.gravitino.exceptions.IllegalNamespaceException;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.RoleEntity;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.utils.ThrowableFunction;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

class TestAuthorizationUtils {

  String metalake = "metalake";

  @Test
  void testCreateNameIdentifier() {
    NameIdentifier user = AuthorizationUtils.ofUser(metalake, "user");
    NameIdentifier group = AuthorizationUtils.ofGroup(metalake, "group");
    NameIdentifier role = AuthorizationUtils.ofRole(metalake, "role");

    Assertions.assertEquals(AuthorizationUtils.ofUserNamespace(metalake), user.namespace());
    Assertions.assertEquals("user", user.name());
    Assertions.assertEquals(AuthorizationUtils.ofGroupNamespace(metalake), group.namespace());
    Assertions.assertEquals("group", group.name());
    Assertions.assertEquals(AuthorizationUtils.ofRoleNamespace(metalake), role.namespace());
    Assertions.assertEquals("role", role.name());
  }

  @Test
  void testCreateNameIdentifierWithInvalidArgs() {
    Assertions.assertThrows(
        IllegalNameIdentifierException.class, () -> AuthorizationUtils.ofUser(metalake, null));
    Assertions.assertThrows(
        IllegalNameIdentifierException.class, () -> AuthorizationUtils.ofUser(metalake, ""));
    Assertions.assertThrows(
        IllegalNameIdentifierException.class, () -> AuthorizationUtils.ofGroup(metalake, null));
    Assertions.assertThrows(
        IllegalNameIdentifierException.class, () -> AuthorizationUtils.ofGroup(metalake, ""));
    Assertions.assertThrows(
        IllegalNameIdentifierException.class, () -> AuthorizationUtils.ofRole(metalake, null));
    Assertions.assertThrows(
        IllegalNameIdentifierException.class, () -> AuthorizationUtils.ofRole(metalake, ""));
  }

  @Test
  void testCreateNamespace() {
    Namespace namespace = AuthorizationUtils.ofUserNamespace(metalake);
    Assertions.assertEquals(3, namespace.length());
    Assertions.assertEquals(metalake, namespace.level(0));
    Assertions.assertEquals("system", namespace.level(1));
    Assertions.assertEquals("user", namespace.level(2));

    namespace = AuthorizationUtils.ofGroupNamespace(metalake);
    Assertions.assertEquals(3, namespace.length());
    Assertions.assertEquals(metalake, namespace.level(0));
    Assertions.assertEquals("system", namespace.level(1));
    Assertions.assertEquals("group", namespace.level(2));

    namespace = AuthorizationUtils.ofRoleNamespace(metalake);
    Assertions.assertEquals(3, namespace.length());
    Assertions.assertEquals(metalake, namespace.level(0));
    Assertions.assertEquals("system", namespace.level(1));
    Assertions.assertEquals("role", namespace.level(2));
  }

  @Test
  void testCreateNamespaceWithInvalidArgs() {
    Assertions.assertThrows(
        IllegalNamespaceException.class, () -> AuthorizationUtils.ofUserNamespace(null));
    Assertions.assertThrows(
        IllegalNamespaceException.class, () -> AuthorizationUtils.ofUserNamespace(""));
    Assertions.assertThrows(
        IllegalNamespaceException.class, () -> AuthorizationUtils.ofGroupNamespace(null));
    Assertions.assertThrows(
        IllegalNamespaceException.class, () -> AuthorizationUtils.ofGroupNamespace(""));
    Assertions.assertThrows(
        IllegalNamespaceException.class, () -> AuthorizationUtils.ofRoleNamespace(null));
    Assertions.assertThrows(
        IllegalNamespaceException.class, () -> AuthorizationUtils.ofRoleNamespace(""));
  }

  @Test
  void testCheckNameIdentifier() {
    NameIdentifier user = AuthorizationUtils.ofUser(metalake, "user");
    NameIdentifier group = AuthorizationUtils.ofGroup(metalake, "group");
    NameIdentifier role = AuthorizationUtils.ofRole(metalake, "role");

    Assertions.assertDoesNotThrow(() -> AuthorizationUtils.checkUser(user));
    Assertions.assertDoesNotThrow(() -> AuthorizationUtils.checkGroup(group));
    Assertions.assertDoesNotThrow(() -> AuthorizationUtils.checkRole(role));

    Assertions.assertThrows(
        IllegalNameIdentifierException.class, () -> AuthorizationUtils.checkUser(null));
    Assertions.assertThrows(
        IllegalNameIdentifierException.class, () -> AuthorizationUtils.checkGroup(null));
    Assertions.assertThrows(
        IllegalNameIdentifierException.class, () -> AuthorizationUtils.checkRole(null));
    Assertions.assertThrows(
        IllegalNameIdentifierException.class,
        () -> AuthorizationUtils.checkUser(NameIdentifier.of("")));
    Assertions.assertThrows(
        IllegalNameIdentifierException.class,
        () -> AuthorizationUtils.checkGroup(NameIdentifier.of("")));
    Assertions.assertThrows(
        IllegalNameIdentifierException.class,
        () -> AuthorizationUtils.checkRole(NameIdentifier.of("")));
  }

  @Test
  void testCheckNamespace() {
    Namespace userNamespace = AuthorizationUtils.ofUserNamespace(metalake);
    Namespace groupNamespace = AuthorizationUtils.ofGroupNamespace(metalake);
    Namespace roleNamespace = AuthorizationUtils.ofRoleNamespace(metalake);

    Assertions.assertDoesNotThrow(() -> AuthorizationUtils.checkUserNamespace(userNamespace));
    Assertions.assertDoesNotThrow(() -> AuthorizationUtils.checkGroupNamespace(groupNamespace));
    Assertions.assertDoesNotThrow(() -> AuthorizationUtils.checkRoleNamespace(roleNamespace));

    Assertions.assertThrows(
        IllegalNamespaceException.class, () -> AuthorizationUtils.checkUserNamespace(null));
    Assertions.assertThrows(
        IllegalNamespaceException.class, () -> AuthorizationUtils.checkGroupNamespace(null));
    Assertions.assertThrows(
        IllegalNamespaceException.class, () -> AuthorizationUtils.checkRoleNamespace(null));
    Assertions.assertThrows(
        IllegalNamespaceException.class,
        () -> AuthorizationUtils.checkUserNamespace(Namespace.of("a", "b")));
    Assertions.assertThrows(
        IllegalNamespaceException.class,
        () -> AuthorizationUtils.checkGroupNamespace(Namespace.of("a")));
    Assertions.assertThrows(
        IllegalNamespaceException.class,
        () -> AuthorizationUtils.checkRoleNamespace(Namespace.of("a", "b", "c", "d")));
  }

  @Test
  void testFilteredSecurableObjects() {

    List<SecurableObject> securableObjects = Lists.newArrayList();

    SecurableObject metalakeObject =
        SecurableObjects.ofMetalake("metalake", Lists.newArrayList(Privileges.SelectTable.allow()));
    securableObjects.add(metalakeObject);

    SecurableObject catalog1Object =
        SecurableObjects.ofCatalog("catalog1", Lists.newArrayList(Privileges.SelectTable.allow()));
    securableObjects.add(catalog1Object);

    SecurableObject catalog2Object =
        SecurableObjects.ofCatalog("catalog2", Lists.newArrayList(Privileges.SelectTable.allow()));
    securableObjects.add(catalog2Object);

    SecurableObject schema1Object =
        SecurableObjects.ofSchema(
            catalog1Object, "schema1", Lists.newArrayList(Privileges.SelectTable.allow()));
    SecurableObject table1Object =
        SecurableObjects.ofTable(
            schema1Object, "table1", Lists.newArrayList(Privileges.SelectTable.allow()));
    securableObjects.add(table1Object);
    securableObjects.add(schema1Object);

    SecurableObject schema2Object =
        SecurableObjects.ofSchema(
            catalog2Object, "schema2", Lists.newArrayList(Privileges.SelectTable.allow()));
    SecurableObject table2Object =
        SecurableObjects.ofTable(
            schema2Object, "table2", Lists.newArrayList(Privileges.SelectTable.allow()));
    securableObjects.add(table2Object);
    securableObjects.add(schema2Object);

    RoleEntity role =
        RoleEntity.builder()
            .withId(1L)
            .withName("role")
            .withSecurableObjects(securableObjects)
            .withAuditInfo(AuditInfo.EMPTY)
            .build();
    Role filteredRole = AuthorizationUtils.filterSecurableObjects(role, "metalake", "catalog1");
    Assertions.assertEquals(4, filteredRole.securableObjects().size());
    Assertions.assertTrue(filteredRole.securableObjects().contains(metalakeObject));
    Assertions.assertTrue(filteredRole.securableObjects().contains(catalog1Object));
    Assertions.assertTrue(filteredRole.securableObjects().contains(schema1Object));
    Assertions.assertTrue(filteredRole.securableObjects().contains(table1Object));

    filteredRole = AuthorizationUtils.filterSecurableObjects(role, "metalake", "catalog2");
    Assertions.assertEquals(4, filteredRole.securableObjects().size());
    Assertions.assertTrue(filteredRole.securableObjects().contains(metalakeObject));
    Assertions.assertTrue(filteredRole.securableObjects().contains(catalog2Object));
    Assertions.assertTrue(filteredRole.securableObjects().contains(schema2Object));
    Assertions.assertTrue(filteredRole.securableObjects().contains(table2Object));
  }

  @Test
  void testGetMetadataObjectLocation() throws IllegalAccessException {
    CatalogDispatcher catalogDispatcher = Mockito.mock(CatalogDispatcher.class);
    TableDispatcher tableDispatcher = Mockito.mock(TableDispatcher.class);
    Catalog catalog = Mockito.mock(Catalog.class);
    Table table = Mockito.mock(Table.class);
    AccessControlDispatcher accessControlDispatcher = Mockito.mock(AccessControlDispatcher.class);

    Mockito.when(table.properties()).thenReturn(ImmutableMap.of("location", "gs://bucket/1"));
    Mockito.when(catalog.provider()).thenReturn("hive");
    Mockito.when(catalogDispatcher.loadCatalog(Mockito.any())).thenReturn(catalog);
    Mockito.when(tableDispatcher.loadTable(Mockito.any())).thenReturn(table);

    FieldUtils.writeField(
        GravitinoEnv.getInstance(), "internalCatalogDispatcher", catalogDispatcher, true);
    FieldUtils.writeField(
        GravitinoEnv.getInstance(), "internalTableDispatcher", tableDispatcher, true);
    FieldUtils.writeField(
        GravitinoEnv.getInstance(),
        "internalAccessControlDispatcher",
        accessControlDispatcher,
        true);

    List<String> locations =
        AuthorizationUtils.getMetadataObjectLocation(
            NameIdentifier.of("catalog", "schema", "table"), Entity.EntityType.TABLE);
    Assertions.assertEquals(1, locations.size());
    Assertions.assertEquals("gs://bucket/1", locations.get(0));

    locations =
        AuthorizationUtils.getMetadataObjectLocation(
            NameIdentifier.of("catalog", "schema", "fileset"), Entity.EntityType.TABLE);
    Assertions.assertEquals(1, locations.size());
    Assertions.assertEquals("gs://bucket/1", locations.get(0));
  }

  @Test
  void testGetSchemaTypeMetadataObjectLocation() throws IllegalAccessException {
    AccessControlDispatcher accessControlDispatcher = Mockito.mock(AccessControlDispatcher.class);
    SchemaDispatcher schemaDispatcher = Mockito.mock(SchemaDispatcher.class);
    CatalogDispatcher catalogDispatcher = Mockito.mock(CatalogDispatcher.class);
    Catalog catalog = Mockito.mock(Catalog.class);
    Schema schema = Mockito.mock(Schema.class);

    Mockito.when(schema.name()).thenReturn("testSchema");
    Mockito.when(catalog.provider()).thenReturn("fileset");
    Mockito.when(catalog.type()).thenReturn(FILESET);
    Mockito.when(schemaDispatcher.loadSchema(Mockito.any())).thenReturn(schema);
    Mockito.when(catalogDispatcher.loadCatalog(Mockito.any())).thenReturn(catalog);

    FieldUtils.writeField(
        GravitinoEnv.getInstance(),
        "internalAccessControlDispatcher",
        accessControlDispatcher,
        true);
    FieldUtils.writeField(
        GravitinoEnv.getInstance(), "internalCatalogDispatcher", catalogDispatcher, true);
    FieldUtils.writeField(
        GravitinoEnv.getInstance(), "internalSchemaDispatcher", schemaDispatcher, true);

    // Case 1: Schema has no location property (inherits catalog location)
    Mockito.when(schema.properties()).thenReturn(Collections.emptyMap());
    Mockito.when(catalog.properties()).thenReturn(ImmutableMap.of("location", "catalogLocation"));
    List<String> locations =
        AuthorizationUtils.getMetadataObjectLocation(
            NameIdentifier.of("catalog", "schema", "fileset"), Entity.EntityType.SCHEMA);
    Assertions.assertEquals(1, locations.size());
    Assertions.assertEquals("catalogLocation/testSchema", locations.get(0));

    // Case 2: Schema has null properties (inherits catalog location)
    Mockito.when(schema.properties()).thenReturn(null);
    locations =
        AuthorizationUtils.getMetadataObjectLocation(
            NameIdentifier.of("catalog", "schema", "fileset"), Entity.EntityType.SCHEMA);
    Assertions.assertEquals(1, locations.size());
    Assertions.assertEquals("catalogLocation/testSchema", locations.get(0));

    // Case 3: Schema location is empty string (falls back to catalog location)
    Mockito.when(schema.properties()).thenReturn(ImmutableMap.of("location", ""));
    locations =
        AuthorizationUtils.getMetadataObjectLocation(
            NameIdentifier.of("catalog", "schema", "fileset"), Entity.EntityType.SCHEMA);
    Assertions.assertEquals(1, locations.size());
    Assertions.assertEquals("catalogLocation/testSchema", locations.get(0));

    // Case 4: Schema has explicit location (takes precedence over catalog location)
    Mockito.when(schema.properties()).thenReturn(ImmutableMap.of("location", "schemaLocation"));
    locations =
        AuthorizationUtils.getMetadataObjectLocation(
            NameIdentifier.of("catalog", "schema", "fileset"), Entity.EntityType.SCHEMA);
    Assertions.assertEquals(1, locations.size());
    Assertions.assertEquals("schemaLocation", locations.get(0));

    // Case 5: Neither schema nor catalog has location property
    Mockito.when(schema.properties()).thenReturn(Collections.emptyMap());
    Mockito.when(catalog.properties()).thenReturn(Collections.emptyMap());
    locations =
        AuthorizationUtils.getMetadataObjectLocation(
            NameIdentifier.of("catalog", "schema", "fileset"), Entity.EntityType.SCHEMA);
    Assertions.assertEquals(0, locations.size());
  }

  @Test
  void testRenamePrivilegesNotifiesOldEntityNameIdMappingChange() {
    GravitinoAuthorizer authorizer = Mockito.mock(GravitinoAuthorizer.class);
    AccessControlDispatcher accessControlDispatcher = Mockito.mock(AccessControlDispatcher.class);
    CatalogManager catalogManager = Mockito.mock(CatalogManager.class);
    BaseCatalog<?> baseCatalog = Mockito.mock(BaseCatalog.class);
    CatalogTestUtils.mockDoWithCatalog(catalogManager, baseCatalog);

    GravitinoEnv envMock = Mockito.mock(GravitinoEnv.class);
    Mockito.when(envMock.gravitinoAuthorizer()).thenReturn(authorizer);
    Mockito.when(envMock.internalAccessControlDispatcher()).thenReturn(accessControlDispatcher);
    Mockito.when(envMock.catalogManager()).thenReturn(catalogManager);

    try (MockedStatic<GravitinoEnv> envStatic = Mockito.mockStatic(GravitinoEnv.class)) {
      envStatic.when(GravitinoEnv::getInstance).thenReturn(envMock);

      NameIdentifier ident = NameIdentifier.of("metalake", "catalog", "schema", "table");
      AuthorizationUtils.authorizationPluginRenamePrivileges(
          ident, Entity.EntityType.TABLE, "new_table");

      Mockito.verify(authorizer)
          .handleEntityNameIdMappingChange("metalake", ident, Entity.EntityType.TABLE);
    }
  }

  @Test
  void testRemovePrivilegesNotifiesEntityNameIdMappingChange() {
    GravitinoAuthorizer authorizer = Mockito.mock(GravitinoAuthorizer.class);
    AccessControlDispatcher accessControlDispatcher = Mockito.mock(AccessControlDispatcher.class);
    CatalogManager catalogManager = Mockito.mock(CatalogManager.class);
    BaseCatalog<?> baseCatalog = Mockito.mock(BaseCatalog.class);
    Mockito.when(catalogManager.loadCatalog(Mockito.any())).thenReturn(baseCatalog);

    GravitinoEnv envMock = Mockito.mock(GravitinoEnv.class);
    Mockito.when(envMock.gravitinoAuthorizer()).thenReturn(authorizer);
    Mockito.when(envMock.internalAccessControlDispatcher()).thenReturn(accessControlDispatcher);
    Mockito.when(envMock.catalogManager()).thenReturn(catalogManager);

    try (MockedStatic<GravitinoEnv> envStatic = Mockito.mockStatic(GravitinoEnv.class)) {
      envStatic.when(GravitinoEnv::getInstance).thenReturn(envMock);

      NameIdentifier ident = NameIdentifier.of("metalake", "catalog", "schema", "table");
      AuthorizationUtils.authorizationPluginRemovePrivileges(
          ident, Entity.EntityType.TABLE, Collections.emptyList());

      Mockito.verify(authorizer)
          .handleEntityNameIdMappingChange("metalake", ident, Entity.EntityType.TABLE);
    }
  }

  @Test
  void testRenameTablePrivilegesNotifiesAuthorizationPluginWithExpectedChange() {
    NameIdentifier ident = NameIdentifier.of("metalake", "catalog", "schema", "table");
    List<String> locations = Lists.newArrayList("/warehouse/schema/table");

    AccessControlDispatcher accessControlDispatcher = Mockito.mock(AccessControlDispatcher.class);
    CatalogManager catalogManager = Mockito.mock(CatalogManager.class);
    BaseCatalog<?> baseCatalog = Mockito.mock(BaseCatalog.class);
    AuthorizationPlugin authorizationPlugin = Mockito.mock(AuthorizationPlugin.class);
    CatalogTestUtils.mockDoWithCatalog(catalogManager, baseCatalog);
    Mockito.when(baseCatalog.getAuthorizationPlugin()).thenReturn(authorizationPlugin);

    GravitinoEnv envMock = Mockito.mock(GravitinoEnv.class);
    Mockito.when(envMock.internalAccessControlDispatcher()).thenReturn(accessControlDispatcher);
    Mockito.when(envMock.catalogManager()).thenReturn(catalogManager);

    try (MockedStatic<GravitinoEnv> envStatic = Mockito.mockStatic(GravitinoEnv.class)) {
      envStatic.when(GravitinoEnv::getInstance).thenReturn(envMock);

      AuthorizationUtils.authorizationPluginRenamePrivileges(
          ident, Entity.EntityType.TABLE, "renamed_table", locations);
    }

    ArgumentCaptor<MetadataObjectChange[]> changesCaptor =
        ArgumentCaptor.forClass(MetadataObjectChange[].class);
    Mockito.verify(authorizationPlugin).onMetadataUpdated(changesCaptor.capture());
    Assertions.assertEquals(1, changesCaptor.getValue().length);

    MetadataObjectChange.RenameMetadataObject renameChange =
        Assertions.assertInstanceOf(
            MetadataObjectChange.RenameMetadataObject.class, changesCaptor.getValue()[0]);
    Assertions.assertEquals(MetadataObject.Type.TABLE, renameChange.metadataObject().type());
    Assertions.assertEquals("catalog.schema.table", renameChange.metadataObject().fullName());
    Assertions.assertEquals(MetadataObject.Type.TABLE, renameChange.newMetadataObject().type());
    Assertions.assertEquals(
        "catalog.schema.renamed_table", renameChange.newMetadataObject().fullName());
    Assertions.assertEquals(locations, renameChange.locations());
  }

  @Test
  void testRemoveTablePrivilegesNotifiesAuthorizationPluginWithExpectedChange() {
    NameIdentifier ident = NameIdentifier.of("metalake", "catalog", "schema", "table");
    List<String> locations = Lists.newArrayList("/warehouse/schema/table");

    AccessControlDispatcher accessControlDispatcher = Mockito.mock(AccessControlDispatcher.class);
    CatalogManager catalogManager = Mockito.mock(CatalogManager.class);
    BaseCatalog<?> baseCatalog = Mockito.mock(BaseCatalog.class);
    AuthorizationPlugin authorizationPlugin = Mockito.mock(AuthorizationPlugin.class);
    CatalogTestUtils.mockDoWithCatalog(catalogManager, baseCatalog);
    Mockito.when(baseCatalog.getAuthorizationPlugin()).thenReturn(authorizationPlugin);

    GravitinoEnv envMock = Mockito.mock(GravitinoEnv.class);
    Mockito.when(envMock.internalAccessControlDispatcher()).thenReturn(accessControlDispatcher);
    Mockito.when(envMock.catalogManager()).thenReturn(catalogManager);

    try (MockedStatic<GravitinoEnv> envStatic = Mockito.mockStatic(GravitinoEnv.class)) {
      envStatic.when(GravitinoEnv::getInstance).thenReturn(envMock);

      AuthorizationUtils.authorizationPluginRemovePrivileges(
          ident, Entity.EntityType.TABLE, locations);
    }

    ArgumentCaptor<MetadataObjectChange[]> changesCaptor =
        ArgumentCaptor.forClass(MetadataObjectChange[].class);
    Mockito.verify(authorizationPlugin).onMetadataUpdated(changesCaptor.capture());
    Assertions.assertEquals(1, changesCaptor.getValue().length);

    MetadataObjectChange.RemoveMetadataObject removeChange =
        Assertions.assertInstanceOf(
            MetadataObjectChange.RemoveMetadataObject.class, changesCaptor.getValue()[0]);
    Assertions.assertEquals(MetadataObject.Type.TABLE, removeChange.metadataObject().type());
    Assertions.assertEquals("catalog.schema.table", removeChange.metadataObject().fullName());
    Assertions.assertEquals(locations, removeChange.getLocations());
  }

  @Test
  void testRemoveCatalogPrivilegesUsesLeasedCatalogName() {
    NameIdentifier requestedIdent = NameIdentifier.of("metalake", "requested_catalog");
    List<String> locations = List.of("/warehouse/catalog");
    CatalogManager catalogManager = Mockito.mock(CatalogManager.class);
    BaseCatalog<?> catalog = Mockito.mock(BaseCatalog.class);
    AuthorizationPlugin plugin = Mockito.mock(AuthorizationPlugin.class);
    CatalogTestUtils.mockDoWithCatalog(catalogManager, catalog);
    Mockito.when(catalog.name()).thenReturn("canonical_catalog");
    Mockito.when(catalog.getAuthorizationPlugin()).thenReturn(plugin);

    GravitinoEnv env = Mockito.mock(GravitinoEnv.class);
    Mockito.when(env.catalogManager()).thenReturn(catalogManager);
    try (MockedStatic<GravitinoEnv> envStatic = Mockito.mockStatic(GravitinoEnv.class)) {
      envStatic.when(GravitinoEnv::getInstance).thenReturn(env);
      AuthorizationUtils.removeCatalogPrivileges(requestedIdent, locations);
    }

    Mockito.verify(catalogManager).doWithCatalog(Mockito.eq(requestedIdent), Mockito.any());
    ArgumentCaptor<MetadataObjectChange[]> changes =
        ArgumentCaptor.forClass(MetadataObjectChange[].class);
    Mockito.verify(plugin).onMetadataUpdated(changes.capture());
    Assertions.assertEquals(1, changes.getValue().length);
    MetadataObjectChange.RemoveMetadataObject removal =
        Assertions.assertInstanceOf(
            MetadataObjectChange.RemoveMetadataObject.class, changes.getValue()[0]);
    Assertions.assertEquals(MetadataObject.Type.CATALOG, removal.metadataObject().type());
    Assertions.assertEquals("canonical_catalog", removal.metadataObject().fullName());
    Assertions.assertEquals(locations, removal.getLocations());
  }

  @Test
  void testMetalakeUpdateVisitsEachCatalogWithAuthorization() {
    CatalogManager catalogManager = Mockito.mock(CatalogManager.class);
    NameIdentifier first = NameIdentifier.of(metalake, "first");
    NameIdentifier second = NameIdentifier.of(metalake, "second");
    NameIdentifier withoutAuthorization = NameIdentifier.of(metalake, "without_auth");
    Mockito.when(catalogManager.listCatalogs(Namespace.of(metalake)))
        .thenReturn(new NameIdentifier[] {first, second, withoutAuthorization});
    BaseCatalog<?> firstCatalog = Mockito.mock(BaseCatalog.class);
    BaseCatalog<?> secondCatalog = Mockito.mock(BaseCatalog.class);
    BaseCatalog<?> plainCatalog = Mockito.mock(BaseCatalog.class);
    AuthorizationPlugin firstPlugin = Mockito.mock(AuthorizationPlugin.class);
    AuthorizationPlugin secondPlugin = Mockito.mock(AuthorizationPlugin.class);
    Mockito.when(firstCatalog.getAuthorizationPlugin()).thenReturn(firstPlugin);
    Mockito.when(secondCatalog.getAuthorizationPlugin()).thenReturn(secondPlugin);
    CatalogTestUtils.mockDoWithCatalog(catalogManager, plainCatalog);
    Mockito.doAnswer(
            invocation -> {
              ThrowableFunction<BaseCatalog, Object> operation = invocation.getArgument(1);
              NameIdentifier ident = invocation.getArgument(0);
              return operation.apply(first.equals(ident) ? firstCatalog : secondCatalog);
            })
        .when(catalogManager)
        .doWithCatalog(
            Mockito.argThat(ident -> first.equals(ident) || second.equals(ident)), Mockito.any());

    List<AuthorizationPlugin> visited = Lists.newArrayList();
    GravitinoEnv env = Mockito.mock(GravitinoEnv.class);
    Mockito.when(env.catalogManager()).thenReturn(catalogManager);
    try (MockedStatic<GravitinoEnv> envStatic = Mockito.mockStatic(GravitinoEnv.class)) {
      envStatic.when(GravitinoEnv::getInstance).thenReturn(env);
      AuthorizationUtils.callAuthorizationPluginForMetadataObject(
          metalake, MetadataObjects.of(null, metalake, MetadataObject.Type.METALAKE), visited::add);
    }

    Assertions.assertEquals(List.of(firstPlugin, secondPlugin), visited);
    Mockito.verify(catalogManager).doWithCatalog(Mockito.eq(withoutAuthorization), Mockito.any());
  }
}
