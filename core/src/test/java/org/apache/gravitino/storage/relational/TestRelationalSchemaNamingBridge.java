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
package org.apache.gravitino.storage.relational;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.common.collect.Lists;
import java.time.Instant;
import java.util.List;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.Entity;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.authorization.Privilege;
import org.apache.gravitino.authorization.Privileges;
import org.apache.gravitino.authorization.SecurableObject;
import org.apache.gravitino.authorization.SecurableObjects;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.GenericEntity;
import org.apache.gravitino.meta.RoleEntity;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/** Unit tests for {@link RelationalSchemaNamingBridge}. */
public class TestRelationalSchemaNamingBridge {

  /** ASCII-1 internal separator used in physical storage. */
  private static final String P = "";

  @BeforeEach
  public void setUp() throws Exception {
    mockSeparator(":");
  }

  @AfterEach
  public void tearDown() throws Exception {
    FieldUtils.writeField(GravitinoEnv.getInstance(), "config", null, true);
  }

  // -------------------------------------------------------------------------
  // convertMetadataObjectDottedFullName – each MetadataObject.Type
  // -------------------------------------------------------------------------

  @Test
  public void schemaToStorage() {
    // SCHEMA expects 2 dot-parts; schema segment "a:b:c" → "abc"
    String logical = "catalog.a:b:c";
    String physical =
        RelationalSchemaNamingBridge.convertMetadataObjectDottedFullName(
            logical, MetadataObject.Type.SCHEMA, true);
    assertEquals("catalog.a" + P + "b" + P + "c", physical);
  }

  @Test
  public void schemaToApi() {
    String physical = "catalog.a" + P + "b" + P + "c";
    String logical =
        RelationalSchemaNamingBridge.convertMetadataObjectDottedFullName(
            physical, MetadataObject.Type.SCHEMA, false);
    assertEquals("catalog.a:b:c", logical);
  }

  @Test
  public void tableToStorage() {
    // TABLE expects 3 dot-parts; schema segment at index 1
    String logical = "catalog.a:b.mytable";
    String physical =
        RelationalSchemaNamingBridge.convertMetadataObjectDottedFullName(
            logical, MetadataObject.Type.TABLE, true);
    assertEquals("catalog.a" + P + "b.mytable", physical);
  }

  @Test
  public void tableToApi() {
    String physical = "catalog.a" + P + "b.mytable";
    String logical =
        RelationalSchemaNamingBridge.convertMetadataObjectDottedFullName(
            physical, MetadataObject.Type.TABLE, false);
    assertEquals("catalog.a:b.mytable", logical);
  }

  @Test
  public void viewToStorage() {
    String logical = "catalog.a:b.myview";
    String physical =
        RelationalSchemaNamingBridge.convertMetadataObjectDottedFullName(
            logical, MetadataObject.Type.VIEW, true);
    assertEquals("catalog.a" + P + "b.myview", physical);
  }

  @Test
  public void functionToStorage() {
    String logical = "catalog.a:b.myfunc";
    String physical =
        RelationalSchemaNamingBridge.convertMetadataObjectDottedFullName(
            logical, MetadataObject.Type.FUNCTION, true);
    assertEquals("catalog.a" + P + "b.myfunc", physical);
  }

  @Test
  public void columnToStorage() {
    // COLUMN expects 4 dot-parts
    String logical = "catalog.a:b.mytable.mycol";
    String physical =
        RelationalSchemaNamingBridge.convertMetadataObjectDottedFullName(
            logical, MetadataObject.Type.COLUMN, true);
    assertEquals("catalog.a" + P + "b.mytable.mycol", physical);
  }

  @Test
  public void metalakePassthrough() {
    String name = "mymetal";
    String result =
        RelationalSchemaNamingBridge.convertMetadataObjectDottedFullName(
            name, MetadataObject.Type.METALAKE, true);
    assertEquals(name, result);
  }

  @Test
  public void catalogPassthrough() {
    String name = "catalog";
    String result =
        RelationalSchemaNamingBridge.convertMetadataObjectDottedFullName(
            name, MetadataObject.Type.CATALOG, true);
    assertEquals(name, result);
  }

  // -------------------------------------------------------------------------
  // Part-count mismatch → input returned unchanged
  // -------------------------------------------------------------------------

  @Test
  public void schemaPartCountMismatch() {
    // SCHEMA expects 2 parts; 3 parts → no conversion
    String name = "catalog.schema.extra";
    String result =
        RelationalSchemaNamingBridge.convertMetadataObjectDottedFullName(
            name, MetadataObject.Type.SCHEMA, true);
    assertEquals(name, result);
  }

  @Test
  public void tablePartCountMismatch() {
    // TABLE expects 3 parts; 2 parts → no conversion
    String name = "catalog.schema";
    String result =
        RelationalSchemaNamingBridge.convertMetadataObjectDottedFullName(
            name, MetadataObject.Type.TABLE, true);
    assertEquals(name, result);
  }

  // -------------------------------------------------------------------------
  // Round-trip: logical → physical → logical
  // -------------------------------------------------------------------------

  @Test
  public void schemaRoundTrip() {
    String logical = "catalog.a:b:c";
    String physical =
        RelationalSchemaNamingBridge.convertMetadataObjectDottedFullName(
            logical, MetadataObject.Type.SCHEMA, true);
    String backToLogical =
        RelationalSchemaNamingBridge.convertMetadataObjectDottedFullName(
            physical, MetadataObject.Type.SCHEMA, false);
    assertEquals(logical, backToLogical);
  }

  @Test
  public void tableRoundTrip() {
    String logical = "catalog.a:b.mytable";
    String physical =
        RelationalSchemaNamingBridge.convertMetadataObjectDottedFullName(
            logical, MetadataObject.Type.TABLE, true);
    String backToLogical =
        RelationalSchemaNamingBridge.convertMetadataObjectDottedFullName(
            physical, MetadataObject.Type.TABLE, false);
    assertEquals(logical, backToLogical);
  }

  // -------------------------------------------------------------------------
  // Non-default separator
  // -------------------------------------------------------------------------

  @Test
  public void customSeparatorSchema() throws Exception {
    mockSeparator("/");
    String logical = "catalog.a/b/c";
    String physical =
        RelationalSchemaNamingBridge.convertMetadataObjectDottedFullName(
            logical, MetadataObject.Type.SCHEMA, true);
    assertEquals("catalog.a" + P + "b" + P + "c", physical);

    String backToLogical =
        RelationalSchemaNamingBridge.convertMetadataObjectDottedFullName(
            physical, MetadataObject.Type.SCHEMA, false);
    assertEquals(logical, backToLogical);
  }

  // -------------------------------------------------------------------------
  // embeddedNamespaceForStorage / embeddedNamespaceForApi
  // -------------------------------------------------------------------------

  @Test
  public void embeddedNsToStorage() {
    Namespace ns = Namespace.of("ml", "cat", "a:b:c");
    Namespace stored = RelationalSchemaNamingBridge.embeddedNamespaceForStorage(ns);
    assertEquals(Namespace.of("ml", "cat", "a" + P + "b" + P + "c"), stored);
  }

  @Test
  public void embeddedNsToApi() {
    Namespace ns = Namespace.of("ml", "cat", "a" + P + "b" + P + "c");
    Namespace api = RelationalSchemaNamingBridge.embeddedNamespaceForApi(ns);
    assertEquals(Namespace.of("ml", "cat", "a:b:c"), api);
  }

  @Test
  public void embeddedNsShortUnchanged() {
    // Namespaces with fewer than 3 levels are returned as-is
    Namespace ns = Namespace.of("ml", "cat");
    Namespace stored = RelationalSchemaNamingBridge.embeddedNamespaceForStorage(ns);
    assertSame(ns, stored);
  }

  // -------------------------------------------------------------------------
  // nameIdentifierForStorage / nameIdentifierForApi – SCHEMA
  // -------------------------------------------------------------------------

  @Test
  public void schemaIdentToStorage() {
    NameIdentifier ident = NameIdentifier.of("ml", "cat", "a:b");
    NameIdentifier stored =
        RelationalSchemaNamingBridge.nameIdentifierForStorage(ident, Entity.EntityType.SCHEMA);
    assertEquals("a" + P + "b", stored.name());
  }

  @Test
  public void schemaIdentToApi() {
    NameIdentifier ident = NameIdentifier.of("ml", "cat", "a" + P + "b");
    NameIdentifier api =
        RelationalSchemaNamingBridge.nameIdentifierForApi(ident, Entity.EntityType.SCHEMA);
    assertEquals("a:b", api.name());
  }

  @Test
  public void flatSchemaIdent() {
    NameIdentifier ident = NameIdentifier.of("ml", "cat", "flat");
    assertEquals(
        "flat",
        RelationalSchemaNamingBridge.nameIdentifierForStorage(ident, Entity.EntityType.SCHEMA)
            .name());
    assertEquals(
        "flat",
        RelationalSchemaNamingBridge.nameIdentifierForApi(ident, Entity.EntityType.SCHEMA).name());
  }

  // -------------------------------------------------------------------------
  // wrapperUpdater – SCHEMA guard
  // -------------------------------------------------------------------------

  @Test
  public void wrapperUpdaterRejectsSchema() {
    assertThrows(
        IllegalArgumentException.class,
        () -> RelationalSchemaNamingBridge.wrapperUpdater(Entity.EntityType.SCHEMA, e -> e));
  }

  // -------------------------------------------------------------------------
  // genericEntityMetadataFullNameForApi
  // -------------------------------------------------------------------------

  @Test
  public void genericEntityApiTable() {
    // TABLE has 3 dot-parts; schema segment at index 1 gets physical→logical conversion
    String physicalName = "catalog.a" + P + "b.mytable";
    GenericEntity entity =
        GenericEntity.builder()
            .withId(1L)
            .withEntityType(Entity.EntityType.TABLE)
            .withName(physicalName)
            .withNamespace(Namespace.of("ml"))
            .build();
    GenericEntity result = RelationalSchemaNamingBridge.genericEntityMetadataFullNameForApi(entity);
    assertEquals("catalog.a:b.mytable", result.name());
  }

  @Test
  public void genericEntityApiSchema() {
    // SCHEMA has 2 dot-parts
    String physicalName = "catalog.a" + P + "b" + P + "c";
    GenericEntity entity =
        GenericEntity.builder()
            .withId(2L)
            .withEntityType(Entity.EntityType.SCHEMA)
            .withName(physicalName)
            .withNamespace(Namespace.of("ml"))
            .build();
    GenericEntity result = RelationalSchemaNamingBridge.genericEntityMetadataFullNameForApi(entity);
    assertEquals("catalog.a:b:c", result.name());
  }

  @Test
  public void genericEntityApiFlatSchema() {
    // Schema name with no physical separator → no conversion; same instance returned
    String flatName = "catalog.flat";
    GenericEntity entity =
        GenericEntity.builder()
            .withId(3L)
            .withEntityType(Entity.EntityType.SCHEMA)
            .withName(flatName)
            .withNamespace(Namespace.of("ml"))
            .build();
    GenericEntity result = RelationalSchemaNamingBridge.genericEntityMetadataFullNameForApi(entity);
    assertSame(entity, result);
  }

  @Test
  public void genericEntityApiNullName() {
    GenericEntity entity =
        GenericEntity.builder()
            .withId(4L)
            .withEntityType(Entity.EntityType.TABLE)
            .withName(null)
            .withNamespace(Namespace.of("ml"))
            .build();
    GenericEntity result = RelationalSchemaNamingBridge.genericEntityMetadataFullNameForApi(entity);
    assertSame(entity, result);
  }

  @Test
  public void genericEntityApiUnmappedType() {
    // Entity types that don't map to MetadataObject.Type (e.g. TABLE_STATISTIC) are returned as-is
    GenericEntity entity =
        GenericEntity.builder()
            .withId(5L)
            .withEntityType(Entity.EntityType.TABLE_STATISTIC)
            .withName("some.name")
            .withNamespace(Namespace.of("ml"))
            .build();
    GenericEntity result = RelationalSchemaNamingBridge.genericEntityMetadataFullNameForApi(entity);
    assertSame(entity, result);
  }

  // -------------------------------------------------------------------------
  // roleEntityForStorage / roleEntityForApi
  // -------------------------------------------------------------------------

  @Test
  public void roleWithNestedSchemaSecurableObjectToStorage() {
    // SecurableObject of type SCHEMA with logical name "catalog.a:b"
    List<Privilege> privs = Lists.newArrayList(Privileges.UseSchema.allow());
    SecurableObject catalogObj = SecurableObjects.ofCatalog("catalog", Lists.newArrayList());
    SecurableObject schemaObj = SecurableObjects.ofSchema(catalogObj, "a:b", privs);

    RoleEntity role = buildRole(Lists.newArrayList(schemaObj));

    RoleEntity stored = RelationalSchemaNamingBridge.roleEntityForStorage(role);
    SecurableObject storedSchema = stored.securableObjects().get(0);
    // schema segment "a:b" → "ab"
    assertEquals("catalog.a" + P + "b", storedSchema.fullName());
  }

  @Test
  public void roleWithNestedSchemaSecurableObjectToApi() {
    // SecurableObject of type SCHEMA with physical name "catalog.ab"
    List<Privilege> privs = Lists.newArrayList(Privileges.UseSchema.allow());
    SecurableObject schemaObj =
        SecurableObjects.parse("catalog.a" + P + "b", MetadataObject.Type.SCHEMA, privs);

    RoleEntity role = buildRole(Lists.newArrayList(schemaObj));

    RoleEntity api = RelationalSchemaNamingBridge.roleEntityForApi(role);
    SecurableObject apiSchema = api.securableObjects().get(0);
    assertEquals("catalog.a:b", apiSchema.fullName());
  }

  @Test
  public void roleWithFlatSchemaIsUnchanged() {
    // A schema without the separator → same instance returned
    List<Privilege> privs = Lists.newArrayList(Privileges.UseSchema.allow());
    SecurableObject catalogObj = SecurableObjects.ofCatalog("catalog", Lists.newArrayList());
    SecurableObject schemaObj = SecurableObjects.ofSchema(catalogObj, "flat", privs);

    RoleEntity role = buildRole(Lists.newArrayList(schemaObj));
    assertSame(role, RelationalSchemaNamingBridge.roleEntityForStorage(role));
    assertSame(role, RelationalSchemaNamingBridge.roleEntityForApi(role));
  }

  @Test
  public void roleWithNoSecurableObjectsIsUnchanged() {
    RoleEntity role = buildRole(null);
    assertSame(role, RelationalSchemaNamingBridge.roleEntityForStorage(role));
    assertSame(role, RelationalSchemaNamingBridge.roleEntityForApi(role));
  }

  @Test
  public void roleRoundTrip() {
    List<Privilege> privs = Lists.newArrayList(Privileges.UseSchema.allow());
    SecurableObject catalogObj = SecurableObjects.ofCatalog("catalog", Lists.newArrayList());
    SecurableObject schemaObj = SecurableObjects.ofSchema(catalogObj, "a:b:c", privs);

    RoleEntity original = buildRole(Lists.newArrayList(schemaObj));
    RoleEntity stored = RelationalSchemaNamingBridge.roleEntityForStorage(original);
    RoleEntity backToApi = RelationalSchemaNamingBridge.roleEntityForApi(stored);

    assertEquals(
        original.securableObjects().get(0).fullName(),
        backToApi.securableObjects().get(0).fullName());
  }

  // -------------------------------------------------------------------------
  // Helpers
  // -------------------------------------------------------------------------

  private static RoleEntity buildRole(List<SecurableObject> securableObjects) {
    AuditInfo audit =
        AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build();
    return RoleEntity.builder()
        .withId(1L)
        .withName("role1")
        .withNamespace(Namespace.of("ml"))
        .withProperties(null)
        .withAuditInfo(audit)
        .withSecurableObjects(securableObjects)
        .build();
  }

  private static void mockSeparator(String sep) throws Exception {
    Config config = Mockito.mock(Config.class);
    Mockito.when(config.get(Configs.SCHEMA_SEPARATOR)).thenReturn(sep);
    FieldUtils.writeField(GravitinoEnv.getInstance(), "config", config, true);
  }
}
