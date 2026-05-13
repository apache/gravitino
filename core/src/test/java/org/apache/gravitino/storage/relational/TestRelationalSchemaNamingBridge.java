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
import org.apache.gravitino.meta.SchemaEntity;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/** Unit tests for {@link RelationalSchemaNamingBridge}. */
public class TestRelationalSchemaNamingBridge {

  /** ASCII-1 internal separator used in physical storage. */
  private static final String P = "";

  private static final List<Privilege> USE_SCHEMA_PRIVS =
      Lists.newArrayList(Privileges.UseSchema.allow());
  private static final SecurableObject CATALOG_OBJ =
      SecurableObjects.ofCatalog("catalog", Lists.newArrayList());

  @BeforeEach
  public void setUp() throws Exception {
    mockSeparator(":");
  }

  @AfterEach
  public void tearDown() throws Exception {
    FieldUtils.writeField(GravitinoEnv.getInstance(), "config", null, true);
  }

  // -------------------------------------------------------------------------
  // securableObjectForStorage / securableObjectForApi
  // -------------------------------------------------------------------------

  @Test
  public void securableObjectSchemaToStorage() {
    SecurableObject obj = SecurableObjects.ofSchema(CATALOG_OBJ, "a:b:c", USE_SCHEMA_PRIVS);
    SecurableObject result = RelationalSchemaNamingBridge.securableObjectForStorage(obj);
    assertEquals("catalog.a" + P + "b" + P + "c", result.fullName());
  }

  @Test
  public void securableObjectSchemaToApi() {
    SecurableObject obj =
        SecurableObjects.parse(
            "catalog.a" + P + "b" + P + "c", MetadataObject.Type.SCHEMA, USE_SCHEMA_PRIVS);
    SecurableObject result = RelationalSchemaNamingBridge.securableObjectForApi(obj);
    assertEquals("catalog.a:b:c", result.fullName());
  }

  @Test
  public void securableObjectTableToStorage() {
    SecurableObject obj =
        SecurableObjects.parse(
            "catalog.a:b.mytable", MetadataObject.Type.TABLE, Lists.newArrayList());
    SecurableObject result = RelationalSchemaNamingBridge.securableObjectForStorage(obj);
    assertEquals("catalog.a" + P + "b.mytable", result.fullName());
  }

  @Test
  public void securableObjectTableToApi() {
    SecurableObject obj =
        SecurableObjects.parse(
            "catalog.a" + P + "b.mytable", MetadataObject.Type.TABLE, Lists.newArrayList());
    SecurableObject result = RelationalSchemaNamingBridge.securableObjectForApi(obj);
    assertEquals("catalog.a:b.mytable", result.fullName());
  }

  @Test
  public void securableObjectCatalogPassthrough() {
    SecurableObject catalog = SecurableObjects.ofCatalog("catalog", Lists.newArrayList());
    assertSame(catalog, RelationalSchemaNamingBridge.securableObjectForStorage(catalog));
    assertSame(catalog, RelationalSchemaNamingBridge.securableObjectForApi(catalog));
  }

  @Test
  public void securableObjectTableRoundTrip() {
    SecurableObject obj =
        SecurableObjects.parse(
            "catalog.a:b:c.mytable", MetadataObject.Type.TABLE, Lists.newArrayList());
    SecurableObject stored = RelationalSchemaNamingBridge.securableObjectForStorage(obj);
    SecurableObject backToApi = RelationalSchemaNamingBridge.securableObjectForApi(stored);
    assertEquals(obj.fullName(), backToApi.fullName());
  }

  @Test
  public void customSeparatorSecurableObject() throws Exception {
    mockSeparator("/");
    SecurableObject obj =
        SecurableObjects.parse(
            "catalog.a/b.mytable", MetadataObject.Type.TABLE, Lists.newArrayList());
    SecurableObject stored = RelationalSchemaNamingBridge.securableObjectForStorage(obj);
    assertEquals("catalog.a" + P + "b.mytable", stored.fullName());
    SecurableObject backToApi = RelationalSchemaNamingBridge.securableObjectForApi(stored);
    assertEquals("catalog.a/b.mytable", backToApi.fullName());
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
  // wrapperUpdater – SCHEMA
  // -------------------------------------------------------------------------

  @Test
  public void wrapperUpdaterSchemaRoundTrip() throws Exception {
    mockSeparator(":");
    AuditInfo audit = AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build();
    SchemaEntity storageEntity =
        SchemaEntity.builder()
            .withId(1L)
            .withName("a" + P + "b")
            .withNamespace(Namespace.of("ml", "cat"))
            .withComment("c0")
            .withProperties(null)
            .withAuditInfo(audit)
            .build();

    var wrapped =
        RelationalSchemaNamingBridge.<SchemaEntity>wrapperUpdater(
            Entity.EntityType.SCHEMA,
            e ->
                SchemaEntity.builder()
                    .withId(e.id())
                    .withName(e.name())
                    .withNamespace(e.namespace())
                    .withComment("c1")
                    .withProperties(e.properties())
                    .withAuditInfo(e.auditInfo())
                    .build());

    SchemaEntity out = wrapped.apply(storageEntity);
    assertEquals("a" + P + "b", out.name());
    assertEquals("c1", out.comment());

    final String[] seenLogicalName = new String[1];
    var wrappedPassThrough =
        RelationalSchemaNamingBridge.<SchemaEntity>wrapperUpdater(
            Entity.EntityType.SCHEMA,
            e -> {
              seenLogicalName[0] = e.name();
              return e;
            });
    wrappedPassThrough.apply(storageEntity);
    assertEquals("a:b", seenLogicalName[0]);
  }

  // -------------------------------------------------------------------------
  // genericEntityMetadataFullNameForApi
  // -------------------------------------------------------------------------

  @Test
  public void genericEntityApiTable() {
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
    SecurableObject schemaObj = SecurableObjects.ofSchema(CATALOG_OBJ, "a:b", USE_SCHEMA_PRIVS);
    RoleEntity stored =
        RelationalSchemaNamingBridge.roleEntityForStorage(buildRole(Lists.newArrayList(schemaObj)));
    assertEquals("catalog.a" + P + "b", stored.securableObjects().get(0).fullName());
  }

  @Test
  public void roleWithNestedSchemaSecurableObjectToApi() {
    SecurableObject schemaObj =
        SecurableObjects.parse("catalog.a" + P + "b", MetadataObject.Type.SCHEMA, USE_SCHEMA_PRIVS);
    RoleEntity api =
        RelationalSchemaNamingBridge.roleEntityForApi(buildRole(Lists.newArrayList(schemaObj)));
    assertEquals("catalog.a:b", api.securableObjects().get(0).fullName());
  }

  @Test
  public void roleWithFlatSchemaIsUnchanged() {
    SecurableObject schemaObj = SecurableObjects.ofSchema(CATALOG_OBJ, "flat", USE_SCHEMA_PRIVS);
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
    SecurableObject schemaObj = SecurableObjects.ofSchema(CATALOG_OBJ, "a:b:c", USE_SCHEMA_PRIVS);
    RoleEntity original = buildRole(Lists.newArrayList(schemaObj));
    RoleEntity backToApi =
        RelationalSchemaNamingBridge.roleEntityForApi(
            RelationalSchemaNamingBridge.roleEntityForStorage(original));
    assertEquals(
        original.securableObjects().get(0).fullName(),
        backToApi.securableObjects().get(0).fullName());
  }

  // -------------------------------------------------------------------------
  // Helpers
  // -------------------------------------------------------------------------

  private static RoleEntity buildRole(List<SecurableObject> securableObjects) {
    AuditInfo audit = AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build();
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
