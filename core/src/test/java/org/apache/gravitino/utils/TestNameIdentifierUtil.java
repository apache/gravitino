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
package org.apache.gravitino.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.base.Joiner;
import org.apache.gravitino.Entity;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.exceptions.IllegalNameIdentifierException;
import org.apache.gravitino.exceptions.IllegalNamespaceException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestNameIdentifierUtil {

  @Test
  public void testCheckNameIdentifier() {
    NameIdentifier abc = NameIdentifier.of("a", "b", "c");
    NameIdentifier abcd = NameIdentifier.of("a", "b", "c", "d");

    // Test metalake
    assertThrows(
        IllegalNameIdentifierException.class, () -> NameIdentifierUtil.checkMetalake(null));
    Throwable excep =
        assertThrows(IllegalNamespaceException.class, () -> NameIdentifierUtil.checkMetalake(abc));
    assertTrue(excep.getMessage().contains("Metalake namespace must be non-null and empty"));

    // test catalog
    assertThrows(IllegalNameIdentifierException.class, () -> NameIdentifierUtil.checkCatalog(null));
    Throwable excep1 =
        assertThrows(IllegalNamespaceException.class, () -> NameIdentifierUtil.checkCatalog(abc));
    assertTrue(excep1.getMessage().contains("Catalog namespace must be non-null and have 1 level"));

    // test schema
    assertThrows(IllegalNameIdentifierException.class, () -> NameIdentifierUtil.checkSchema(null));
    Throwable excep2 =
        assertThrows(IllegalNamespaceException.class, () -> NameIdentifierUtil.checkSchema(abcd));
    assertTrue(excep2.getMessage().contains("Schema namespace must be non-null and have 2 levels"));

    // test table
    assertThrows(IllegalNameIdentifierException.class, () -> NameIdentifierUtil.checkTable(null));
    Throwable excep3 =
        assertThrows(IllegalNamespaceException.class, () -> NameIdentifierUtil.checkTable(abc));
    assertTrue(excep3.getMessage().contains("Table namespace must be non-null and have 3 levels"));

    // test model
    assertThrows(IllegalNameIdentifierException.class, () -> NameIdentifierUtil.checkModel(null));
    Throwable excep4 =
        assertThrows(IllegalNamespaceException.class, () -> NameIdentifierUtil.checkModel(abc));
    assertTrue(excep4.getMessage().contains("Model namespace must be non-null and have 3 levels"));

    // test model version
    assertThrows(
        IllegalNameIdentifierException.class, () -> NameIdentifierUtil.checkModelVersion(null));
    Throwable excep5 =
        assertThrows(
            IllegalNamespaceException.class, () -> NameIdentifierUtil.checkModelVersion(abcd));
    assertTrue(
        excep5.getMessage().contains("Model version namespace must be non-null and have 4 levels"));
  }

  @Test
  public void testToMetadataObject() {
    // test metalake
    NameIdentifier metalake = NameIdentifier.of("metalake1");
    MetadataObject metalakeObject =
        MetadataObjects.parse("metalake1", MetadataObject.Type.METALAKE);
    assertEquals(
        metalakeObject, NameIdentifierUtil.toMetadataObject(metalake, Entity.EntityType.METALAKE));

    // test catalog
    NameIdentifier catalog = NameIdentifier.of("metalake1", "catalog1");
    MetadataObject catalogObject = MetadataObjects.parse("catalog1", MetadataObject.Type.CATALOG);
    assertEquals(
        catalogObject, NameIdentifierUtil.toMetadataObject(catalog, Entity.EntityType.CATALOG));

    // test schema
    NameIdentifier schema = NameIdentifier.of("metalake1", "catalog1", "schema1");
    MetadataObject schemaObject =
        MetadataObjects.parse("catalog1.schema1", MetadataObject.Type.SCHEMA);
    assertEquals(
        schemaObject, NameIdentifierUtil.toMetadataObject(schema, Entity.EntityType.SCHEMA));

    // test table
    NameIdentifier table = NameIdentifier.of("metalake1", "catalog1", "schema1", "table1");
    MetadataObject tableObject =
        MetadataObjects.parse("catalog1.schema1.table1", MetadataObject.Type.TABLE);
    assertEquals(tableObject, NameIdentifierUtil.toMetadataObject(table, Entity.EntityType.TABLE));

    // test topic
    NameIdentifier topic = NameIdentifier.of("metalake1", "catalog1", "schema1", "topic1");
    MetadataObject topicObject =
        MetadataObjects.parse("catalog1.schema1.topic1", MetadataObject.Type.TOPIC);
    assertEquals(topicObject, NameIdentifierUtil.toMetadataObject(topic, Entity.EntityType.TOPIC));

    // test fileset
    NameIdentifier fileset = NameIdentifier.of("metalake1", "catalog1", "schema1", "fileset1");
    MetadataObject filesetObject =
        MetadataObjects.parse("catalog1.schema1.fileset1", MetadataObject.Type.FILESET);
    assertEquals(
        filesetObject, NameIdentifierUtil.toMetadataObject(fileset, Entity.EntityType.FILESET));

    NameIdentifier column =
        NameIdentifier.of("metalake1", "catalog1", "schema1", "table1", "column1");
    MetadataObject columnObject =
        MetadataObjects.parse("catalog1.schema1.table1.column1", MetadataObject.Type.COLUMN);
    assertEquals(
        columnObject, NameIdentifierUtil.toMetadataObject(column, Entity.EntityType.COLUMN));

    // test model
    NameIdentifier model = NameIdentifier.of("metalake1", "catalog1", "schema1", "model1");
    MetadataObject modelObject =
        MetadataObjects.parse("catalog1.schema1.model1", MetadataObject.Type.MODEL);
    assertEquals(modelObject, NameIdentifierUtil.toMetadataObject(model, Entity.EntityType.MODEL));

    // test null
    Throwable e1 =
        assertThrows(
            IllegalArgumentException.class, () -> NameIdentifierUtil.toMetadataObject(null, null));
    assertTrue(e1.getMessage().contains("The identifier and entity type must not be null"));

    // test tag
    MetadataObject expectedTagObject = MetadataObjects.parse("tag1", MetadataObject.Type.TAG);
    MetadataObject tagObject =
        NameIdentifierUtil.toMetadataObject(
            NameIdentifierUtil.ofTag("metalake1", "tag1"), Entity.EntityType.TAG);
    assertEquals(expectedTagObject, tagObject);

    // test model version
    Throwable e3 =
        assertThrows(
            IllegalArgumentException.class,
            () -> NameIdentifierUtil.toMetadataObject(model, Entity.EntityType.MODEL_VERSION));
    assertTrue(e3.getMessage().contains("Entity type MODEL_VERSION is not supported"));
  }

  @Test
  void testOfUser() {
    String userName = "userA";
    String metalake = "demo_metalake";

    NameIdentifier nameIdentifier = NameIdentifierUtil.ofUser(metalake, userName);
    Assertions.assertEquals(
        Joiner.on(".")
            .join(metalake, Entity.SYSTEM_CATALOG_RESERVED_NAME, Entity.USER_SCHEMA_NAME, userName),
        nameIdentifier.toString());
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> NameIdentifierUtil.ofUser(null, userName));
  }

  @Test
  void testOfGroup() {
    String groupName = "groupA";
    String metalake = "demo_metalake";

    NameIdentifier nameIdentifier = NameIdentifierUtil.ofGroup(metalake, groupName);
    Assertions.assertEquals(
        Joiner.on(".")
            .join(
                metalake, Entity.SYSTEM_CATALOG_RESERVED_NAME, Entity.GROUP_SCHEMA_NAME, groupName),
        nameIdentifier.toString());
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> NameIdentifierUtil.ofGroup(null, groupName));
  }

  @Test
  void testOfRole() {
    String roleName = "roleA";
    String metalake = "demo_metalake";

    NameIdentifier nameIdentifier = NameIdentifierUtil.ofRole(metalake, roleName);
    Assertions.assertEquals(
        Joiner.on(".")
            .join(metalake, Entity.SYSTEM_CATALOG_RESERVED_NAME, Entity.ROLE_SCHEMA_NAME, roleName),
        nameIdentifier.toString());
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> NameIdentifierUtil.ofRole(null, roleName));
  }

  @Test
  void testBuildNameIdentifier() {
    String metalake = "my_metalake";
    String catalog = "my_catalog";
    String schema = "my_schema";
    String tableName = "users";
    String columnName = "id";
    String userName = "john_doe";

    // Test 1: Build a METALAKE identifier
    java.util.Map<Entity.EntityType, String> metalakeEntities = new java.util.HashMap<>();
    metalakeEntities.put(Entity.EntityType.METALAKE, metalake);
    NameIdentifier metalakeIdent =
        NameIdentifierUtil.buildNameIdentifier(
            Entity.EntityType.METALAKE, metalake, metalakeEntities);
    assertEquals(NameIdentifier.of(metalake), metalakeIdent);
    assertEquals(metalake, metalakeIdent.name());

    // Test 2: Build a CATALOG identifier
    java.util.Map<Entity.EntityType, String> catalogEntities = new java.util.HashMap<>();
    catalogEntities.put(Entity.EntityType.METALAKE, metalake);
    NameIdentifier catalogIdent =
        NameIdentifierUtil.buildNameIdentifier(Entity.EntityType.CATALOG, catalog, catalogEntities);
    assertEquals(NameIdentifier.of(metalake, catalog), catalogIdent);
    assertEquals(catalog, catalogIdent.name());
    assertEquals(1, catalogIdent.namespace().length());

    // Test 3: Build a SCHEMA identifier
    java.util.Map<Entity.EntityType, String> schemaEntities = new java.util.HashMap<>();
    schemaEntities.put(Entity.EntityType.METALAKE, metalake);
    schemaEntities.put(Entity.EntityType.CATALOG, catalog);
    NameIdentifier schemaIdent =
        NameIdentifierUtil.buildNameIdentifier(Entity.EntityType.SCHEMA, schema, schemaEntities);
    assertEquals(NameIdentifier.of(metalake, catalog, schema), schemaIdent);
    assertEquals(schema, schemaIdent.name());
    assertEquals(2, schemaIdent.namespace().length());

    // Test 4: Build a TABLE identifier
    java.util.Map<Entity.EntityType, String> tableEntities = new java.util.HashMap<>();
    tableEntities.put(Entity.EntityType.METALAKE, metalake);
    tableEntities.put(Entity.EntityType.CATALOG, catalog);
    tableEntities.put(Entity.EntityType.SCHEMA, schema);
    NameIdentifier tableIdent =
        NameIdentifierUtil.buildNameIdentifier(Entity.EntityType.TABLE, tableName, tableEntities);
    assertEquals(NameIdentifier.of(metalake, catalog, schema, tableName), tableIdent);
    assertEquals(tableName, tableIdent.name());
    assertEquals(3, tableIdent.namespace().length());

    // Test 5: Build a COLUMN identifier
    java.util.Map<Entity.EntityType, String> columnEntities = new java.util.HashMap<>();
    columnEntities.put(Entity.EntityType.METALAKE, metalake);
    columnEntities.put(Entity.EntityType.CATALOG, catalog);
    columnEntities.put(Entity.EntityType.SCHEMA, schema);
    columnEntities.put(Entity.EntityType.TABLE, tableName);
    NameIdentifier columnIdent =
        NameIdentifierUtil.buildNameIdentifier(
            Entity.EntityType.COLUMN, columnName, columnEntities);
    assertEquals(NameIdentifier.of(metalake, catalog, schema, tableName, columnName), columnIdent);
    assertEquals(columnName, columnIdent.name());
    assertEquals(4, columnIdent.namespace().length());

    // Test 6: Build a USER identifier (virtual namespace type)
    java.util.Map<Entity.EntityType, String> userEntities = new java.util.HashMap<>();
    userEntities.put(Entity.EntityType.METALAKE, metalake);
    NameIdentifier userIdent =
        NameIdentifierUtil.buildNameIdentifier(Entity.EntityType.USER, userName, userEntities);
    NameIdentifier expectedUserIdent = NameIdentifierUtil.ofUser(metalake, userName);
    assertEquals(expectedUserIdent, userIdent);
    assertEquals(userName, userIdent.name());
    // Virtual namespace should have 3 levels: [metalake, "system", "user"]
    assertEquals(3, userIdent.namespace().length());
    assertEquals(metalake, userIdent.namespace().level(0));
    assertEquals(Entity.SYSTEM_CATALOG_RESERVED_NAME, userIdent.namespace().level(1));
    assertEquals(Entity.USER_SCHEMA_NAME, userIdent.namespace().level(2));

    // Test 7: Build a TAG identifier (virtual namespace type)
    String tagName = "sensitive";
    java.util.Map<Entity.EntityType, String> tagEntities = new java.util.HashMap<>();
    tagEntities.put(Entity.EntityType.METALAKE, metalake);
    NameIdentifier tagIdent =
        NameIdentifierUtil.buildNameIdentifier(Entity.EntityType.TAG, tagName, tagEntities);
    NameIdentifier expectedTagIdent = NameIdentifierUtil.ofTag(metalake, tagName);
    assertEquals(expectedTagIdent, tagIdent);
    assertEquals(tagName, tagIdent.name());

    // Test 8: Build a ROLE identifier (virtual namespace type)
    String roleName = "admin";
    java.util.Map<Entity.EntityType, String> roleEntities = new java.util.HashMap<>();
    roleEntities.put(Entity.EntityType.METALAKE, metalake);
    NameIdentifier roleIdent =
        NameIdentifierUtil.buildNameIdentifier(Entity.EntityType.ROLE, roleName, roleEntities);
    NameIdentifier expectedRoleIdent = NameIdentifierUtil.ofRole(metalake, roleName);
    assertEquals(expectedRoleIdent, roleIdent);
    assertEquals(roleName, roleIdent.name());

    // Test 9: Error case - missing parent entity (CATALOG without METALAKE)
    java.util.Map<Entity.EntityType, String> invalidEntities = new java.util.HashMap<>();
    // Missing METALAKE entry - should throw an exception about null or empty identifiers
    Throwable exception1 =
        assertThrows(
            Exception.class,
            () ->
                NameIdentifierUtil.buildNameIdentifier(
                    Entity.EntityType.CATALOG, catalog, invalidEntities));
    // The exception could be about null identifiers or missing parent, both are valid
    assertTrue(
        exception1.getMessage().contains("null")
            || exception1.getMessage().contains("empty")
            || exception1.getMessage().contains("missing parent entity of type"));

    // Test 10: Error case - missing intermediate parent (TABLE without SCHEMA)
    java.util.Map<Entity.EntityType, String> missingSchemaEntities = new java.util.HashMap<>();
    missingSchemaEntities.put(Entity.EntityType.METALAKE, metalake);
    missingSchemaEntities.put(Entity.EntityType.CATALOG, catalog);
    // Missing SCHEMA
    Throwable exception2 =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                NameIdentifierUtil.buildNameIdentifier(
                    Entity.EntityType.TABLE, tableName, missingSchemaEntities));
    assertTrue(exception2.getMessage().contains("missing parent entity of type"));
    assertTrue(exception2.getMessage().contains("SCHEMA"));

    // Test 11: Build a FILESET identifier
    String filesetName = "my_fileset";
    java.util.Map<Entity.EntityType, String> filesetEntities = new java.util.HashMap<>();
    filesetEntities.put(Entity.EntityType.METALAKE, metalake);
    filesetEntities.put(Entity.EntityType.CATALOG, catalog);
    filesetEntities.put(Entity.EntityType.SCHEMA, schema);
    NameIdentifier filesetIdent =
        NameIdentifierUtil.buildNameIdentifier(
            Entity.EntityType.FILESET, filesetName, filesetEntities);
    assertEquals(NameIdentifier.of(metalake, catalog, schema, filesetName), filesetIdent);
    assertEquals(filesetName, filesetIdent.name());

    // Test 12: Build a TOPIC identifier
    String topicName = "my_topic";
    java.util.Map<Entity.EntityType, String> topicEntities = new java.util.HashMap<>();
    topicEntities.put(Entity.EntityType.METALAKE, metalake);
    topicEntities.put(Entity.EntityType.CATALOG, catalog);
    topicEntities.put(Entity.EntityType.SCHEMA, schema);
    NameIdentifier topicIdent =
        NameIdentifierUtil.buildNameIdentifier(Entity.EntityType.TOPIC, topicName, topicEntities);
    assertEquals(NameIdentifier.of(metalake, catalog, schema, topicName), topicIdent);
    assertEquals(topicName, topicIdent.name());
  }
}
