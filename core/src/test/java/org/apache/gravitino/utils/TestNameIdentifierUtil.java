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
}
