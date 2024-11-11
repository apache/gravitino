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

import org.apache.gravitino.Entity;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.exceptions.IllegalNameIdentifierException;
import org.apache.gravitino.exceptions.IllegalNamespaceException;
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

    // test null
    Throwable e1 =
        assertThrows(
            IllegalArgumentException.class, () -> NameIdentifierUtil.toMetadataObject(null, null));
    assertTrue(e1.getMessage().contains("The identifier and entity type must not be null"));

    // test unsupported
    Throwable e2 =
        assertThrows(
            IllegalArgumentException.class,
            () -> NameIdentifierUtil.toMetadataObject(fileset, Entity.EntityType.TAG));
    assertTrue(e2.getMessage().contains("Entity type TAG is not supported"));
  }
}
