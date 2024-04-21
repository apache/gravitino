/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.authorization;

import static com.datastrato.gravitino.authorization.SecurableObject.SecurableObjectType.CATALOG;
import static com.datastrato.gravitino.authorization.SecurableObject.SecurableObjectType.FILESET;
import static com.datastrato.gravitino.authorization.SecurableObject.SecurableObjectType.METALAKE;
import static com.datastrato.gravitino.authorization.SecurableObject.SecurableObjectType.SCHEMA;
import static com.datastrato.gravitino.authorization.SecurableObject.SecurableObjectType.TABLE;
import static com.datastrato.gravitino.authorization.SecurableObject.SecurableObjectType.TOPIC;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestSecurableObjects {

  @Test
  public void testSecurableObjects() {
    SecurableObject allMetalakes = SecurableObjects.ofAllMetalakes();
    Assertions.assertEquals("*", allMetalakes.fullName());
    Assertions.assertEquals(METALAKE, allMetalakes.type());
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> SecurableObjects.of(METALAKE, "*"));

    SecurableObject metalake = SecurableObjects.ofMetalake("metalake");
    Assertions.assertEquals("metalake", metalake.fullName());
    Assertions.assertEquals(METALAKE, metalake.type());
    SecurableObject anotherMetalake = SecurableObjects.of(METALAKE, "metalake");
    Assertions.assertEquals(metalake, anotherMetalake);

    SecurableObject catalog = SecurableObjects.ofCatalog("catalog");
    Assertions.assertEquals("catalog", catalog.fullName());
    Assertions.assertEquals(CATALOG, catalog.type());
    SecurableObject anotherCatalog = SecurableObjects.of(CATALOG, "catalog");
    Assertions.assertEquals(catalog, anotherCatalog);

    SecurableObject schema = SecurableObjects.ofSchema(catalog, "schema");
    Assertions.assertEquals("catalog.schema", schema.fullName());
    Assertions.assertEquals(SCHEMA, schema.type());
    SecurableObject anotherSchema = SecurableObjects.of(SCHEMA, "catalog", "schema");
    Assertions.assertEquals(schema, anotherSchema);

    SecurableObject table = SecurableObjects.ofTable(schema, "table");
    Assertions.assertEquals("catalog.schema.table", table.fullName());
    Assertions.assertEquals(TABLE, table.type());
    SecurableObject anotherTable = SecurableObjects.of(TABLE, "catalog", "schema", "table");
    Assertions.assertEquals(table, anotherTable);

    SecurableObject fileset = SecurableObjects.ofFileset(schema, "fileset");
    Assertions.assertEquals("catalog.schema.fileset", fileset.fullName());
    Assertions.assertEquals(FILESET, fileset.type());
    SecurableObject anotherFileset = SecurableObjects.of(FILESET, "catalog", "schema", "fileset");
    Assertions.assertEquals(fileset, anotherFileset);

    SecurableObject topic = SecurableObjects.ofTopic(schema, "topic");
    Assertions.assertEquals("catalog.schema.topic", topic.fullName());
    Assertions.assertEquals(TOPIC, topic.type());

    SecurableObject anotherTopic = SecurableObjects.of(TOPIC, "catalog", "schema", "topic");
    Assertions.assertEquals(topic, anotherTopic);

    Exception e =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> SecurableObjects.of(METALAKE, "metalake", "catalog"));
    Assertions.assertTrue(e.getMessage().contains("length of names is 2"));
    e =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> SecurableObjects.of(CATALOG, "metalake", "catalog"));
    Assertions.assertTrue(e.getMessage().contains("length of names is 2"));

    e =
        Assertions.assertThrows(
            IllegalArgumentException.class, () -> SecurableObjects.of(TABLE, "metalake"));
    Assertions.assertTrue(e.getMessage().contains("the length of names is 1"));
    e =
        Assertions.assertThrows(
            IllegalArgumentException.class, () -> SecurableObjects.of(TOPIC, "metalake"));
    Assertions.assertTrue(e.getMessage().contains("the length of names is 1"));
    e =
        Assertions.assertThrows(
            IllegalArgumentException.class, () -> SecurableObjects.of(FILESET, "metalake"));
    Assertions.assertTrue(e.getMessage().contains("the length of names is 1"));

    e =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> SecurableObjects.of(SCHEMA, "catalog", "schema", "table"));
    Assertions.assertTrue(e.getMessage().contains("the length of names is 3"));
  }
}
