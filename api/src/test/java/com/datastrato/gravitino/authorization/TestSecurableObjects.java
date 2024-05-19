/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.authorization;

import static com.datastrato.gravitino.authorization.SecurableObject.Type.CATALOG;
import static com.datastrato.gravitino.authorization.SecurableObject.Type.FILESET;
import static com.datastrato.gravitino.authorization.SecurableObject.Type.METALAKE;
import static com.datastrato.gravitino.authorization.SecurableObject.Type.SCHEMA;
import static com.datastrato.gravitino.authorization.SecurableObject.Type.TABLE;
import static com.datastrato.gravitino.authorization.SecurableObject.Type.TOPIC;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestSecurableObjects {

  @Test
  public void testSecurableObjects() {
    SecurableObject allMetalakes =
        SecurableObjects.ofAllMetalakes(Lists.newArrayList(Privileges.CreateMetalake.allow()));
    Assertions.assertEquals("*", allMetalakes.fullName());
    Assertions.assertEquals(METALAKE, allMetalakes.type());

    SecurableObject metalake =
        SecurableObjects.ofMetalake("metalake", Lists.newArrayList(Privileges.UseMetalake.allow()));
    Assertions.assertEquals("metalake", metalake.fullName());
    Assertions.assertEquals(METALAKE, metalake.type());
    SecurableObject anotherMetalake =
        SecurableObjects.of(
            METALAKE,
            Lists.newArrayList("metalake"),
            Lists.newArrayList(Privileges.UseMetalake.allow()));
    Assertions.assertEquals(metalake, anotherMetalake);

    SecurableObject catalog =
        SecurableObjects.ofCatalog("catalog", Lists.newArrayList(Privileges.UseCatalog.allow()));
    Assertions.assertEquals("catalog", catalog.fullName());
    Assertions.assertEquals(CATALOG, catalog.type());
    SecurableObject anotherCatalog =
        SecurableObjects.of(
            CATALOG,
            Lists.newArrayList("catalog"),
            Lists.newArrayList(Privileges.UseCatalog.allow()));
    Assertions.assertEquals(catalog, anotherCatalog);

    SecurableObject schema =
        SecurableObjects.ofSchema(
            catalog.fullName(), "schema", Lists.newArrayList(Privileges.UseSchema.allow()));
    Assertions.assertEquals("catalog.schema", schema.fullName());
    Assertions.assertEquals(SCHEMA, schema.type());
    SecurableObject anotherSchema =
        SecurableObjects.of(
            SCHEMA,
            Lists.newArrayList("catalog", "schema"),
            Lists.newArrayList(Privileges.UseSchema.allow()));
    Assertions.assertEquals(schema, anotherSchema);

    SecurableObject table =
        SecurableObjects.ofTable(
            schema.fullName(), "table", Lists.newArrayList(Privileges.ReadTable.allow()));
    Assertions.assertEquals("catalog.schema.table", table.fullName());
    Assertions.assertEquals(TABLE, table.type());
    SecurableObject anotherTable =
        SecurableObjects.of(
            TABLE,
            Lists.newArrayList("catalog", "schema", "table"),
            Lists.newArrayList(Privileges.ReadTable.allow()));
    Assertions.assertEquals(table, anotherTable);

    SecurableObject fileset =
        SecurableObjects.ofFileset(
            schema.fullName(), "fileset", Lists.newArrayList(Privileges.UseSchema.allow()));
    Assertions.assertEquals("catalog.schema.fileset", fileset.fullName());
    Assertions.assertEquals(FILESET, fileset.type());
    SecurableObject anotherFileset =
        SecurableObjects.of(
            FILESET,
            Lists.newArrayList("catalog", "schema", "fileset"),
            Lists.newArrayList(Privileges.UseSchema.allow()));
    Assertions.assertEquals(fileset, anotherFileset);

    SecurableObject topic =
        SecurableObjects.ofTopic(
            schema.fullName(), "topic", Lists.newArrayList(Privileges.ReadTopic.allow()));
    Assertions.assertEquals("catalog.schema.topic", topic.fullName());
    Assertions.assertEquals(TOPIC, topic.type());

    SecurableObject anotherTopic =
        SecurableObjects.of(
            TOPIC,
            Lists.newArrayList("catalog", "schema", "topic"),
            Lists.newArrayList(Privileges.ReadTopic.allow()));
    Assertions.assertEquals(topic, anotherTopic);

    Exception e =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                SecurableObjects.of(
                    METALAKE,
                    Lists.newArrayList("metalake", "catalog"),
                    Lists.newArrayList(Privileges.UseCatalog.allow())));
    Assertions.assertTrue(e.getMessage().contains("length of names is 2"));
    e =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                SecurableObjects.of(
                    CATALOG,
                    Lists.newArrayList("metalake", "catalog"),
                    Lists.newArrayList(Privileges.UseCatalog.allow())));
    Assertions.assertTrue(e.getMessage().contains("length of names is 2"));

    e =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                SecurableObjects.of(
                    TABLE,
                    Lists.newArrayList("metalake"),
                    Lists.newArrayList(Privileges.ReadTable.allow())));
    Assertions.assertTrue(e.getMessage().contains("the length of names is 1"));
    e =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                SecurableObjects.of(
                    TOPIC,
                    Lists.newArrayList("metalake"),
                    Lists.newArrayList(Privileges.ReadTopic.allow())));
    Assertions.assertTrue(e.getMessage().contains("the length of names is 1"));
    e =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                SecurableObjects.of(
                    FILESET,
                    Lists.newArrayList("metalake"),
                    Lists.newArrayList(Privileges.ReadFileset.allow())));
    Assertions.assertTrue(e.getMessage().contains("the length of names is 1"));

    e =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                SecurableObjects.of(
                    SCHEMA,
                    Lists.newArrayList("catalog", "schema", "table"),
                    Lists.newArrayList(Privileges.UseSchema.allow())));
    Assertions.assertTrue(e.getMessage().contains("the length of names is 3"));
  }
}
