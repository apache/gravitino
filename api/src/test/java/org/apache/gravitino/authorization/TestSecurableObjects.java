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

import com.google.common.collect.Lists;
import org.apache.gravitino.MetadataObject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestSecurableObjects {

  @Test
  public void testSecurableObjects() {

    SecurableObject metalake =
        SecurableObjects.ofMetalake(
            "metalake", Lists.newArrayList(Privileges.CreateCatalog.allow()));
    Assertions.assertEquals("metalake", metalake.fullName());
    Assertions.assertEquals(MetadataObject.Type.METALAKE, metalake.type());
    SecurableObject anotherMetalake =
        SecurableObjects.of(
            MetadataObject.Type.METALAKE,
            Lists.newArrayList("metalake"),
            Lists.newArrayList(Privileges.CreateCatalog.allow()));
    Assertions.assertEquals(metalake, anotherMetalake);

    SecurableObject catalog =
        SecurableObjects.ofCatalog("catalog", Lists.newArrayList(Privileges.UseCatalog.allow()));
    Assertions.assertEquals("catalog", catalog.fullName());
    Assertions.assertEquals(MetadataObject.Type.CATALOG, catalog.type());
    SecurableObject anotherCatalog =
        SecurableObjects.of(
            MetadataObject.Type.CATALOG,
            Lists.newArrayList("catalog"),
            Lists.newArrayList(Privileges.UseCatalog.allow()));
    Assertions.assertEquals(catalog, anotherCatalog);

    SecurableObject schema =
        SecurableObjects.ofSchema(
            catalog, "schema", Lists.newArrayList(Privileges.UseSchema.allow()));
    Assertions.assertEquals("catalog.schema", schema.fullName());
    Assertions.assertEquals(MetadataObject.Type.SCHEMA, schema.type());
    SecurableObject anotherSchema =
        SecurableObjects.of(
            MetadataObject.Type.SCHEMA,
            Lists.newArrayList("catalog", "schema"),
            Lists.newArrayList(Privileges.UseSchema.allow()));
    Assertions.assertEquals(schema, anotherSchema);

    SecurableObject table =
        SecurableObjects.ofTable(
            schema, "table", Lists.newArrayList(Privileges.SelectTable.allow()));
    Assertions.assertEquals("catalog.schema.table", table.fullName());
    Assertions.assertEquals(MetadataObject.Type.TABLE, table.type());
    SecurableObject anotherTable =
        SecurableObjects.of(
            MetadataObject.Type.TABLE,
            Lists.newArrayList("catalog", "schema", "table"),
            Lists.newArrayList(Privileges.SelectTable.allow()));
    Assertions.assertEquals(table, anotherTable);

    SecurableObject fileset =
        SecurableObjects.ofFileset(
            schema, "fileset", Lists.newArrayList(Privileges.UseSchema.allow()));
    Assertions.assertEquals("catalog.schema.fileset", fileset.fullName());
    Assertions.assertEquals(MetadataObject.Type.FILESET, fileset.type());
    SecurableObject anotherFileset =
        SecurableObjects.of(
            MetadataObject.Type.FILESET,
            Lists.newArrayList("catalog", "schema", "fileset"),
            Lists.newArrayList(Privileges.UseSchema.allow()));
    Assertions.assertEquals(fileset, anotherFileset);

    SecurableObject topic =
        SecurableObjects.ofTopic(
            schema, "topic", Lists.newArrayList(Privileges.ConsumeTopic.allow()));
    Assertions.assertEquals("catalog.schema.topic", topic.fullName());
    Assertions.assertEquals(MetadataObject.Type.TOPIC, topic.type());

    SecurableObject anotherTopic =
        SecurableObjects.of(
            MetadataObject.Type.TOPIC,
            Lists.newArrayList("catalog", "schema", "topic"),
            Lists.newArrayList(Privileges.ConsumeTopic.allow()));
    Assertions.assertEquals(topic, anotherTopic);

    Exception e =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                SecurableObjects.of(
                    MetadataObject.Type.METALAKE,
                    Lists.newArrayList("metalake", "catalog"),
                    Lists.newArrayList(Privileges.UseCatalog.allow())));
    Assertions.assertTrue(e.getMessage().contains("length of names is 2"));
    e =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                SecurableObjects.of(
                    MetadataObject.Type.CATALOG,
                    Lists.newArrayList("metalake", "catalog"),
                    Lists.newArrayList(Privileges.UseCatalog.allow())));
    Assertions.assertTrue(e.getMessage().contains("length of names is 2"));

    e =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                SecurableObjects.of(
                    MetadataObject.Type.TABLE,
                    Lists.newArrayList("metalake"),
                    Lists.newArrayList(Privileges.SelectTable.allow())));
    Assertions.assertTrue(e.getMessage().contains("the length of names is 1"));
    e =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                SecurableObjects.of(
                    MetadataObject.Type.TOPIC,
                    Lists.newArrayList("metalake"),
                    Lists.newArrayList(Privileges.ConsumeTopic.allow())));
    Assertions.assertTrue(e.getMessage().contains("the length of names is 1"));
    e =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                SecurableObjects.of(
                    MetadataObject.Type.FILESET,
                    Lists.newArrayList("metalake"),
                    Lists.newArrayList(Privileges.ReadFileset.allow())));
    Assertions.assertTrue(e.getMessage().contains("the length of names is 1"));

    e =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                SecurableObjects.of(
                    MetadataObject.Type.SCHEMA,
                    Lists.newArrayList("catalog", "schema", "table"),
                    Lists.newArrayList(Privileges.UseSchema.allow())));
    Assertions.assertTrue(e.getMessage().contains("the length of names is 3"));
  }
}
