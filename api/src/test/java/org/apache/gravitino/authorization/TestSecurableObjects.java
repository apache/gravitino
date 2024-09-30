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

  @Test
  public void testPrivileges() {
    Privilege createCatalog = Privileges.CreateCatalog.allow();
    Privilege useCatalog = Privileges.UseCatalog.allow();
    Privilege createSchema = Privileges.CreateSchema.allow();
    Privilege useSchema = Privileges.UseSchema.allow();
    Privilege createTable = Privileges.CreateTable.allow();
    Privilege selectTable = Privileges.SelectTable.allow();
    Privilege modifyTable = Privileges.ModifyTable.allow();
    Privilege createFileset = Privileges.CreateFileset.allow();
    Privilege readFileset = Privileges.ReadFileset.allow();
    Privilege writeFileset = Privileges.WriteFileset.allow();
    Privilege createTopic = Privileges.CreateTopic.allow();
    Privilege consumeTopic = Privileges.ConsumeTopic.allow();
    Privilege produceTopic = Privileges.ProduceTopic.allow();
    Privilege createRole = Privileges.CreateRole.allow();
    Privilege manageUsers = Privileges.ManageUsers.allow();
    Privilege manageGroups = Privileges.ManageGroups.allow();
    Privilege manageGrants = Privileges.ManageGrants.allow();

    // Test create catalog
    Assertions.assertTrue(createCatalog.supportsMetadataObjectType(MetadataObject.Type.METALAKE));
    Assertions.assertFalse(createCatalog.supportsMetadataObjectType(MetadataObject.Type.CATALOG));
    Assertions.assertFalse(createCatalog.supportsMetadataObjectType(MetadataObject.Type.SCHEMA));
    Assertions.assertFalse(createCatalog.supportsMetadataObjectType(MetadataObject.Type.TABLE));
    Assertions.assertFalse(createCatalog.supportsMetadataObjectType(MetadataObject.Type.TOPIC));
    Assertions.assertFalse(createCatalog.supportsMetadataObjectType(MetadataObject.Type.FILESET));
    Assertions.assertFalse(createCatalog.supportsMetadataObjectType(MetadataObject.Type.ROLE));
    Assertions.assertFalse(createCatalog.supportsMetadataObjectType(MetadataObject.Type.COLUMN));

    // Test use catalog
    Assertions.assertTrue(useCatalog.supportsMetadataObjectType(MetadataObject.Type.METALAKE));
    Assertions.assertTrue(useCatalog.supportsMetadataObjectType(MetadataObject.Type.CATALOG));
    Assertions.assertFalse(useCatalog.supportsMetadataObjectType(MetadataObject.Type.SCHEMA));
    Assertions.assertFalse(useCatalog.supportsMetadataObjectType(MetadataObject.Type.TABLE));
    Assertions.assertFalse(useCatalog.supportsMetadataObjectType(MetadataObject.Type.TOPIC));
    Assertions.assertFalse(useCatalog.supportsMetadataObjectType(MetadataObject.Type.FILESET));
    Assertions.assertFalse(useCatalog.supportsMetadataObjectType(MetadataObject.Type.ROLE));
    Assertions.assertFalse(useCatalog.supportsMetadataObjectType(MetadataObject.Type.COLUMN));

    // Test create schema
    Assertions.assertTrue(createSchema.supportsMetadataObjectType(MetadataObject.Type.METALAKE));
    Assertions.assertTrue(createSchema.supportsMetadataObjectType(MetadataObject.Type.CATALOG));
    Assertions.assertFalse(createSchema.supportsMetadataObjectType(MetadataObject.Type.SCHEMA));
    Assertions.assertFalse(createSchema.supportsMetadataObjectType(MetadataObject.Type.TABLE));
    Assertions.assertFalse(createSchema.supportsMetadataObjectType(MetadataObject.Type.TOPIC));
    Assertions.assertFalse(createSchema.supportsMetadataObjectType(MetadataObject.Type.FILESET));
    Assertions.assertFalse(createSchema.supportsMetadataObjectType(MetadataObject.Type.ROLE));
    Assertions.assertFalse(createSchema.supportsMetadataObjectType(MetadataObject.Type.COLUMN));

    // Test use schema
    Assertions.assertTrue(useSchema.supportsMetadataObjectType(MetadataObject.Type.METALAKE));
    Assertions.assertTrue(useSchema.supportsMetadataObjectType(MetadataObject.Type.CATALOG));
    Assertions.assertTrue(useSchema.supportsMetadataObjectType(MetadataObject.Type.SCHEMA));
    Assertions.assertFalse(useSchema.supportsMetadataObjectType(MetadataObject.Type.TABLE));
    Assertions.assertFalse(useSchema.supportsMetadataObjectType(MetadataObject.Type.TOPIC));
    Assertions.assertFalse(useSchema.supportsMetadataObjectType(MetadataObject.Type.FILESET));
    Assertions.assertFalse(useSchema.supportsMetadataObjectType(MetadataObject.Type.ROLE));
    Assertions.assertFalse(useSchema.supportsMetadataObjectType(MetadataObject.Type.COLUMN));

    // Test create table
    Assertions.assertTrue(createTable.supportsMetadataObjectType(MetadataObject.Type.METALAKE));
    Assertions.assertTrue(createTable.supportsMetadataObjectType(MetadataObject.Type.CATALOG));
    Assertions.assertTrue(createTable.supportsMetadataObjectType(MetadataObject.Type.SCHEMA));
    Assertions.assertFalse(createTable.supportsMetadataObjectType(MetadataObject.Type.TABLE));
    Assertions.assertFalse(createTable.supportsMetadataObjectType(MetadataObject.Type.TOPIC));
    Assertions.assertFalse(createTable.supportsMetadataObjectType(MetadataObject.Type.FILESET));
    Assertions.assertFalse(createTable.supportsMetadataObjectType(MetadataObject.Type.ROLE));
    Assertions.assertFalse(createTable.supportsMetadataObjectType(MetadataObject.Type.COLUMN));

    // Test select table
    Assertions.assertTrue(selectTable.supportsMetadataObjectType(MetadataObject.Type.METALAKE));
    Assertions.assertTrue(selectTable.supportsMetadataObjectType(MetadataObject.Type.CATALOG));
    Assertions.assertTrue(selectTable.supportsMetadataObjectType(MetadataObject.Type.SCHEMA));
    Assertions.assertTrue(selectTable.supportsMetadataObjectType(MetadataObject.Type.TABLE));
    Assertions.assertFalse(selectTable.supportsMetadataObjectType(MetadataObject.Type.TOPIC));
    Assertions.assertFalse(selectTable.supportsMetadataObjectType(MetadataObject.Type.FILESET));
    Assertions.assertFalse(selectTable.supportsMetadataObjectType(MetadataObject.Type.ROLE));
    Assertions.assertFalse(selectTable.supportsMetadataObjectType(MetadataObject.Type.COLUMN));

    // Test modify table
    Assertions.assertTrue(modifyTable.supportsMetadataObjectType(MetadataObject.Type.METALAKE));
    Assertions.assertTrue(modifyTable.supportsMetadataObjectType(MetadataObject.Type.CATALOG));
    Assertions.assertTrue(modifyTable.supportsMetadataObjectType(MetadataObject.Type.SCHEMA));
    Assertions.assertTrue(modifyTable.supportsMetadataObjectType(MetadataObject.Type.TABLE));
    Assertions.assertFalse(modifyTable.supportsMetadataObjectType(MetadataObject.Type.TOPIC));
    Assertions.assertFalse(modifyTable.supportsMetadataObjectType(MetadataObject.Type.FILESET));
    Assertions.assertFalse(modifyTable.supportsMetadataObjectType(MetadataObject.Type.ROLE));
    Assertions.assertFalse(modifyTable.supportsMetadataObjectType(MetadataObject.Type.COLUMN));

    // Test create topic
    Assertions.assertTrue(createTopic.supportsMetadataObjectType(MetadataObject.Type.METALAKE));
    Assertions.assertTrue(createTopic.supportsMetadataObjectType(MetadataObject.Type.CATALOG));
    Assertions.assertTrue(createTopic.supportsMetadataObjectType(MetadataObject.Type.SCHEMA));
    Assertions.assertFalse(createTopic.supportsMetadataObjectType(MetadataObject.Type.TABLE));
    Assertions.assertFalse(createTopic.supportsMetadataObjectType(MetadataObject.Type.TOPIC));
    Assertions.assertFalse(createTopic.supportsMetadataObjectType(MetadataObject.Type.FILESET));
    Assertions.assertFalse(createTopic.supportsMetadataObjectType(MetadataObject.Type.ROLE));
    Assertions.assertFalse(createTopic.supportsMetadataObjectType(MetadataObject.Type.COLUMN));

    // Test consume topic
    Assertions.assertTrue(consumeTopic.supportsMetadataObjectType(MetadataObject.Type.METALAKE));
    Assertions.assertTrue(consumeTopic.supportsMetadataObjectType(MetadataObject.Type.CATALOG));
    Assertions.assertTrue(consumeTopic.supportsMetadataObjectType(MetadataObject.Type.SCHEMA));
    Assertions.assertFalse(consumeTopic.supportsMetadataObjectType(MetadataObject.Type.TABLE));
    Assertions.assertTrue(consumeTopic.supportsMetadataObjectType(MetadataObject.Type.TOPIC));
    Assertions.assertFalse(consumeTopic.supportsMetadataObjectType(MetadataObject.Type.FILESET));
    Assertions.assertFalse(consumeTopic.supportsMetadataObjectType(MetadataObject.Type.ROLE));
    Assertions.assertFalse(consumeTopic.supportsMetadataObjectType(MetadataObject.Type.COLUMN));

    // Test produce topic
    Assertions.assertTrue(produceTopic.supportsMetadataObjectType(MetadataObject.Type.METALAKE));
    Assertions.assertTrue(produceTopic.supportsMetadataObjectType(MetadataObject.Type.CATALOG));
    Assertions.assertTrue(produceTopic.supportsMetadataObjectType(MetadataObject.Type.SCHEMA));
    Assertions.assertFalse(produceTopic.supportsMetadataObjectType(MetadataObject.Type.TABLE));
    Assertions.assertTrue(produceTopic.supportsMetadataObjectType(MetadataObject.Type.TOPIC));
    Assertions.assertFalse(produceTopic.supportsMetadataObjectType(MetadataObject.Type.FILESET));
    Assertions.assertFalse(produceTopic.supportsMetadataObjectType(MetadataObject.Type.ROLE));
    Assertions.assertFalse(produceTopic.supportsMetadataObjectType(MetadataObject.Type.COLUMN));

    // Test create fileset
    Assertions.assertTrue(createFileset.supportsMetadataObjectType(MetadataObject.Type.METALAKE));
    Assertions.assertTrue(createFileset.supportsMetadataObjectType(MetadataObject.Type.CATALOG));
    Assertions.assertTrue(createFileset.supportsMetadataObjectType(MetadataObject.Type.SCHEMA));
    Assertions.assertFalse(createFileset.supportsMetadataObjectType(MetadataObject.Type.TABLE));
    Assertions.assertFalse(createFileset.supportsMetadataObjectType(MetadataObject.Type.TOPIC));
    Assertions.assertFalse(createFileset.supportsMetadataObjectType(MetadataObject.Type.FILESET));
    Assertions.assertFalse(createFileset.supportsMetadataObjectType(MetadataObject.Type.ROLE));
    Assertions.assertFalse(createFileset.supportsMetadataObjectType(MetadataObject.Type.COLUMN));

    // Test read fileset
    Assertions.assertTrue(readFileset.supportsMetadataObjectType(MetadataObject.Type.METALAKE));
    Assertions.assertTrue(readFileset.supportsMetadataObjectType(MetadataObject.Type.CATALOG));
    Assertions.assertTrue(readFileset.supportsMetadataObjectType(MetadataObject.Type.SCHEMA));
    Assertions.assertFalse(readFileset.supportsMetadataObjectType(MetadataObject.Type.TABLE));
    Assertions.assertFalse(readFileset.supportsMetadataObjectType(MetadataObject.Type.TOPIC));
    Assertions.assertTrue(readFileset.supportsMetadataObjectType(MetadataObject.Type.FILESET));
    Assertions.assertFalse(readFileset.supportsMetadataObjectType(MetadataObject.Type.ROLE));
    Assertions.assertFalse(readFileset.supportsMetadataObjectType(MetadataObject.Type.COLUMN));

    // Test write fileset
    Assertions.assertTrue(writeFileset.supportsMetadataObjectType(MetadataObject.Type.METALAKE));
    Assertions.assertTrue(writeFileset.supportsMetadataObjectType(MetadataObject.Type.CATALOG));
    Assertions.assertTrue(writeFileset.supportsMetadataObjectType(MetadataObject.Type.SCHEMA));
    Assertions.assertFalse(writeFileset.supportsMetadataObjectType(MetadataObject.Type.TABLE));
    Assertions.assertFalse(writeFileset.supportsMetadataObjectType(MetadataObject.Type.TOPIC));
    Assertions.assertTrue(writeFileset.supportsMetadataObjectType(MetadataObject.Type.FILESET));
    Assertions.assertFalse(writeFileset.supportsMetadataObjectType(MetadataObject.Type.ROLE));
    Assertions.assertFalse(writeFileset.supportsMetadataObjectType(MetadataObject.Type.COLUMN));

    // Test create role
    Assertions.assertTrue(createRole.supportsMetadataObjectType(MetadataObject.Type.METALAKE));
    Assertions.assertFalse(createRole.supportsMetadataObjectType(MetadataObject.Type.CATALOG));
    Assertions.assertFalse(createRole.supportsMetadataObjectType(MetadataObject.Type.SCHEMA));
    Assertions.assertFalse(createRole.supportsMetadataObjectType(MetadataObject.Type.TABLE));
    Assertions.assertFalse(createRole.supportsMetadataObjectType(MetadataObject.Type.TOPIC));
    Assertions.assertFalse(createRole.supportsMetadataObjectType(MetadataObject.Type.FILESET));
    Assertions.assertFalse(createRole.supportsMetadataObjectType(MetadataObject.Type.ROLE));
    Assertions.assertFalse(createRole.supportsMetadataObjectType(MetadataObject.Type.COLUMN));

    // Test manager users
    Assertions.assertTrue(manageUsers.supportsMetadataObjectType(MetadataObject.Type.METALAKE));
    Assertions.assertFalse(manageUsers.supportsMetadataObjectType(MetadataObject.Type.CATALOG));
    Assertions.assertFalse(manageUsers.supportsMetadataObjectType(MetadataObject.Type.SCHEMA));
    Assertions.assertFalse(manageUsers.supportsMetadataObjectType(MetadataObject.Type.TABLE));
    Assertions.assertFalse(manageUsers.supportsMetadataObjectType(MetadataObject.Type.TOPIC));
    Assertions.assertFalse(manageUsers.supportsMetadataObjectType(MetadataObject.Type.FILESET));
    Assertions.assertFalse(manageUsers.supportsMetadataObjectType(MetadataObject.Type.ROLE));
    Assertions.assertFalse(manageUsers.supportsMetadataObjectType(MetadataObject.Type.COLUMN));

    // Test manager groups
    Assertions.assertTrue(manageGroups.supportsMetadataObjectType(MetadataObject.Type.METALAKE));
    Assertions.assertFalse(manageGroups.supportsMetadataObjectType(MetadataObject.Type.CATALOG));
    Assertions.assertFalse(manageGroups.supportsMetadataObjectType(MetadataObject.Type.SCHEMA));
    Assertions.assertFalse(manageGroups.supportsMetadataObjectType(MetadataObject.Type.TABLE));
    Assertions.assertFalse(manageGroups.supportsMetadataObjectType(MetadataObject.Type.TOPIC));
    Assertions.assertFalse(manageGroups.supportsMetadataObjectType(MetadataObject.Type.FILESET));
    Assertions.assertFalse(manageGroups.supportsMetadataObjectType(MetadataObject.Type.ROLE));
    Assertions.assertFalse(manageGroups.supportsMetadataObjectType(MetadataObject.Type.COLUMN));

    // Test manager grants
    Assertions.assertTrue(manageGrants.supportsMetadataObjectType(MetadataObject.Type.METALAKE));
    Assertions.assertFalse(manageGrants.supportsMetadataObjectType(MetadataObject.Type.CATALOG));
    Assertions.assertFalse(manageGrants.supportsMetadataObjectType(MetadataObject.Type.SCHEMA));
    Assertions.assertFalse(manageGrants.supportsMetadataObjectType(MetadataObject.Type.TABLE));
    Assertions.assertFalse(manageGrants.supportsMetadataObjectType(MetadataObject.Type.TOPIC));
    Assertions.assertFalse(manageGrants.supportsMetadataObjectType(MetadataObject.Type.FILESET));
    Assertions.assertFalse(manageGrants.supportsMetadataObjectType(MetadataObject.Type.ROLE));
    Assertions.assertFalse(manageGrants.supportsMetadataObjectType(MetadataObject.Type.COLUMN));
  }
}
