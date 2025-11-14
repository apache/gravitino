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
    Privilege createModel = Privileges.CreateModel.allow();
    Privilege createModelVersion = Privileges.CreateModelVersion.allow();
    Privilege useModel = Privileges.UseModel.allow();
    Privilege createTag = Privileges.CreateTag.allow();
    Privilege applyTag = Privileges.ApplyTag.allow();
    Privilege createPolicy = Privileges.CreatePolicy.allow();
    Privilege applyPolicy = Privileges.ApplyPolicy.allow();

    // Test create catalog
    Assertions.assertTrue(createCatalog.canBindTo(MetadataObject.Type.METALAKE));
    Assertions.assertFalse(createCatalog.canBindTo(MetadataObject.Type.CATALOG));
    Assertions.assertFalse(createCatalog.canBindTo(MetadataObject.Type.SCHEMA));
    Assertions.assertFalse(createCatalog.canBindTo(MetadataObject.Type.TABLE));
    Assertions.assertFalse(createCatalog.canBindTo(MetadataObject.Type.TOPIC));
    Assertions.assertFalse(createCatalog.canBindTo(MetadataObject.Type.FILESET));
    Assertions.assertFalse(createCatalog.canBindTo(MetadataObject.Type.ROLE));
    Assertions.assertFalse(createCatalog.canBindTo(MetadataObject.Type.COLUMN));

    // Test use catalog
    Assertions.assertTrue(useCatalog.canBindTo(MetadataObject.Type.METALAKE));
    Assertions.assertTrue(useCatalog.canBindTo(MetadataObject.Type.CATALOG));
    Assertions.assertFalse(useCatalog.canBindTo(MetadataObject.Type.SCHEMA));
    Assertions.assertFalse(useCatalog.canBindTo(MetadataObject.Type.TABLE));
    Assertions.assertFalse(useCatalog.canBindTo(MetadataObject.Type.TOPIC));
    Assertions.assertFalse(useCatalog.canBindTo(MetadataObject.Type.FILESET));
    Assertions.assertFalse(useCatalog.canBindTo(MetadataObject.Type.ROLE));
    Assertions.assertFalse(useCatalog.canBindTo(MetadataObject.Type.COLUMN));

    // Test create schema
    Assertions.assertTrue(createSchema.canBindTo(MetadataObject.Type.METALAKE));
    Assertions.assertTrue(createSchema.canBindTo(MetadataObject.Type.CATALOG));
    Assertions.assertFalse(createSchema.canBindTo(MetadataObject.Type.SCHEMA));
    Assertions.assertFalse(createSchema.canBindTo(MetadataObject.Type.TABLE));
    Assertions.assertFalse(createSchema.canBindTo(MetadataObject.Type.TOPIC));
    Assertions.assertFalse(createSchema.canBindTo(MetadataObject.Type.FILESET));
    Assertions.assertFalse(createSchema.canBindTo(MetadataObject.Type.ROLE));
    Assertions.assertFalse(createSchema.canBindTo(MetadataObject.Type.COLUMN));

    // Test use schema
    Assertions.assertTrue(useSchema.canBindTo(MetadataObject.Type.METALAKE));
    Assertions.assertTrue(useSchema.canBindTo(MetadataObject.Type.CATALOG));
    Assertions.assertTrue(useSchema.canBindTo(MetadataObject.Type.SCHEMA));
    Assertions.assertFalse(useSchema.canBindTo(MetadataObject.Type.TABLE));
    Assertions.assertFalse(useSchema.canBindTo(MetadataObject.Type.TOPIC));
    Assertions.assertFalse(useSchema.canBindTo(MetadataObject.Type.FILESET));
    Assertions.assertFalse(useSchema.canBindTo(MetadataObject.Type.ROLE));
    Assertions.assertFalse(useSchema.canBindTo(MetadataObject.Type.COLUMN));

    // Test create table
    Assertions.assertTrue(createTable.canBindTo(MetadataObject.Type.METALAKE));
    Assertions.assertTrue(createTable.canBindTo(MetadataObject.Type.CATALOG));
    Assertions.assertTrue(createTable.canBindTo(MetadataObject.Type.SCHEMA));
    Assertions.assertFalse(createTable.canBindTo(MetadataObject.Type.TABLE));
    Assertions.assertFalse(createTable.canBindTo(MetadataObject.Type.TOPIC));
    Assertions.assertFalse(createTable.canBindTo(MetadataObject.Type.FILESET));
    Assertions.assertFalse(createTable.canBindTo(MetadataObject.Type.ROLE));
    Assertions.assertFalse(createTable.canBindTo(MetadataObject.Type.COLUMN));

    // Test select table
    Assertions.assertTrue(selectTable.canBindTo(MetadataObject.Type.METALAKE));
    Assertions.assertTrue(selectTable.canBindTo(MetadataObject.Type.CATALOG));
    Assertions.assertTrue(selectTable.canBindTo(MetadataObject.Type.SCHEMA));
    Assertions.assertTrue(selectTable.canBindTo(MetadataObject.Type.TABLE));
    Assertions.assertFalse(selectTable.canBindTo(MetadataObject.Type.TOPIC));
    Assertions.assertFalse(selectTable.canBindTo(MetadataObject.Type.FILESET));
    Assertions.assertFalse(selectTable.canBindTo(MetadataObject.Type.ROLE));
    Assertions.assertFalse(selectTable.canBindTo(MetadataObject.Type.COLUMN));

    // Test modify table
    Assertions.assertTrue(modifyTable.canBindTo(MetadataObject.Type.METALAKE));
    Assertions.assertTrue(modifyTable.canBindTo(MetadataObject.Type.CATALOG));
    Assertions.assertTrue(modifyTable.canBindTo(MetadataObject.Type.SCHEMA));
    Assertions.assertTrue(modifyTable.canBindTo(MetadataObject.Type.TABLE));
    Assertions.assertFalse(modifyTable.canBindTo(MetadataObject.Type.TOPIC));
    Assertions.assertFalse(modifyTable.canBindTo(MetadataObject.Type.FILESET));
    Assertions.assertFalse(modifyTable.canBindTo(MetadataObject.Type.ROLE));
    Assertions.assertFalse(modifyTable.canBindTo(MetadataObject.Type.COLUMN));

    // Test create topic
    Assertions.assertTrue(createTopic.canBindTo(MetadataObject.Type.METALAKE));
    Assertions.assertTrue(createTopic.canBindTo(MetadataObject.Type.CATALOG));
    Assertions.assertTrue(createTopic.canBindTo(MetadataObject.Type.SCHEMA));
    Assertions.assertFalse(createTopic.canBindTo(MetadataObject.Type.TABLE));
    Assertions.assertFalse(createTopic.canBindTo(MetadataObject.Type.TOPIC));
    Assertions.assertFalse(createTopic.canBindTo(MetadataObject.Type.FILESET));
    Assertions.assertFalse(createTopic.canBindTo(MetadataObject.Type.ROLE));
    Assertions.assertFalse(createTopic.canBindTo(MetadataObject.Type.COLUMN));

    // Test consume topic
    Assertions.assertTrue(consumeTopic.canBindTo(MetadataObject.Type.METALAKE));
    Assertions.assertTrue(consumeTopic.canBindTo(MetadataObject.Type.CATALOG));
    Assertions.assertTrue(consumeTopic.canBindTo(MetadataObject.Type.SCHEMA));
    Assertions.assertFalse(consumeTopic.canBindTo(MetadataObject.Type.TABLE));
    Assertions.assertTrue(consumeTopic.canBindTo(MetadataObject.Type.TOPIC));
    Assertions.assertFalse(consumeTopic.canBindTo(MetadataObject.Type.FILESET));
    Assertions.assertFalse(consumeTopic.canBindTo(MetadataObject.Type.ROLE));
    Assertions.assertFalse(consumeTopic.canBindTo(MetadataObject.Type.COLUMN));

    // Test produce topic
    Assertions.assertTrue(produceTopic.canBindTo(MetadataObject.Type.METALAKE));
    Assertions.assertTrue(produceTopic.canBindTo(MetadataObject.Type.CATALOG));
    Assertions.assertTrue(produceTopic.canBindTo(MetadataObject.Type.SCHEMA));
    Assertions.assertFalse(produceTopic.canBindTo(MetadataObject.Type.TABLE));
    Assertions.assertTrue(produceTopic.canBindTo(MetadataObject.Type.TOPIC));
    Assertions.assertFalse(produceTopic.canBindTo(MetadataObject.Type.FILESET));
    Assertions.assertFalse(produceTopic.canBindTo(MetadataObject.Type.ROLE));
    Assertions.assertFalse(produceTopic.canBindTo(MetadataObject.Type.COLUMN));

    // Test create fileset
    Assertions.assertTrue(createFileset.canBindTo(MetadataObject.Type.METALAKE));
    Assertions.assertTrue(createFileset.canBindTo(MetadataObject.Type.CATALOG));
    Assertions.assertTrue(createFileset.canBindTo(MetadataObject.Type.SCHEMA));
    Assertions.assertFalse(createFileset.canBindTo(MetadataObject.Type.TABLE));
    Assertions.assertFalse(createFileset.canBindTo(MetadataObject.Type.TOPIC));
    Assertions.assertFalse(createFileset.canBindTo(MetadataObject.Type.FILESET));
    Assertions.assertFalse(createFileset.canBindTo(MetadataObject.Type.ROLE));
    Assertions.assertFalse(createFileset.canBindTo(MetadataObject.Type.COLUMN));

    // Test read fileset
    Assertions.assertTrue(readFileset.canBindTo(MetadataObject.Type.METALAKE));
    Assertions.assertTrue(readFileset.canBindTo(MetadataObject.Type.CATALOG));
    Assertions.assertTrue(readFileset.canBindTo(MetadataObject.Type.SCHEMA));
    Assertions.assertFalse(readFileset.canBindTo(MetadataObject.Type.TABLE));
    Assertions.assertFalse(readFileset.canBindTo(MetadataObject.Type.TOPIC));
    Assertions.assertTrue(readFileset.canBindTo(MetadataObject.Type.FILESET));
    Assertions.assertFalse(readFileset.canBindTo(MetadataObject.Type.ROLE));
    Assertions.assertFalse(readFileset.canBindTo(MetadataObject.Type.COLUMN));

    // Test write fileset
    Assertions.assertTrue(writeFileset.canBindTo(MetadataObject.Type.METALAKE));
    Assertions.assertTrue(writeFileset.canBindTo(MetadataObject.Type.CATALOG));
    Assertions.assertTrue(writeFileset.canBindTo(MetadataObject.Type.SCHEMA));
    Assertions.assertFalse(writeFileset.canBindTo(MetadataObject.Type.TABLE));
    Assertions.assertFalse(writeFileset.canBindTo(MetadataObject.Type.TOPIC));
    Assertions.assertTrue(writeFileset.canBindTo(MetadataObject.Type.FILESET));
    Assertions.assertFalse(writeFileset.canBindTo(MetadataObject.Type.ROLE));
    Assertions.assertFalse(writeFileset.canBindTo(MetadataObject.Type.COLUMN));

    // Test create role
    Assertions.assertTrue(createRole.canBindTo(MetadataObject.Type.METALAKE));
    Assertions.assertFalse(createRole.canBindTo(MetadataObject.Type.CATALOG));
    Assertions.assertFalse(createRole.canBindTo(MetadataObject.Type.SCHEMA));
    Assertions.assertFalse(createRole.canBindTo(MetadataObject.Type.TABLE));
    Assertions.assertFalse(createRole.canBindTo(MetadataObject.Type.TOPIC));
    Assertions.assertFalse(createRole.canBindTo(MetadataObject.Type.FILESET));
    Assertions.assertFalse(createRole.canBindTo(MetadataObject.Type.ROLE));
    Assertions.assertFalse(createRole.canBindTo(MetadataObject.Type.COLUMN));

    // Test manager users
    Assertions.assertTrue(manageUsers.canBindTo(MetadataObject.Type.METALAKE));
    Assertions.assertFalse(manageUsers.canBindTo(MetadataObject.Type.CATALOG));
    Assertions.assertFalse(manageUsers.canBindTo(MetadataObject.Type.SCHEMA));
    Assertions.assertFalse(manageUsers.canBindTo(MetadataObject.Type.TABLE));
    Assertions.assertFalse(manageUsers.canBindTo(MetadataObject.Type.TOPIC));
    Assertions.assertFalse(manageUsers.canBindTo(MetadataObject.Type.FILESET));
    Assertions.assertFalse(manageUsers.canBindTo(MetadataObject.Type.ROLE));
    Assertions.assertFalse(manageUsers.canBindTo(MetadataObject.Type.COLUMN));

    // Test manager groups
    Assertions.assertTrue(manageGroups.canBindTo(MetadataObject.Type.METALAKE));
    Assertions.assertFalse(manageGroups.canBindTo(MetadataObject.Type.CATALOG));
    Assertions.assertFalse(manageGroups.canBindTo(MetadataObject.Type.SCHEMA));
    Assertions.assertFalse(manageGroups.canBindTo(MetadataObject.Type.TABLE));
    Assertions.assertFalse(manageGroups.canBindTo(MetadataObject.Type.TOPIC));
    Assertions.assertFalse(manageGroups.canBindTo(MetadataObject.Type.FILESET));
    Assertions.assertFalse(manageGroups.canBindTo(MetadataObject.Type.ROLE));
    Assertions.assertFalse(manageGroups.canBindTo(MetadataObject.Type.COLUMN));

    // Test manager grants
    Assertions.assertTrue(manageGrants.canBindTo(MetadataObject.Type.METALAKE));
    Assertions.assertFalse(manageGrants.canBindTo(MetadataObject.Type.CATALOG));
    Assertions.assertFalse(manageGrants.canBindTo(MetadataObject.Type.SCHEMA));
    Assertions.assertFalse(manageGrants.canBindTo(MetadataObject.Type.TABLE));
    Assertions.assertFalse(manageGrants.canBindTo(MetadataObject.Type.TOPIC));
    Assertions.assertFalse(manageGrants.canBindTo(MetadataObject.Type.FILESET));
    Assertions.assertFalse(manageGrants.canBindTo(MetadataObject.Type.ROLE));
    Assertions.assertFalse(manageGrants.canBindTo(MetadataObject.Type.COLUMN));

    // Test create model
    Assertions.assertTrue(createModel.canBindTo(MetadataObject.Type.METALAKE));
    Assertions.assertTrue(createModel.canBindTo(MetadataObject.Type.CATALOG));
    Assertions.assertTrue(createModel.canBindTo(MetadataObject.Type.SCHEMA));
    Assertions.assertFalse(createModel.canBindTo(MetadataObject.Type.TABLE));
    Assertions.assertFalse(createModel.canBindTo(MetadataObject.Type.TOPIC));
    Assertions.assertFalse(createModel.canBindTo(MetadataObject.Type.FILESET));
    Assertions.assertFalse(createModel.canBindTo(MetadataObject.Type.ROLE));
    Assertions.assertFalse(createModel.canBindTo(MetadataObject.Type.COLUMN));
    Assertions.assertFalse(createModel.canBindTo(MetadataObject.Type.MODEL));
    // Test create model version
    Assertions.assertTrue(createModelVersion.canBindTo(MetadataObject.Type.METALAKE));
    Assertions.assertTrue(createModelVersion.canBindTo(MetadataObject.Type.CATALOG));
    Assertions.assertTrue(createModelVersion.canBindTo(MetadataObject.Type.SCHEMA));
    Assertions.assertFalse(createModelVersion.canBindTo(MetadataObject.Type.TABLE));
    Assertions.assertFalse(createModelVersion.canBindTo(MetadataObject.Type.TOPIC));
    Assertions.assertFalse(createModelVersion.canBindTo(MetadataObject.Type.FILESET));
    Assertions.assertFalse(createModelVersion.canBindTo(MetadataObject.Type.ROLE));
    Assertions.assertFalse(createModelVersion.canBindTo(MetadataObject.Type.COLUMN));
    Assertions.assertTrue(createModelVersion.canBindTo(MetadataObject.Type.MODEL));

    // Test use model
    Assertions.assertTrue(useModel.canBindTo(MetadataObject.Type.METALAKE));
    Assertions.assertTrue(useModel.canBindTo(MetadataObject.Type.CATALOG));
    Assertions.assertTrue(useModel.canBindTo(MetadataObject.Type.SCHEMA));
    Assertions.assertFalse(useModel.canBindTo(MetadataObject.Type.TABLE));
    Assertions.assertFalse(useModel.canBindTo(MetadataObject.Type.TOPIC));
    Assertions.assertFalse(useModel.canBindTo(MetadataObject.Type.FILESET));
    Assertions.assertFalse(useModel.canBindTo(MetadataObject.Type.ROLE));
    Assertions.assertFalse(useModel.canBindTo(MetadataObject.Type.COLUMN));
    Assertions.assertTrue(useModel.canBindTo(MetadataObject.Type.MODEL));

    Assertions.assertTrue(createTag.canBindTo(MetadataObject.Type.METALAKE));
    Assertions.assertFalse(createTag.canBindTo(MetadataObject.Type.CATALOG));

    Assertions.assertTrue(applyTag.canBindTo(MetadataObject.Type.METALAKE));
    Assertions.assertTrue(applyTag.canBindTo(MetadataObject.Type.TAG));
    Assertions.assertFalse(applyTag.canBindTo(MetadataObject.Type.CATALOG));
    Assertions.assertFalse(applyTag.canBindTo(MetadataObject.Type.SCHEMA));
    Assertions.assertFalse(applyTag.canBindTo(MetadataObject.Type.TABLE));
    Assertions.assertFalse(applyTag.canBindTo(MetadataObject.Type.MODEL));
    Assertions.assertFalse(useModel.canBindTo(MetadataObject.Type.TOPIC));
    Assertions.assertFalse(useModel.canBindTo(MetadataObject.Type.FILESET));
    Assertions.assertFalse(useModel.canBindTo(MetadataObject.Type.ROLE));

    Assertions.assertTrue(createPolicy.canBindTo(MetadataObject.Type.METALAKE));
    Assertions.assertFalse(createPolicy.canBindTo(MetadataObject.Type.POLICY));
    Assertions.assertFalse(createPolicy.canBindTo(MetadataObject.Type.CATALOG));
    Assertions.assertFalse(createPolicy.canBindTo(MetadataObject.Type.SCHEMA));
    Assertions.assertFalse(createPolicy.canBindTo(MetadataObject.Type.TABLE));
    Assertions.assertFalse(createPolicy.canBindTo(MetadataObject.Type.MODEL));
    Assertions.assertFalse(createPolicy.canBindTo(MetadataObject.Type.TOPIC));
    Assertions.assertFalse(createPolicy.canBindTo(MetadataObject.Type.FILESET));
    Assertions.assertFalse(createPolicy.canBindTo(MetadataObject.Type.ROLE));

    Assertions.assertTrue(applyPolicy.canBindTo(MetadataObject.Type.METALAKE));
    Assertions.assertTrue(applyPolicy.canBindTo(MetadataObject.Type.POLICY));
    Assertions.assertFalse(applyTag.canBindTo(MetadataObject.Type.CATALOG));
    Assertions.assertFalse(applyTag.canBindTo(MetadataObject.Type.SCHEMA));
    Assertions.assertFalse(applyTag.canBindTo(MetadataObject.Type.TABLE));
    Assertions.assertFalse(applyTag.canBindTo(MetadataObject.Type.MODEL));
    Assertions.assertFalse(useModel.canBindTo(MetadataObject.Type.TOPIC));
    Assertions.assertFalse(useModel.canBindTo(MetadataObject.Type.FILESET));
    Assertions.assertFalse(useModel.canBindTo(MetadataObject.Type.ROLE));
  }
}
