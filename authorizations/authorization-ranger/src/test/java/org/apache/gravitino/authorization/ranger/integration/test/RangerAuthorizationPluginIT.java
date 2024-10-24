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
package org.apache.gravitino.authorization.ranger.integration.test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import java.util.Arrays;
import java.util.List;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.authorization.Privilege;
import org.apache.gravitino.authorization.Privileges;
import org.apache.gravitino.authorization.SecurableObject;
import org.apache.gravitino.authorization.SecurableObjects;
import org.apache.gravitino.authorization.ranger.RangerAuthorizationPlugin;
import org.apache.gravitino.authorization.ranger.RangerMetadataObject;
import org.apache.gravitino.authorization.ranger.RangerSecurableObject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("gravitino-docker-test")
public class RangerAuthorizationPluginIT {
  private static RangerAuthorizationPlugin rangerAuthPlugin;

  @BeforeAll
  public static void setup() {
    RangerITEnv.init();
    rangerAuthPlugin = RangerITEnv.rangerAuthHivePlugin;
  }

  @Test
  public void testTranslatePrivilege() {
    SecurableObject createSchemaInMetalake =
        SecurableObjects.parse(
            String.format("metalake1"),
            MetadataObject.Type.METALAKE,
            Lists.newArrayList(Privileges.CreateSchema.allow()));
    List<RangerSecurableObject> createSchemaInMetalake1 =
        rangerAuthPlugin.translatePrivilege(createSchemaInMetalake);
    Assertions.assertEquals(1, createSchemaInMetalake1.size());
    Assertions.assertEquals("*", createSchemaInMetalake1.get(0).fullName());
    Assertions.assertEquals(
        RangerMetadataObject.Type.SCHEMA, createSchemaInMetalake1.get(0).type());

    SecurableObject createSchemaInCatalog =
        SecurableObjects.parse(
            String.format("catalog1"),
            MetadataObject.Type.CATALOG,
            Lists.newArrayList(Privileges.CreateSchema.allow()));
    List<RangerSecurableObject> createSchemaInCatalog1 =
        rangerAuthPlugin.translatePrivilege(createSchemaInCatalog);
    Assertions.assertEquals(1, createSchemaInCatalog1.size());
    Assertions.assertEquals("*", createSchemaInCatalog1.get(0).fullName());
    Assertions.assertEquals(RangerMetadataObject.Type.SCHEMA, createSchemaInCatalog1.get(0).type());

    for (Privilege privilege :
        ImmutableList.of(
            Privileges.CreateTable.allow(),
            Privileges.ModifyTable.allow(),
            Privileges.SelectTable.allow())) {
      SecurableObject metalake =
          SecurableObjects.parse(
              String.format("metalake1"),
              MetadataObject.Type.METALAKE,
              Lists.newArrayList(Privileges.CreateTable.allow()));
      List<RangerSecurableObject> metalake1 = rangerAuthPlugin.translatePrivilege(metalake);
      Assertions.assertEquals(2, metalake1.size());
      Assertions.assertEquals("*.*", metalake1.get(0).fullName());
      Assertions.assertEquals(RangerMetadataObject.Type.TABLE, metalake1.get(0).type());
      Assertions.assertEquals("*.*.*", metalake1.get(1).fullName());
      Assertions.assertEquals(RangerMetadataObject.Type.COLUMN, metalake1.get(1).type());

      SecurableObject catalog =
          SecurableObjects.parse(
              String.format("catalog1"),
              MetadataObject.Type.CATALOG,
              Lists.newArrayList(Privileges.CreateTable.allow()));
      List<RangerSecurableObject> catalog1 = rangerAuthPlugin.translatePrivilege(catalog);
      Assertions.assertEquals(2, catalog1.size());
      Assertions.assertEquals("*.*", catalog1.get(0).fullName());
      Assertions.assertEquals(RangerMetadataObject.Type.TABLE, catalog1.get(0).type());
      Assertions.assertEquals("*.*.*", catalog1.get(1).fullName());
      Assertions.assertEquals(RangerMetadataObject.Type.COLUMN, catalog1.get(1).type());

      SecurableObject schema =
          SecurableObjects.parse(
              String.format("catalog1.schema1"),
              MetadataObject.Type.SCHEMA,
              Lists.newArrayList(privilege));
      List<RangerSecurableObject> schema1 = rangerAuthPlugin.translatePrivilege(schema);
      Assertions.assertEquals(2, schema1.size());
      Assertions.assertEquals("schema1.*", schema1.get(0).fullName());
      Assertions.assertEquals(RangerMetadataObject.Type.TABLE, schema1.get(0).type());
      Assertions.assertEquals("schema1.*.*", schema1.get(1).fullName());
      Assertions.assertEquals(RangerMetadataObject.Type.COLUMN, schema1.get(1).type());

      if (!privilege.equals(Privileges.CreateTable.allow())) {
        // `CREATE_TABLE` not support securable object for table, So ignore check for table.
        SecurableObject table =
            SecurableObjects.parse(
                String.format("catalog1.schema1.table1"),
                MetadataObject.Type.TABLE,
                Lists.newArrayList(privilege));
        List<RangerSecurableObject> table1 = rangerAuthPlugin.translatePrivilege(table);
        Assertions.assertEquals(2, table1.size());
        Assertions.assertEquals("schema1.table1", table1.get(0).fullName());
        Assertions.assertEquals(RangerMetadataObject.Type.TABLE, table1.get(0).type());
        Assertions.assertEquals("schema1.table1.*", table1.get(1).fullName());
        Assertions.assertEquals(RangerMetadataObject.Type.COLUMN, table1.get(1).type());
      }
    }
  }

  @Test
  public void testTranslateOwner() {
    for (MetadataObject.Type type :
        ImmutableList.of(MetadataObject.Type.METALAKE, MetadataObject.Type.CATALOG)) {
      MetadataObject metalake = MetadataObjects.parse("metalake_or_catalog", type);
      List<RangerSecurableObject> metalakeOwner = rangerAuthPlugin.translateOwner(metalake);
      Assertions.assertEquals(3, metalakeOwner.size());
      Assertions.assertEquals("*", metalakeOwner.get(0).fullName());
      Assertions.assertEquals(RangerMetadataObject.Type.SCHEMA, metalakeOwner.get(0).type());
      Assertions.assertEquals("*.*", metalakeOwner.get(1).fullName());
      Assertions.assertEquals(RangerMetadataObject.Type.TABLE, metalakeOwner.get(1).type());
      Assertions.assertEquals("*.*.*", metalakeOwner.get(2).fullName());
      Assertions.assertEquals(RangerMetadataObject.Type.COLUMN, metalakeOwner.get(2).type());
    }

    MetadataObject schema = MetadataObjects.parse("catalog1.schema1", MetadataObject.Type.SCHEMA);
    List<RangerSecurableObject> schemaOwner = rangerAuthPlugin.translateOwner(schema);
    Assertions.assertEquals(3, schemaOwner.size());
    Assertions.assertEquals("schema1", schemaOwner.get(0).fullName());
    Assertions.assertEquals(RangerMetadataObject.Type.SCHEMA, schemaOwner.get(0).type());
    Assertions.assertEquals("schema1.*", schemaOwner.get(1).fullName());
    Assertions.assertEquals(RangerMetadataObject.Type.TABLE, schemaOwner.get(1).type());
    Assertions.assertEquals("schema1.*.*", schemaOwner.get(2).fullName());
    Assertions.assertEquals(RangerMetadataObject.Type.COLUMN, schemaOwner.get(2).type());

    MetadataObject table =
        MetadataObjects.parse("catalog1.schema1.table1", MetadataObject.Type.TABLE);
    List<RangerSecurableObject> tableOwner = rangerAuthPlugin.translateOwner(table);
    Assertions.assertEquals(2, tableOwner.size());
    Assertions.assertEquals("schema1.table1", tableOwner.get(0).fullName());
    Assertions.assertEquals(RangerMetadataObject.Type.TABLE, tableOwner.get(0).type());
    Assertions.assertEquals("schema1.table1.*", tableOwner.get(1).fullName());
    Assertions.assertEquals(RangerMetadataObject.Type.COLUMN, tableOwner.get(1).type());
  }

  @Test
  public void testValidAuthorizationOperation() {
    // Create Catalog
    SecurableObject createCatalog =
        SecurableObjects.parse(
            String.format("metalake"),
            MetadataObject.Type.METALAKE,
            Lists.newArrayList(Privileges.CreateCatalog.allow()));
    Assertions.assertTrue(
        rangerAuthPlugin.validAuthorizationOperation(Arrays.asList(createCatalog)));

    // Use catalog operation
    SecurableObject useCatalogInMetalake =
        SecurableObjects.parse(
            String.format("metalake"),
            MetadataObject.Type.METALAKE,
            Lists.newArrayList(Privileges.UseCatalog.allow()));
    Assertions.assertTrue(
        rangerAuthPlugin.validAuthorizationOperation(Arrays.asList(useCatalogInMetalake)));
    SecurableObject useCatalog =
        SecurableObjects.parse(
            String.format("catalog"),
            MetadataObject.Type.CATALOG,
            Lists.newArrayList(Privileges.UseCatalog.allow()));
    Assertions.assertTrue(rangerAuthPlugin.validAuthorizationOperation(Arrays.asList(useCatalog)));

    // Create schema
    SecurableObject createSchemaInMetalake =
        SecurableObjects.parse(
            String.format("metalake"),
            MetadataObject.Type.METALAKE,
            Lists.newArrayList(Privileges.CreateSchema.allow()));
    SecurableObject createSchemaInCatalog =
        SecurableObjects.parse(
            String.format("catalog"),
            MetadataObject.Type.CATALOG,
            Lists.newArrayList(Privileges.CreateSchema.allow()));
    Assertions.assertTrue(
        rangerAuthPlugin.validAuthorizationOperation(
            Arrays.asList(createSchemaInMetalake, createSchemaInCatalog)));

    // Use schema operation
    SecurableObject useSchema =
        SecurableObjects.parse(
            String.format("catalog.schema"),
            MetadataObject.Type.SCHEMA,
            Lists.newArrayList(Privileges.UseSchema.allow()));
    Assertions.assertTrue(rangerAuthPlugin.validAuthorizationOperation(Arrays.asList(useSchema)));

    // Table
    SecurableObject createTableInMetalake =
        SecurableObjects.parse(
            String.format("metalake"),
            MetadataObject.Type.METALAKE,
            Lists.newArrayList(Privileges.CreateTable.allow()));
    SecurableObject createTableInCatalog =
        SecurableObjects.parse(
            String.format("catalog"),
            MetadataObject.Type.CATALOG,
            Lists.newArrayList(Privileges.CreateTable.allow()));
    SecurableObject createTableInSchema =
        SecurableObjects.parse(
            String.format("catalog.schema"),
            MetadataObject.Type.SCHEMA,
            Lists.newArrayList(Privileges.CreateTable.allow()));
    Assertions.assertTrue(
        rangerAuthPlugin.validAuthorizationOperation(
            Arrays.asList(createTableInMetalake, createTableInCatalog, createTableInSchema)));

    SecurableObject modifyTableInMetalake =
        SecurableObjects.parse(
            String.format("metalake"),
            MetadataObject.Type.METALAKE,
            Lists.newArrayList(Privileges.ModifyTable.allow()));
    SecurableObject modifyTableInCatalog =
        SecurableObjects.parse(
            String.format("catalog"),
            MetadataObject.Type.CATALOG,
            Lists.newArrayList(Privileges.ModifyTable.allow()));
    SecurableObject modifyTableInSchema =
        SecurableObjects.parse(
            String.format("catalog.schema"),
            MetadataObject.Type.SCHEMA,
            Lists.newArrayList(Privileges.ModifyTable.allow()));
    SecurableObject modifyTable =
        SecurableObjects.parse(
            String.format("catalog.schema.table"),
            MetadataObject.Type.TABLE,
            Lists.newArrayList(Privileges.ModifyTable.allow()));
    Assertions.assertTrue(
        rangerAuthPlugin.validAuthorizationOperation(
            Arrays.asList(
                modifyTableInMetalake, modifyTableInCatalog, modifyTableInSchema, modifyTable)));

    SecurableObject selectTableInMetalake =
        SecurableObjects.parse(
            String.format("metalake"),
            MetadataObject.Type.METALAKE,
            Lists.newArrayList(Privileges.SelectTable.allow()));
    SecurableObject selectTableInCatalog =
        SecurableObjects.parse(
            String.format("catalog"),
            MetadataObject.Type.CATALOG,
            Lists.newArrayList(Privileges.SelectTable.allow()));
    SecurableObject selectTableInSchema =
        SecurableObjects.parse(
            String.format("catalog.schema"),
            MetadataObject.Type.SCHEMA,
            Lists.newArrayList(Privileges.SelectTable.allow()));
    SecurableObject selectTable =
        SecurableObjects.parse(
            String.format("catalog.schema.table"),
            MetadataObject.Type.TABLE,
            Lists.newArrayList(Privileges.SelectTable.allow()));
    Assertions.assertTrue(
        rangerAuthPlugin.validAuthorizationOperation(
            Arrays.asList(
                selectTableInMetalake, selectTableInCatalog, selectTableInSchema, selectTable)));

    // Ignore the Fileset operation
    SecurableObject createFilesetInMetalake =
        SecurableObjects.parse(
            String.format("metalake"),
            MetadataObject.Type.METALAKE,
            Lists.newArrayList(Privileges.CreateFileset.allow()));
    SecurableObject createFilesetInCatalog =
        SecurableObjects.parse(
            String.format("catalog"),
            MetadataObject.Type.CATALOG,
            Lists.newArrayList(Privileges.CreateFileset.allow()));
    SecurableObject createFilesetInSchema =
        SecurableObjects.parse(
            String.format("catalog.schema"),
            MetadataObject.Type.SCHEMA,
            Lists.newArrayList(Privileges.ReadFileset.allow()));
    Assertions.assertFalse(
        rangerAuthPlugin.validAuthorizationOperation(Arrays.asList(createFilesetInMetalake)));
    Assertions.assertFalse(
        rangerAuthPlugin.validAuthorizationOperation(Arrays.asList(createFilesetInCatalog)));
    Assertions.assertFalse(
        rangerAuthPlugin.validAuthorizationOperation(Arrays.asList(createFilesetInSchema)));

    // Ignore the Topic operation
    SecurableObject writeFilesetInMetalake =
        SecurableObjects.parse(
            String.format("metalake"),
            MetadataObject.Type.METALAKE,
            Lists.newArrayList(Privileges.WriteFileset.allow()));
    SecurableObject writeFilesetInCatalog =
        SecurableObjects.parse(
            String.format("catalog"),
            MetadataObject.Type.CATALOG,
            Lists.newArrayList(Privileges.WriteFileset.allow()));
    SecurableObject writeFilesetInScheam =
        SecurableObjects.parse(
            String.format("catalog.schema"),
            MetadataObject.Type.SCHEMA,
            Lists.newArrayList(Privileges.WriteFileset.allow()));
    SecurableObject writeFileset =
        SecurableObjects.parse(
            String.format("catalog.schema.fileset"),
            MetadataObject.Type.FILESET,
            Lists.newArrayList(Privileges.WriteFileset.allow()));
    Assertions.assertFalse(
        rangerAuthPlugin.validAuthorizationOperation(Arrays.asList(writeFilesetInMetalake)));
    Assertions.assertFalse(
        rangerAuthPlugin.validAuthorizationOperation(Arrays.asList(writeFilesetInCatalog)));
    Assertions.assertFalse(
        rangerAuthPlugin.validAuthorizationOperation(Arrays.asList(writeFilesetInScheam)));
    Assertions.assertFalse(
        rangerAuthPlugin.validAuthorizationOperation(Arrays.asList(writeFileset)));

    // Ignore the Fileset operation
    SecurableObject readFilesetInMetalake =
        SecurableObjects.parse(
            String.format("metalake"),
            MetadataObject.Type.METALAKE,
            Lists.newArrayList(Privileges.ReadFileset.allow()));
    SecurableObject readFilesetInCatalog =
        SecurableObjects.parse(
            String.format("catalog"),
            MetadataObject.Type.CATALOG,
            Lists.newArrayList(Privileges.ReadFileset.allow()));
    SecurableObject readFilesetInSchema =
        SecurableObjects.parse(
            String.format("catalog.schema"),
            MetadataObject.Type.SCHEMA,
            Lists.newArrayList(Privileges.ReadFileset.allow()));
    SecurableObject readFileset =
        SecurableObjects.parse(
            String.format("catalog.schema.table"),
            MetadataObject.Type.FILESET,
            Lists.newArrayList(Privileges.ReadFileset.allow()));
    Assertions.assertFalse(
        rangerAuthPlugin.validAuthorizationOperation(Arrays.asList(readFilesetInMetalake)));
    Assertions.assertFalse(
        rangerAuthPlugin.validAuthorizationOperation(Arrays.asList(readFilesetInCatalog)));
    Assertions.assertFalse(
        rangerAuthPlugin.validAuthorizationOperation(Arrays.asList(readFilesetInSchema)));
    Assertions.assertFalse(
        rangerAuthPlugin.validAuthorizationOperation(Arrays.asList(readFileset)));

    // Ignore the Topic operation
    SecurableObject createTopicInMetalake =
        SecurableObjects.parse(
            String.format("metalake"),
            MetadataObject.Type.METALAKE,
            Lists.newArrayList(Privileges.CreateTopic.allow()));
    SecurableObject createTopicInCatalog =
        SecurableObjects.parse(
            String.format("catalog"),
            MetadataObject.Type.CATALOG,
            Lists.newArrayList(Privileges.CreateTopic.allow()));
    SecurableObject createTopicInSchema =
        SecurableObjects.parse(
            String.format("catalog.schema"),
            MetadataObject.Type.SCHEMA,
            Lists.newArrayList(Privileges.CreateTopic.allow()));
    Assertions.assertFalse(
        rangerAuthPlugin.validAuthorizationOperation(Arrays.asList(createTopicInMetalake)));
    Assertions.assertFalse(
        rangerAuthPlugin.validAuthorizationOperation(Arrays.asList(createTopicInCatalog)));
    Assertions.assertFalse(
        rangerAuthPlugin.validAuthorizationOperation(Arrays.asList(createTopicInSchema)));

    SecurableObject produceTopicInMetalake =
        SecurableObjects.parse(
            String.format("metalake"),
            MetadataObject.Type.METALAKE,
            Lists.newArrayList(Privileges.ProduceTopic.allow()));
    SecurableObject produceTopicInCatalog =
        SecurableObjects.parse(
            String.format("catalog"),
            MetadataObject.Type.CATALOG,
            Lists.newArrayList(Privileges.ProduceTopic.allow()));
    SecurableObject produceTopicInSchema =
        SecurableObjects.parse(
            String.format("catalog.schema"),
            MetadataObject.Type.SCHEMA,
            Lists.newArrayList(Privileges.ProduceTopic.allow()));
    SecurableObject produceTopic =
        SecurableObjects.parse(
            String.format("catalog.schema.fileset"),
            MetadataObject.Type.TOPIC,
            Lists.newArrayList(Privileges.ProduceTopic.allow()));
    Assertions.assertFalse(
        rangerAuthPlugin.validAuthorizationOperation(Arrays.asList(produceTopicInMetalake)));
    Assertions.assertFalse(
        rangerAuthPlugin.validAuthorizationOperation(Arrays.asList(produceTopicInCatalog)));
    Assertions.assertFalse(
        rangerAuthPlugin.validAuthorizationOperation(Arrays.asList(produceTopicInSchema)));
    Assertions.assertFalse(
        rangerAuthPlugin.validAuthorizationOperation(Arrays.asList(produceTopic)));

    SecurableObject consumeTopicInMetalake =
        SecurableObjects.parse(
            String.format("metalake"),
            MetadataObject.Type.METALAKE,
            Lists.newArrayList(Privileges.ConsumeTopic.allow()));
    SecurableObject consumeTopicInCatalog =
        SecurableObjects.parse(
            String.format("catalog"),
            MetadataObject.Type.CATALOG,
            Lists.newArrayList(Privileges.ConsumeTopic.allow()));
    SecurableObject consumeTopicInSchema =
        SecurableObjects.parse(
            String.format("catalog.schema"),
            MetadataObject.Type.SCHEMA,
            Lists.newArrayList(Privileges.ConsumeTopic.allow()));
    SecurableObject consumeTopic =
        SecurableObjects.parse(
            String.format("catalog.schema.topic"),
            MetadataObject.Type.TOPIC,
            Lists.newArrayList(Privileges.ConsumeTopic.allow()));
    Assertions.assertFalse(
        rangerAuthPlugin.validAuthorizationOperation(Arrays.asList(consumeTopicInMetalake)));
    Assertions.assertFalse(
        rangerAuthPlugin.validAuthorizationOperation(Arrays.asList(consumeTopicInCatalog)));
    Assertions.assertFalse(
        rangerAuthPlugin.validAuthorizationOperation(Arrays.asList(consumeTopicInSchema)));
    Assertions.assertFalse(
        rangerAuthPlugin.validAuthorizationOperation(Arrays.asList(consumeTopic)));
  }

  @Test
  public void testInvalidAuthorizationOperation() {
    // Catalog
    SecurableObject createCatalog1 =
        SecurableObjects.parse(
            String.format("catalog"),
            MetadataObject.Type.CATALOG,
            Lists.newArrayList(Privileges.CreateCatalog.allow()));
    Assertions.assertFalse(
        rangerAuthPlugin.validAuthorizationOperation(Arrays.asList(createCatalog1)));
    SecurableObject createCatalog2 =
        SecurableObjects.parse(
            String.format("catalog.schema"),
            MetadataObject.Type.SCHEMA,
            Lists.newArrayList(Privileges.CreateCatalog.allow()));
    Assertions.assertFalse(
        rangerAuthPlugin.validAuthorizationOperation(Arrays.asList(createCatalog2)));
    SecurableObject createCatalog3 =
        SecurableObjects.parse(
            String.format("catalog.schema.table"),
            MetadataObject.Type.TABLE,
            Lists.newArrayList(Privileges.CreateCatalog.allow()));
    Assertions.assertFalse(
        rangerAuthPlugin.validAuthorizationOperation(Arrays.asList(createCatalog3)));

    SecurableObject useCatalog1 =
        SecurableObjects.parse(
            String.format("catalog.schema"),
            MetadataObject.Type.SCHEMA,
            Lists.newArrayList(Privileges.UseCatalog.allow()));
    Assertions.assertFalse(
        rangerAuthPlugin.validAuthorizationOperation(Arrays.asList(useCatalog1)));
    SecurableObject useCatalog2 =
        SecurableObjects.parse(
            String.format("catalog.schema.table"),
            MetadataObject.Type.TABLE,
            Lists.newArrayList(Privileges.UseCatalog.allow()));
    Assertions.assertFalse(
        rangerAuthPlugin.validAuthorizationOperation(Arrays.asList(useCatalog2)));

    // Schema
    SecurableObject createSchema1 =
        SecurableObjects.parse(
            String.format("catalog.schema"),
            MetadataObject.Type.SCHEMA,
            Lists.newArrayList(Privileges.CreateSchema.allow()));
    Assertions.assertFalse(
        rangerAuthPlugin.validAuthorizationOperation(Arrays.asList(createSchema1)));
    SecurableObject createSchema2 =
        SecurableObjects.parse(
            String.format("catalog.schema.table"),
            MetadataObject.Type.TABLE,
            Lists.newArrayList(Privileges.CreateSchema.allow()));
    Assertions.assertFalse(
        rangerAuthPlugin.validAuthorizationOperation(Arrays.asList(createSchema2)));

    SecurableObject useSchema1 =
        SecurableObjects.parse(
            String.format("catalog.schema.table"),
            MetadataObject.Type.TABLE,
            Lists.newArrayList(Privileges.UseSchema.allow()));
    Assertions.assertFalse(rangerAuthPlugin.validAuthorizationOperation(Arrays.asList(useSchema1)));

    // Table
    SecurableObject createTable1 =
        SecurableObjects.parse(
            String.format("catalog.schema.table"),
            MetadataObject.Type.TABLE,
            Lists.newArrayList(Privileges.CreateTable.allow()));
    Assertions.assertFalse(
        rangerAuthPlugin.validAuthorizationOperation(Arrays.asList(createTable1)));
    SecurableObject createTable2 =
        SecurableObjects.parse(
            String.format("catalog.schema.fileset"),
            MetadataObject.Type.FILESET,
            Lists.newArrayList(Privileges.CreateTable.allow()));
    Assertions.assertFalse(
        rangerAuthPlugin.validAuthorizationOperation(Arrays.asList(createTable2)));
    SecurableObject createTable3 =
        SecurableObjects.parse(
            String.format("catalog.schema.topic"),
            MetadataObject.Type.TOPIC,
            Lists.newArrayList(Privileges.CreateTable.allow()));
    Assertions.assertFalse(
        rangerAuthPlugin.validAuthorizationOperation(Arrays.asList(createTable3)));

    SecurableObject modifyTable1 =
        SecurableObjects.parse(
            String.format("catalog.schema.fileset"),
            MetadataObject.Type.FILESET,
            Lists.newArrayList(Privileges.ModifyTable.allow()));
    Assertions.assertFalse(
        rangerAuthPlugin.validAuthorizationOperation(Arrays.asList(modifyTable1)));
    SecurableObject modifyTable2 =
        SecurableObjects.parse(
            String.format("catalog.schema.topic"),
            MetadataObject.Type.TOPIC,
            Lists.newArrayList(Privileges.ModifyTable.allow()));
    Assertions.assertFalse(
        rangerAuthPlugin.validAuthorizationOperation(Arrays.asList(modifyTable2)));

    SecurableObject selectTable1 =
        SecurableObjects.parse(
            String.format("catalog.schema.fileset"),
            MetadataObject.Type.FILESET,
            Lists.newArrayList(Privileges.SelectTable.allow()));
    Assertions.assertFalse(
        rangerAuthPlugin.validAuthorizationOperation(Arrays.asList(selectTable1)));
    SecurableObject selectTable2 =
        SecurableObjects.parse(
            String.format("catalog.schema.topic"),
            MetadataObject.Type.TOPIC,
            Lists.newArrayList(Privileges.SelectTable.allow()));
    Assertions.assertFalse(
        rangerAuthPlugin.validAuthorizationOperation(Arrays.asList(selectTable2)));

    // Topic
    SecurableObject createTopic1 =
        SecurableObjects.parse(
            String.format("catalog.schema.table"),
            MetadataObject.Type.TABLE,
            Lists.newArrayList(Privileges.CreateTopic.allow()));
    Assertions.assertFalse(
        rangerAuthPlugin.validAuthorizationOperation(Arrays.asList(createTopic1)));
    SecurableObject createTopic2 =
        SecurableObjects.parse(
            String.format("catalog.schema.topic"),
            MetadataObject.Type.TOPIC,
            Lists.newArrayList(Privileges.CreateTopic.allow()));
    Assertions.assertFalse(
        rangerAuthPlugin.validAuthorizationOperation(Arrays.asList(createTopic2)));
    SecurableObject createTopic3 =
        SecurableObjects.parse(
            String.format("catalog.schema.fileset"),
            MetadataObject.Type.FILESET,
            Lists.newArrayList(Privileges.CreateTopic.allow()));
    Assertions.assertFalse(
        rangerAuthPlugin.validAuthorizationOperation(Arrays.asList(createTopic3)));
    SecurableObject produceTopic1 =
        SecurableObjects.parse(
            String.format("catalog.schema.fileset"),
            MetadataObject.Type.FILESET,
            Lists.newArrayList(Privileges.ProduceTopic.allow()));
    Assertions.assertFalse(
        rangerAuthPlugin.validAuthorizationOperation(Arrays.asList(produceTopic1)));
    SecurableObject produceTopic2 =
        SecurableObjects.parse(
            String.format("catalog.schema.table"),
            MetadataObject.Type.TABLE,
            Lists.newArrayList(Privileges.ProduceTopic.allow()));
    Assertions.assertFalse(
        rangerAuthPlugin.validAuthorizationOperation(Arrays.asList(produceTopic2)));

    SecurableObject consumeTopic1 =
        SecurableObjects.parse(
            String.format("catalog.schema.fileset"),
            MetadataObject.Type.FILESET,
            Lists.newArrayList(Privileges.ConsumeTopic.allow()));
    Assertions.assertFalse(
        rangerAuthPlugin.validAuthorizationOperation(Arrays.asList(consumeTopic1)));
    SecurableObject consumeTopic2 =
        SecurableObjects.parse(
            String.format("catalog.schema.table"),
            MetadataObject.Type.TABLE,
            Lists.newArrayList(Privileges.ConsumeTopic.allow()));
    Assertions.assertFalse(
        rangerAuthPlugin.validAuthorizationOperation(Arrays.asList(consumeTopic2)));

    // Fileset
    SecurableObject createFileset1 =
        SecurableObjects.parse(
            String.format("catalog.schema.table"),
            MetadataObject.Type.TABLE,
            Lists.newArrayList(Privileges.CreateFileset.allow()));
    Assertions.assertFalse(
        rangerAuthPlugin.validAuthorizationOperation(Arrays.asList(createFileset1)));
    SecurableObject createFileset2 =
        SecurableObjects.parse(
            String.format("catalog.schema.topic"),
            MetadataObject.Type.TOPIC,
            Lists.newArrayList(Privileges.CreateTopic.allow()));
    Assertions.assertFalse(
        rangerAuthPlugin.validAuthorizationOperation(Arrays.asList(createFileset2)));
    SecurableObject createFileset3 =
        SecurableObjects.parse(
            String.format("catalog.schema.fileset"),
            MetadataObject.Type.FILESET,
            Lists.newArrayList(Privileges.CreateTopic.allow()));
    Assertions.assertFalse(
        rangerAuthPlugin.validAuthorizationOperation(Arrays.asList(createFileset3)));
    SecurableObject writeFileset1 =
        SecurableObjects.parse(
            String.format("catalog.schema.table"),
            MetadataObject.Type.TABLE,
            Lists.newArrayList(Privileges.WriteFileset.allow()));
    Assertions.assertFalse(
        rangerAuthPlugin.validAuthorizationOperation(Arrays.asList(writeFileset1)));
    SecurableObject writeFileset2 =
        SecurableObjects.parse(
            String.format("catalog.schema.topic"),
            MetadataObject.Type.TOPIC,
            Lists.newArrayList(Privileges.WriteFileset.allow()));
    Assertions.assertFalse(
        rangerAuthPlugin.validAuthorizationOperation(Arrays.asList(writeFileset2)));

    SecurableObject readFileset1 =
        SecurableObjects.parse(
            String.format("catalog.schema.table"),
            MetadataObject.Type.TABLE,
            Lists.newArrayList(Privileges.ReadFileset.allow()));
    Assertions.assertFalse(
        rangerAuthPlugin.validAuthorizationOperation(Arrays.asList(readFileset1)));
    SecurableObject readFileset2 =
        SecurableObjects.parse(
            String.format("catalog.schema.topic"),
            MetadataObject.Type.TOPIC,
            Lists.newArrayList(Privileges.ReadFileset.allow()));
    Assertions.assertFalse(
        rangerAuthPlugin.validAuthorizationOperation(Arrays.asList(readFileset2)));
  }
}
