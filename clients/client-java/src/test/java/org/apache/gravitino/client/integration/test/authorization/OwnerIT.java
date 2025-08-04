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
package org.apache.gravitino.client.integration.test.authorization;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.util.Collections;
import java.util.Map;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.Configs;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.auth.AuthConstants;
import org.apache.gravitino.authorization.Owner;
import org.apache.gravitino.authorization.Privileges;
import org.apache.gravitino.authorization.SecurableObject;
import org.apache.gravitino.authorization.SecurableObjects;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.exceptions.NoSuchMetadataObjectException;
import org.apache.gravitino.exceptions.NotFoundException;
import org.apache.gravitino.file.Fileset;
import org.apache.gravitino.integration.test.container.ContainerSuite;
import org.apache.gravitino.integration.test.container.HiveContainer;
import org.apache.gravitino.integration.test.container.KafkaContainer;
import org.apache.gravitino.integration.test.util.BaseIT;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.types.Types;
import org.apache.gravitino.utils.RandomNameUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Tag("gravitino-docker-test")
public class OwnerIT extends BaseIT {

  private static final Logger LOG = LoggerFactory.getLogger(OwnerIT.class);

  private static final ContainerSuite containerSuite = ContainerSuite.getInstance();
  private static String hmsUri;
  private static String kafkaBootstrapServers;

  @BeforeAll
  public void startIntegrationTest() throws Exception {
    Map<String, String> configs = Maps.newHashMap();
    configs.put(Configs.ENABLE_AUTHORIZATION.getKey(), String.valueOf(true));
    configs.put(Configs.SERVICE_ADMINS.getKey(), AuthConstants.ANONYMOUS_USER);
    configs.put(
        Configs.AUTHORIZATION_IMPL.getKey(),
        "org.apache.gravitino.server.authorization.PassThroughAuthorizer");
    registerCustomConfigs(configs);
    super.startIntegrationTest();

    containerSuite.startHiveContainer();
    hmsUri =
        String.format(
            "thrift://%s:%d",
            containerSuite.getHiveContainer().getContainerIpAddress(),
            HiveContainer.HIVE_METASTORE_PORT);

    containerSuite.startKafkaContainer();
    kafkaBootstrapServers =
        String.format(
            "%s:%d",
            containerSuite.getKafkaContainer().getContainerIpAddress(),
            KafkaContainer.DEFAULT_BROKER_PORT);
  }

  @AfterAll
  public void tearDown() {
    if (client != null) {
      client.close();
      client = null;
    }

    try {
      closer.close();
    } catch (Exception e) {
      LOG.error("Exception in closing CloseableGroup", e);
    }
  }

  @Test
  public void testCreateFileset() {
    String metalakeNameA = RandomNameUtils.genRandomName("metalakeA");
    client.createMetalake(metalakeNameA, "metalake A comment", Collections.emptyMap());
    GravitinoMetalake metalake = client.loadMetalake(metalakeNameA);
    String catalogNameA = RandomNameUtils.genRandomName("catalogA");
    Catalog catalog =
        metalake.createCatalog(
            catalogNameA, Catalog.Type.FILESET, "hadoop", "comment", Collections.emptyMap());
    NameIdentifier fileIdent = NameIdentifier.of("schema_owner", "fileset_owner");
    catalog.asSchemas().createSchema("schema_owner", "comment", Collections.emptyMap());
    catalog
        .asFilesetCatalog()
        .createFileset(fileIdent, "comment", Fileset.Type.EXTERNAL, "tmp", Collections.emptyMap());

    MetadataObject metalakeObject =
        MetadataObjects.of(null, metalakeNameA, MetadataObject.Type.METALAKE);
    Owner owner = metalake.getOwner(metalakeObject).get();
    Assertions.assertEquals(AuthConstants.ANONYMOUS_USER, owner.name());
    Assertions.assertEquals(Owner.Type.USER, owner.type());

    // Set another owner
    String anotherUser = "another";
    metalake.addUser(anotherUser);
    metalake.setOwner(metalakeObject, anotherUser, Owner.Type.USER);
    owner = metalake.getOwner(metalakeObject).get();
    Assertions.assertEquals(anotherUser, owner.name());
    Assertions.assertEquals(Owner.Type.USER, owner.type());

    MetadataObject catalogObject =
        MetadataObjects.of(Lists.newArrayList(catalogNameA), MetadataObject.Type.CATALOG);
    owner = metalake.getOwner(catalogObject).get();
    Assertions.assertEquals(AuthConstants.ANONYMOUS_USER, owner.name());
    Assertions.assertEquals(Owner.Type.USER, owner.type());

    // Set another owner
    metalake.setOwner(catalogObject, anotherUser, Owner.Type.USER);
    owner = metalake.getOwner(catalogObject).get();
    Assertions.assertEquals(anotherUser, owner.name());
    Assertions.assertEquals(Owner.Type.USER, owner.type());

    MetadataObject schemaObject =
        MetadataObjects.of(
            Lists.newArrayList(catalogNameA, "schema_owner"), MetadataObject.Type.SCHEMA);
    owner = metalake.getOwner(schemaObject).get();
    Assertions.assertEquals(AuthConstants.ANONYMOUS_USER, owner.name());
    Assertions.assertEquals(Owner.Type.USER, owner.type());

    // Set another owner
    metalake.setOwner(schemaObject, anotherUser, Owner.Type.USER);
    owner = metalake.getOwner(schemaObject).get();
    Assertions.assertEquals(anotherUser, owner.name());
    Assertions.assertEquals(Owner.Type.USER, owner.type());

    MetadataObject filesetObject =
        MetadataObjects.of(
            Lists.newArrayList(catalogNameA, "schema_owner", "fileset_owner"),
            MetadataObject.Type.FILESET);
    owner = metalake.getOwner(filesetObject).get();
    Assertions.assertEquals(AuthConstants.ANONYMOUS_USER, owner.name());
    Assertions.assertEquals(Owner.Type.USER, owner.type());

    // Set another owner
    metalake.setOwner(filesetObject, anotherUser, Owner.Type.USER);
    owner = metalake.getOwner(catalogObject).get();
    Assertions.assertEquals(anotherUser, owner.name());
    Assertions.assertEquals(Owner.Type.USER, owner.type());

    // Clean up
    catalog.asFilesetCatalog().dropFileset(fileIdent);
    catalog.asSchemas().dropSchema("schema_owner", true);
    metalake.dropCatalog(catalogNameA, true);
    client.dropMetalake(metalakeNameA, true);
  }

  @Test
  public void testCreateTopic() {
    String metalakeNameB = RandomNameUtils.genRandomName("metalakeB");
    GravitinoMetalake metalake =
        client.createMetalake(metalakeNameB, "metalake B comment", Collections.emptyMap());
    String catalogNameB = RandomNameUtils.genRandomName("catalogB");
    Map<String, String> properties = Maps.newHashMap();
    properties.put("bootstrap.servers", kafkaBootstrapServers);
    Catalog catalogB =
        metalake.createCatalog(
            catalogNameB, Catalog.Type.MESSAGING, "kafka", "comment", properties);
    NameIdentifier topicIdent = NameIdentifier.of("default", "topic_owner");
    catalogB.asTopicCatalog().createTopic(topicIdent, "comment", null, Collections.emptyMap());

    MetadataObject metalakeObject =
        MetadataObjects.of(null, metalakeNameB, MetadataObject.Type.METALAKE);
    Owner owner = metalake.getOwner(metalakeObject).get();
    Assertions.assertEquals(AuthConstants.ANONYMOUS_USER, owner.name());
    Assertions.assertEquals(Owner.Type.USER, owner.type());

    MetadataObject catalogObject =
        MetadataObjects.of(Lists.newArrayList(catalogNameB), MetadataObject.Type.CATALOG);
    owner = metalake.getOwner(catalogObject).get();
    Assertions.assertEquals(AuthConstants.ANONYMOUS_USER, owner.name());
    Assertions.assertEquals(Owner.Type.USER, owner.type());

    MetadataObject schemaObject =
        MetadataObjects.of(Lists.newArrayList(catalogNameB, "default"), MetadataObject.Type.SCHEMA);
    Assertions.assertFalse(metalake.getOwner(schemaObject).isPresent());

    MetadataObject topicObject =
        MetadataObjects.of(
            Lists.newArrayList(catalogNameB, "default", "topic_owner"), MetadataObject.Type.TOPIC);
    owner = metalake.getOwner(topicObject).get();
    Assertions.assertEquals(AuthConstants.ANONYMOUS_USER, owner.name());
    Assertions.assertEquals(Owner.Type.USER, owner.type());

    // Set another owner
    String anotherUser = "another";
    metalake.addUser(anotherUser);
    metalake.setOwner(topicObject, anotherUser, Owner.Type.USER);
    owner = metalake.getOwner(topicObject).get();
    Assertions.assertEquals(anotherUser, owner.name());
    Assertions.assertEquals(Owner.Type.USER, owner.type());

    // Clean up
    catalogB.asTopicCatalog().dropTopic(topicIdent);
    metalake.dropCatalog(catalogNameB, true);
    client.dropMetalake(metalakeNameB, true);
  }

  @Test
  public void testCreateRole() {
    String metalakeNameC = RandomNameUtils.genRandomName("metalakeC");
    GravitinoMetalake metalake =
        client.createMetalake(metalakeNameC, "metalake C comment", Collections.emptyMap());
    SecurableObject metalakeSecObject =
        SecurableObjects.ofMetalake(
            metalakeNameC, Lists.newArrayList(Privileges.CreateCatalog.allow()));
    metalake.createRole(
        "role_owner", Collections.emptyMap(), Lists.newArrayList(metalakeSecObject));

    MetadataObject metalakeObject =
        MetadataObjects.of(null, metalakeNameC, MetadataObject.Type.METALAKE);
    Owner owner = metalake.getOwner(metalakeObject).get();
    Assertions.assertEquals(AuthConstants.ANONYMOUS_USER, owner.name());
    Assertions.assertEquals(Owner.Type.USER, owner.type());

    MetadataObject roleObject = MetadataObjects.of(null, "role_owner", MetadataObject.Type.ROLE);
    owner = metalake.getOwner(roleObject).get();
    Assertions.assertEquals(AuthConstants.ANONYMOUS_USER, owner.name());
    Assertions.assertEquals(Owner.Type.USER, owner.type());

    // Set another owner
    String anotherUser = "another";
    metalake.addUser(anotherUser);
    metalake.setOwner(roleObject, anotherUser, Owner.Type.USER);
    owner = metalake.getOwner(roleObject).get();
    Assertions.assertEquals(anotherUser, owner.name());
    Assertions.assertEquals(Owner.Type.USER, owner.type());

    // Clean up
    metalake.deleteRole("role_owner");
    client.dropMetalake(metalakeNameC, true);
  }

  @Test
  public void testCreateTable() {
    String metalakeNameD = RandomNameUtils.genRandomName("metalakeD");
    GravitinoMetalake metalake =
        client.createMetalake(metalakeNameD, "metalake D comment", Collections.emptyMap());
    String catalogNameD = RandomNameUtils.genRandomName("catalogD");
    Map<String, String> properties = Maps.newHashMap();
    properties.put("metastore.uris", hmsUri);
    Catalog catalog =
        metalake.createCatalog(
            catalogNameD, Catalog.Type.RELATIONAL, "hive", "catalog comment", properties);

    NameIdentifier tableIdent = NameIdentifier.of("schema_owner", "table_owner");
    catalog.asSchemas().createSchema("schema_owner", "comment", Collections.emptyMap());
    catalog
        .asTableCatalog()
        .createTable(
            tableIdent,
            new Column[] {
              Column.of("col1", Types.IntegerType.get()), Column.of("col2", Types.StringType.get())
            },
            "comment",
            Collections.emptyMap());

    MetadataObject metalakeObject =
        MetadataObjects.of(null, metalakeNameD, MetadataObject.Type.METALAKE);
    Owner owner = metalake.getOwner(metalakeObject).get();
    Assertions.assertEquals(AuthConstants.ANONYMOUS_USER, owner.name());
    Assertions.assertEquals(Owner.Type.USER, owner.type());

    MetadataObject catalogObject =
        MetadataObjects.of(Lists.newArrayList(catalogNameD), MetadataObject.Type.CATALOG);
    owner = metalake.getOwner(catalogObject).get();
    Assertions.assertEquals(AuthConstants.ANONYMOUS_USER, owner.name());
    Assertions.assertEquals(Owner.Type.USER, owner.type());

    MetadataObject schemaObject =
        MetadataObjects.of(
            Lists.newArrayList(catalogNameD, "schema_owner"), MetadataObject.Type.SCHEMA);
    owner = metalake.getOwner(schemaObject).get();
    Assertions.assertEquals(AuthConstants.ANONYMOUS_USER, owner.name());
    Assertions.assertEquals(Owner.Type.USER, owner.type());

    MetadataObject tableObject =
        MetadataObjects.of(
            Lists.newArrayList(catalogNameD, "schema_owner", "table_owner"),
            MetadataObject.Type.TABLE);
    owner = metalake.getOwner(tableObject).get();
    Assertions.assertEquals(AuthConstants.ANONYMOUS_USER, owner.name());
    Assertions.assertEquals(Owner.Type.USER, owner.type());

    // Set another owner
    String anotherUser = "another";
    metalake.addUser(anotherUser);
    metalake.setOwner(tableObject, anotherUser, Owner.Type.USER);
    owner = metalake.getOwner(tableObject).get();
    Assertions.assertEquals(anotherUser, owner.name());
    Assertions.assertEquals(Owner.Type.USER, owner.type());

    // Clean up
    catalog.asTableCatalog().dropTable(tableIdent);
    catalog.asSchemas().dropSchema("schema_owner", true);
    metalake.dropCatalog(catalogNameD, true);
    client.dropMetalake(metalakeNameD, true);
  }

  @Test
  public void testCreateModel() {
    String metalakeNameF = RandomNameUtils.genRandomName("metalakeF");
    GravitinoMetalake metalake =
        client.createMetalake(metalakeNameF, "metalake F comment", Collections.emptyMap());
    String catalogNameF = RandomNameUtils.genRandomName("catalogF");
    Map<String, String> properties = Maps.newHashMap();
    Catalog catalog =
        metalake.createCatalog(catalogNameF, Catalog.Type.MODEL, "catalog comment", properties);

    NameIdentifier modelIdent = NameIdentifier.of("schema_owner", "model_owner");
    catalog.asSchemas().createSchema("schema_owner", "comment", Collections.emptyMap());
    catalog.asModelCatalog().registerModel(modelIdent, "comment", Collections.emptyMap());

    MetadataObject metalakeObject =
        MetadataObjects.of(null, metalakeNameF, MetadataObject.Type.METALAKE);
    Owner owner = metalake.getOwner(metalakeObject).get();
    Assertions.assertEquals(AuthConstants.ANONYMOUS_USER, owner.name());
    Assertions.assertEquals(Owner.Type.USER, owner.type());

    MetadataObject catalogObject =
        MetadataObjects.of(Lists.newArrayList(catalogNameF), MetadataObject.Type.CATALOG);
    owner = metalake.getOwner(catalogObject).get();
    Assertions.assertEquals(AuthConstants.ANONYMOUS_USER, owner.name());
    Assertions.assertEquals(Owner.Type.USER, owner.type());

    MetadataObject schemaObject =
        MetadataObjects.of(
            Lists.newArrayList(catalogNameF, "schema_owner"), MetadataObject.Type.SCHEMA);
    owner = metalake.getOwner(schemaObject).get();
    Assertions.assertEquals(AuthConstants.ANONYMOUS_USER, owner.name());
    Assertions.assertEquals(Owner.Type.USER, owner.type());

    MetadataObject modelObject =
        MetadataObjects.of(
            Lists.newArrayList(catalogNameF, "schema_owner", "model_owner"),
            MetadataObject.Type.MODEL);
    owner = metalake.getOwner(modelObject).get();
    Assertions.assertEquals(AuthConstants.ANONYMOUS_USER, owner.name());
    Assertions.assertEquals(Owner.Type.USER, owner.type());

    // Set another owner
    String anotherUser = "another";
    metalake.addUser(anotherUser);
    metalake.setOwner(modelObject, anotherUser, Owner.Type.USER);
    owner = metalake.getOwner(modelObject).get();
    Assertions.assertEquals(anotherUser, owner.name());
    Assertions.assertEquals(Owner.Type.USER, owner.type());

    // Clean up
    catalog.asModelCatalog().deleteModel(modelIdent);
    catalog.asSchemas().dropSchema("schema_owner", true);
    metalake.dropCatalog(catalogNameF, true);
    client.dropMetalake(metalakeNameF, true);
  }

  @Test
  public void testOwnerWithException() {
    String metalakeNameE = RandomNameUtils.genRandomName("metalakeE");
    String catalogNameE = RandomNameUtils.genRandomName("catalogE");
    GravitinoMetalake metalake =
        client.createMetalake(metalakeNameE, "metalake E comment", Collections.emptyMap());

    MetadataObject metalakeObject =
        MetadataObjects.of(Lists.newArrayList(metalakeNameE), MetadataObject.Type.METALAKE);

    MetadataObject catalogObject =
        MetadataObjects.of(Lists.newArrayList(catalogNameE), MetadataObject.Type.CATALOG);

    // Get a not-existed catalog
    Assertions.assertThrows(
        NoSuchMetadataObjectException.class, () -> metalake.getOwner(catalogObject));

    // Set a not-existed catalog
    Assertions.assertThrows(
        NotFoundException.class,
        () -> metalake.setOwner(catalogObject, AuthConstants.ANONYMOUS_USER, Owner.Type.USER));

    // Set a not-existed user
    Assertions.assertThrows(
        NotFoundException.class,
        () -> metalake.setOwner(metalakeObject, "not-existed", Owner.Type.USER));

    // Cleanup
    client.dropMetalake(metalakeNameE, true);
  }
}
