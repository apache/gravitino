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

import static org.apache.gravitino.server.GravitinoServer.WEBSERVER_CONF_PREFIX;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.util.Collections;
import java.util.Map;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.Configs;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.auth.AuthConstants;
import org.apache.gravitino.authorization.Privileges;
import org.apache.gravitino.authorization.SecurableObject;
import org.apache.gravitino.authorization.SecurableObjects;
import org.apache.gravitino.client.GravitinoAdminClient;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.file.Fileset;
import org.apache.gravitino.integration.test.container.ContainerSuite;
import org.apache.gravitino.integration.test.container.HiveContainer;
import org.apache.gravitino.integration.test.container.KafkaContainer;
import org.apache.gravitino.integration.test.util.BaseIT;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.types.Types;
import org.apache.gravitino.server.web.JettyServerConfig;
import org.apache.gravitino.utils.RandomNameUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Tag("gravitino-docker-test")
public class CheckCurrentUserIT extends BaseIT {

  private static final Logger LOG = LoggerFactory.getLogger(CheckCurrentUserIT.class);
  private static final ContainerSuite containerSuite = ContainerSuite.getInstance();
  private static String hmsUri;
  private static String kafkaBootstrapServers;
  private static GravitinoMetalake metalake;
  private static GravitinoMetalake anotherMetalake;
  private static String metalakeName = RandomNameUtils.genRandomName("metalake");

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

    JettyServerConfig jettyServerConfig =
        JettyServerConfig.fromConfig(serverConfig, WEBSERVER_CONF_PREFIX);

    String uri = "http://" + jettyServerConfig.getHost() + ":" + jettyServerConfig.getHttpPort();
    System.setProperty("user.name", "test");
    GravitinoAdminClient anotherClient = GravitinoAdminClient.builder(uri).withSimpleAuth().build();

    metalake = client.createMetalake(metalakeName, "metalake comment", Collections.emptyMap());
    anotherMetalake = anotherClient.loadMetalake(metalakeName);
  }

  @AfterAll
  public void tearDown() {
    if (client != null) {
      client.dropMetalake(metalakeName, true);
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
  public void testCreateTopic() {
    String catalogName = RandomNameUtils.genRandomName("catalogA");

    Map<String, String> properties = Maps.newHashMap();
    properties.put("bootstrap.servers", kafkaBootstrapServers);

    // Test to create catalog with not-existed user
    Assertions.assertThrows(
        ForbiddenException.class,
        () ->
            anotherMetalake.createCatalog(
                catalogName, Catalog.Type.MESSAGING, "kafka", "comment", properties));

    Catalog catalog =
        metalake.createCatalog(catalogName, Catalog.Type.MESSAGING, "kafka", "comment", properties);

    // Test to create topic with not-existed user
    metalake.addUser("test");
    Catalog anotherCatalog = anotherMetalake.loadCatalog(catalogName);
    metalake.removeUser("test");
    NameIdentifier topicIdent = NameIdentifier.of("default", "topic");
    Assertions.assertThrows(
        ForbiddenException.class,
        () ->
            anotherCatalog
                .asTopicCatalog()
                .createTopic(topicIdent, "comment", null, Collections.emptyMap()));

    Assertions.assertDoesNotThrow(
        () ->
            catalog
                .asTopicCatalog()
                .createTopic(topicIdent, "comment", null, Collections.emptyMap()));
    catalog.asTopicCatalog().dropTopic(topicIdent);

    metalake.dropCatalog(catalogName, true);
  }

  @Test
  public void testCreateFileset() {
    String catalogName = RandomNameUtils.genRandomName("catalog");
    // Test to create a fileset with a not-existed user
    Assertions.assertThrows(
        ForbiddenException.class,
        () ->
            anotherMetalake.createCatalog(
                catalogName, Catalog.Type.FILESET, "hadoop", "comment", Collections.emptyMap()));

    Catalog catalog =
        metalake.createCatalog(
            catalogName, Catalog.Type.FILESET, "hadoop", "comment", Collections.emptyMap());

    // Test to create a schema with a not-existed user
    Catalog anotherCatalog = anotherMetalake.loadCatalog(catalogName);
    Assertions.assertThrows(
        ForbiddenException.class,
        () -> anotherCatalog.asSchemas().createSchema("schema", "comment", Collections.emptyMap()));

    catalog.asSchemas().createSchema("schema", "comment", Collections.emptyMap());

    // Test to create a fileset with a not-existed user
    NameIdentifier fileIdent = NameIdentifier.of("schema", "fileset");
    Assertions.assertThrows(
        ForbiddenException.class,
        () ->
            anotherCatalog
                .asFilesetCatalog()
                .createFileset(
                    fileIdent, "comment", Fileset.Type.EXTERNAL, "tmp", Collections.emptyMap()));

    Assertions.assertDoesNotThrow(
        () ->
            catalog
                .asFilesetCatalog()
                .createFileset(
                    fileIdent, "comment", Fileset.Type.EXTERNAL, "tmp", Collections.emptyMap()));

    // Clean up
    catalog.asFilesetCatalog().dropFileset(fileIdent);
    catalog.asSchemas().dropSchema("schema", true);
    metalake.dropCatalog(catalogName, true);
  }

  @Test
  public void testCreateRole() {
    SecurableObject metalakeSecObject =
        SecurableObjects.ofMetalake(
            metalakeName, Lists.newArrayList(Privileges.CreateCatalog.allow()));
    Assertions.assertThrows(
        ForbiddenException.class,
        () ->
            anotherMetalake.createRole(
                "role", Collections.emptyMap(), Lists.newArrayList(metalakeSecObject)));

    Assertions.assertDoesNotThrow(
        () ->
            metalake.createRole(
                "role", Collections.emptyMap(), Lists.newArrayList(metalakeSecObject)));
    metalake.deleteRole("role");
  }

  @Test
  public void testCreateTable() {
    String catalogName = RandomNameUtils.genRandomName("catalog");
    Map<String, String> properties = Maps.newHashMap();
    properties.put("metastore.uris", hmsUri);

    // Test to create catalog with not-existed user
    Assertions.assertThrows(
        ForbiddenException.class,
        () ->
            anotherMetalake.createCatalog(
                catalogName, Catalog.Type.RELATIONAL, "hive", "catalog comment", properties));
    Catalog catalog =
        metalake.createCatalog(
            catalogName, Catalog.Type.RELATIONAL, "hive", "catalog comment", properties);

    // Test to create schema with not-existed user
    Catalog anotherCatalog = anotherMetalake.loadCatalog(catalogName);
    Assertions.assertThrows(
        ForbiddenException.class,
        () -> anotherCatalog.asSchemas().createSchema("schema", "comment", Collections.emptyMap()));

    catalog.asSchemas().createSchema("schema", "comment", Collections.emptyMap());

    // Test to create table with not-existed user
    NameIdentifier tableIdent = NameIdentifier.of("schema", "table");
    Assertions.assertThrows(
        ForbiddenException.class,
        () ->
            anotherCatalog
                .asTableCatalog()
                .createTable(
                    tableIdent,
                    new Column[] {
                      Column.of("col1", Types.IntegerType.get()),
                      Column.of("col2", Types.StringType.get())
                    },
                    "comment",
                    Collections.emptyMap()));

    Assertions.assertDoesNotThrow(
        () ->
            catalog
                .asTableCatalog()
                .createTable(
                    tableIdent,
                    new Column[] {
                      Column.of("col1", Types.IntegerType.get()),
                      Column.of("col2", Types.StringType.get())
                    },
                    "comment",
                    Collections.emptyMap()));

    // Clean up
    catalog.asTableCatalog().dropTable(tableIdent);
    catalog.asSchemas().dropSchema("schema", true);
    metalake.dropCatalog(catalogName, true);
  }
}
