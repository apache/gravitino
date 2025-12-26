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

import static org.junit.Assert.assertThrows;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.authorization.Owner;
import org.apache.gravitino.authorization.Privileges;
import org.apache.gravitino.authorization.SecurableObject;
import org.apache.gravitino.authorization.SecurableObjects;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.integration.test.container.ContainerSuite;
import org.apache.gravitino.integration.test.container.KafkaContainer;
import org.apache.gravitino.messaging.Topic;
import org.apache.gravitino.messaging.TopicCatalog;
import org.apache.gravitino.messaging.TopicChange;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

@Tag("gravitino-docker-test")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class TopicAuthorizationIT extends BaseRestApiAuthorizationIT {

  private static final String CATALOG = "catalog";
  private static final String SCHEMA = "default";
  private static final ContainerSuite containerSuite = ContainerSuite.getInstance();
  private static String kafkaBootstrapServers;
  private static String role = "role";

  @BeforeAll
  public void startIntegrationTest() throws Exception {
    containerSuite.startKafkaContainer();
    super.startIntegrationTest();
    kafkaBootstrapServers =
        String.format(
            "%s:%d",
            containerSuite.getKafkaContainer().getContainerIpAddress(),
            KafkaContainer.DEFAULT_BROKER_PORT);
    Map<String, String> properties = Maps.newHashMap();
    properties.put("bootstrap.servers", kafkaBootstrapServers);
    client
        .loadMetalake(METALAKE)
        .createCatalog(CATALOG, Catalog.Type.MESSAGING, "kafka", "comment", properties);
    // try to load the schema as normal user, expect failure
    assertThrows(
        "Can not access metadata {" + CATALOG + "." + SCHEMA + "}.",
        RuntimeException.class,
        () -> {
          normalUserClient
              .loadMetalake(METALAKE)
              .loadCatalog(CATALOG)
              .asSchemas()
              .loadSchema(SCHEMA);
        });
    // grant tester privilege
    List<SecurableObject> securableObjects = new ArrayList<>();
    GravitinoMetalake gravitinoMetalake = client.loadMetalake(METALAKE);
    SecurableObject catalogObject =
        SecurableObjects.ofCatalog(CATALOG, ImmutableList.of(Privileges.UseCatalog.allow()));
    securableObjects.add(catalogObject);
    gravitinoMetalake.createRole(role, new HashMap<>(), securableObjects);
    gravitinoMetalake.grantRolesToUser(ImmutableList.of(role), NORMAL_USER);
    // normal user can load the catalog but not the schema
    Catalog catalogLoadByNormalUser = normalUserClient.loadMetalake(METALAKE).loadCatalog(CATALOG);
    assertEquals(CATALOG, catalogLoadByNormalUser.name());
    assertThrows(
        "Can not access metadata {" + CATALOG + "." + SCHEMA + "}.",
        ForbiddenException.class,
        () -> {
          catalogLoadByNormalUser.asSchemas().loadSchema(SCHEMA);
        });
  }

  @Test
  @Order(1)
  public void testCreateTopic() {
    // owner can create topic
    TopicCatalog topicCatalog = client.loadMetalake(METALAKE).loadCatalog(CATALOG).asTopicCatalog();
    topicCatalog.createTopic(NameIdentifier.of(SCHEMA, "topic1"), "test", null, new HashMap<>());
    // normal user cannot create topic
    TopicCatalog topicCatalogNormalUser =
        normalUserClient.loadMetalake(METALAKE).loadCatalog(CATALOG).asTopicCatalog();
    assertThrows(
        "Can not access metadata {" + CATALOG + "." + SCHEMA + "}.",
        ForbiddenException.class,
        () -> {
          topicCatalogNormalUser.createTopic(
              NameIdentifier.of(SCHEMA, "topic2"), "test2", null, new HashMap<>());
        });

    assertThrows(
        "Can not access metadata {" + CATALOG + "." + SCHEMA + "}.",
        ForbiddenException.class,
        () -> {
          topicCatalogNormalUser.listTopics(Namespace.of(SCHEMA));
        });

    // grant privileges
    GravitinoMetalake gravitinoMetalake = client.loadMetalake(METALAKE);
    gravitinoMetalake.grantPrivilegesToRole(
        role,
        MetadataObjects.of(CATALOG, SCHEMA, MetadataObject.Type.SCHEMA),
        ImmutableList.of(Privileges.UseSchema.allow(), Privileges.CreateTopic.allow()));

    normalUserClient.loadMetalake(METALAKE).loadCatalog(CATALOG).asSchemas().loadSchema(SCHEMA);

    // normal user can now create topic
    topicCatalogNormalUser.createTopic(
        NameIdentifier.of(SCHEMA, "topic2"), "test2", null, new HashMap<>());
    topicCatalogNormalUser.createTopic(
        NameIdentifier.of(SCHEMA, "topic3"), "test3", null, new HashMap<>());
  }

  @Test
  @Order(2)
  public void testListTopic() {
    TopicCatalog topicCatalog = client.loadMetalake(METALAKE).loadCatalog(CATALOG).asTopicCatalog();
    NameIdentifier[] topicsList = topicCatalog.listTopics(Namespace.of(SCHEMA));
    assertArrayEquals(
        new NameIdentifier[] {
          NameIdentifier.of(SCHEMA, "topic1"),
          NameIdentifier.of(SCHEMA, "topic2"),
          NameIdentifier.of(SCHEMA, "topic3")
        },
        topicsList);
    // normal user can only see topics they have privilege for
    TopicCatalog topicCatalogNormalUser =
        normalUserClient.loadMetalake(METALAKE).loadCatalog(CATALOG).asTopicCatalog();

    NameIdentifier[] topicsListNormalUser = topicCatalogNormalUser.listTopics(Namespace.of(SCHEMA));
    assertArrayEquals(
        new NameIdentifier[] {
          NameIdentifier.of(SCHEMA, "topic2"), NameIdentifier.of(SCHEMA, "topic3")
        },
        topicsListNormalUser);
  }

  @Test
  @Order(3)
  public void testLoadTopic() {
    TopicCatalog topicCatalogNormalUser =
        normalUserClient.loadMetalake(METALAKE).loadCatalog(CATALOG).asTopicCatalog();
    // normal user can load topic2 and topic3, but not topic1
    assertThrows(
        String.format("Can not access metadata {%s.%s.%s}.", CATALOG, SCHEMA, "topic1"),
        ForbiddenException.class,
        () -> {
          topicCatalogNormalUser.loadTopic(NameIdentifier.of(SCHEMA, "topic1"));
        });
    Topic topic2 = topicCatalogNormalUser.loadTopic(NameIdentifier.of(SCHEMA, "topic2"));
    assertEquals("topic2", topic2.name());
    Topic topic3 = topicCatalogNormalUser.loadTopic(NameIdentifier.of(SCHEMA, "topic3"));
    assertEquals("topic3", topic3.name());

    // grant normal user privilege to use topic1
    GravitinoMetalake gravitinoMetalake = client.loadMetalake(METALAKE);
    gravitinoMetalake.grantPrivilegesToRole(
        role,
        MetadataObjects.of(ImmutableList.of(CATALOG, SCHEMA, "topic1"), MetadataObject.Type.TOPIC),
        ImmutableList.of(Privileges.ConsumeTopic.allow()));
    topicCatalogNormalUser.loadTopic(NameIdentifier.of(SCHEMA, "topic1"));
  }

  @Test
  @Order(4)
  public void testAlterTopic() {
    GravitinoMetalake gravitinoMetalake = client.loadMetalake(METALAKE);
    TopicCatalog topicCatalogNormalUser =
        normalUserClient.loadMetalake(METALAKE).loadCatalog(CATALOG).asTopicCatalog();

    // normal user cannot alter topic1 (no privilege)
    assertThrows(
        String.format("Can not access metadata {%s.%s.%s}.", CATALOG, SCHEMA, "topic1"),
        ForbiddenException.class,
        () -> {
          topicCatalogNormalUser.alterTopic(
              NameIdentifier.of(SCHEMA, "topic1"), TopicChange.updateComment("new comment"));
        });
    // grant normal user owner privilege on topic1
    gravitinoMetalake.setOwner(
        MetadataObjects.of(ImmutableList.of(CATALOG, SCHEMA, "topic1"), MetadataObject.Type.TOPIC),
        NORMAL_USER,
        Owner.Type.USER);
    topicCatalogNormalUser.alterTopic(
        NameIdentifier.of(SCHEMA, "topic1"), TopicChange.updateComment("new comment"));
  }

  @Test
  @Order(5)
  public void testDropTopic() {
    GravitinoMetalake gravitinoMetalake = client.loadMetalake(METALAKE);
    TopicCatalog topicCatalogNormalUser =
        normalUserClient.loadMetalake(METALAKE).loadCatalog(CATALOG).asTopicCatalog();
    // reset owner
    gravitinoMetalake.setOwner(
        MetadataObjects.of(ImmutableList.of(CATALOG, SCHEMA, "topic1"), MetadataObject.Type.TOPIC),
        USER,
        Owner.Type.USER);
    // normal user cannot drop topic1
    assertThrows(
        String.format("Can not access metadata {%s.%s.%s}.", CATALOG, SCHEMA, "topic1"),
        ForbiddenException.class,
        () -> {
          topicCatalogNormalUser.dropTopic(NameIdentifier.of(SCHEMA, "topic1"));
        });
    // normal user can drop topic2 and topic3 (they created them)
    topicCatalogNormalUser.dropTopic(NameIdentifier.of(SCHEMA, "topic2"));
    topicCatalogNormalUser.dropTopic(NameIdentifier.of(SCHEMA, "topic3"));

    // owner can drop topic1
    TopicCatalog topicCatalog = client.loadMetalake(METALAKE).loadCatalog(CATALOG).asTopicCatalog();
    topicCatalog.dropTopic(NameIdentifier.of(SCHEMA, "topic1"));
    // check topics are dropped
    NameIdentifier[] topicsList = topicCatalog.listTopics(Namespace.of(SCHEMA));
    assertArrayEquals(new NameIdentifier[] {}, topicsList);
    NameIdentifier[] topicsListNormalUser = topicCatalogNormalUser.listTopics(Namespace.of(SCHEMA));
    assertArrayEquals(new NameIdentifier[] {}, topicsListNormalUser);
  }
}
