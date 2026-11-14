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
package org.apache.gravitino.authorization.kafka.integration.test;

import static org.apache.gravitino.Catalog.AUTHORIZATION_PROVIDER;

import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.PortBinding;
import com.github.dockerjava.api.model.Ports;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.net.ServerSocket;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.Configs;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.authorization.Owner;
import org.apache.gravitino.authorization.Privileges;
import org.apache.gravitino.authorization.SecurableObject;
import org.apache.gravitino.authorization.SecurableObjects;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.integration.test.util.BaseIT;
import org.apache.gravitino.integration.test.util.GravitinoITUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.acl.AccessControlEntryFilter;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.errors.ClusterAuthorizationException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePatternFilter;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.utility.DockerImageName;

@Tag("gravitino-docker-test")
public class KafkaAuthorizationIT extends BaseIT {

  private static final String GRAVITINO_ADMIN = "gravitino_admin";
  private static final String DEFAULT_KAFKA_IMAGE = "apache/kafka:3.7.0";
  private static final String LISTENER_JAAS =
      "org.apache.kafka.common.security.plain.PlainLoginModule required "
          + "username=\"admin\" password=\"admin-secret\" "
          + "user_admin=\"admin-secret\" "
          + "user_alice=\"alice-secret\" "
          + "user_bob=\"bob-secret\" "
          + "user_creator=\"creator-secret\";";

  private GenericContainer<?> kafkaContainer;
  private int kafkaPort;
  private String bootstrapServers;
  private AdminClient superAdminClient;

  private String metalakeName;
  private String catalogName;
  private GravitinoMetalake metalake;
  private Catalog catalog;

  @BeforeAll
  @Override
  public void startIntegrationTest() throws Exception {
    kafkaPort = findFreePort();
    bootstrapServers = "127.0.0.1:" + kafkaPort;
    String kafkaImage =
        System.getenv().getOrDefault("GRAVITINO_CI_KAFKA_DOCKER_IMAGE", DEFAULT_KAFKA_IMAGE);

    kafkaContainer =
        new GenericContainer<>(DockerImageName.parse(kafkaImage))
            .withExposedPorts(9092)
            .withCreateContainerCmdModifier(
                cmd ->
                    cmd.getHostConfig()
                        .withPortBindings(
                            new PortBinding(
                                Ports.Binding.bindIpAndPort("127.0.0.1", kafkaPort),
                                new ExposedPort(9092))))
            .withEnv("CLUSTER_ID", "MkU3OEVBNTcwNTJENDM2Qk")
            .withEnv("KAFKA_NODE_ID", "1")
            .withEnv("KAFKA_PROCESS_ROLES", "broker,controller")
            .withEnv("KAFKA_LISTENERS", "CLIENT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093")
            .withEnv("KAFKA_ADVERTISED_LISTENERS", "CLIENT://" + bootstrapServers)
            .withEnv(
                "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP",
                "CLIENT:SASL_PLAINTEXT,CONTROLLER:SASL_PLAINTEXT")
            .withEnv("KAFKA_CONTROLLER_LISTENER_NAMES", "CONTROLLER")
            .withEnv("KAFKA_INTER_BROKER_LISTENER_NAME", "CLIENT")
            .withEnv("KAFKA_CONTROLLER_QUORUM_VOTERS", "1@localhost:9093")
            .withEnv("KAFKA_SASL_ENABLED_MECHANISMS", "PLAIN")
            .withEnv("KAFKA_SASL_MECHANISM_INTER_BROKER_PROTOCOL", "PLAIN")
            .withEnv("KAFKA_SASL_MECHANISM_CONTROLLER_PROTOCOL", "PLAIN")
            .withEnv("KAFKA_LISTENER_NAME_CLIENT_PLAIN_SASL_JAAS_CONFIG", LISTENER_JAAS)
            .withEnv("KAFKA_LISTENER_NAME_CONTROLLER_PLAIN_SASL_JAAS_CONFIG", LISTENER_JAAS)
            .withCopyToContainer(
                Transferable.of("KafkaServer {\n  " + LISTENER_JAAS + "\n};\n", 0644),
                "/tmp/kafka_jaas.conf")
            .withEnv("KAFKA_OPTS", "-Djava.security.auth.login.config=/tmp/kafka_jaas.conf")
            .withEnv(
                "KAFKA_AUTHORIZER_CLASS_NAME",
                "org.apache.kafka.metadata.authorizer.StandardAuthorizer")
            .withEnv("KAFKA_ALLOW_EVERYONE_IF_NO_ACL_FOUND", "false")
            .withEnv("KAFKA_SUPER_USERS", "User:admin")
            .withEnv("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1")
            .withEnv("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
            .withEnv("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1")
            .withEnv("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0")
            .withStartupTimeout(Duration.ofMinutes(3));
    kafkaContainer.start();

    superAdminClient = AdminClient.create(saslClientProperties("admin", "admin-secret"));
    Awaitility.await()
        .atMost(60, TimeUnit.SECONDS)
        .pollInterval(1, TimeUnit.SECONDS)
        .until(
            () -> {
              try {
                superAdminClient.describeCluster().clusterId().get(5, TimeUnit.SECONDS);
                return true;
              } catch (Exception e) {
                return false;
              }
            });

    registerCustomConfigs(
        ImmutableMap.of(
            Configs.ENABLE_AUTHORIZATION.getKey(),
            "true",
            Configs.SERVICE_ADMINS.getKey(),
            GRAVITINO_ADMIN,
            Configs.AUTHENTICATORS.getKey(),
            "simple",
            "SimpleAuthUserName",
            GRAVITINO_ADMIN));

    super.startIntegrationTest();

    metalakeName = GravitinoITUtils.genRandomName("kafka_authz_ml");
    catalogName = GravitinoITUtils.genRandomName("kafka_authz_cat");
    client.createMetalake(
        metalakeName, "Kafka authorization integration test", Collections.emptyMap());
    metalake = client.loadMetalake(metalakeName);

    catalog =
        metalake.createCatalog(
            catalogName,
            Catalog.Type.MESSAGING,
            "kafka",
            "Kafka catalog with native StandardAuthorizer push-down",
            ImmutableMap.of(
                "bootstrap.servers",
                bootstrapServers,
                AUTHORIZATION_PROVIDER,
                "kafka",
                "gravitino.bypass.security.protocol",
                "SASL_PLAINTEXT",
                "gravitino.bypass.sasl.mechanism",
                "PLAIN",
                "gravitino.bypass.sasl.jaas.config",
                plainJaasConfig("admin", "admin-secret")));

    metalake.addUser("alice");
    metalake.addUser("bob");
    metalake.addUser("creator");
  }

  @AfterAll
  @Override
  public void stopIntegrationTest() throws IOException, InterruptedException {
    try {
      if (superAdminClient != null) {
        superAdminClient.close();
      }
      if (kafkaContainer != null) {
        kafkaContainer.stop();
      }
    } finally {
      super.stopIntegrationTest();
    }
  }

  @Test
  public void testEndToEndKafkaAuthorizationPushDown() throws Exception {
    String ordersTopic = "orders_topic";
    String paymentsTopic = "payments_topic";
    catalog
        .asTopicCatalog()
        .createTopic(
            NameIdentifier.of("default", ordersTopic),
            "Orders events topic",
            null,
            ImmutableMap.of("partition-count", "1", "replication-factor", "1"));
    catalog
        .asTopicCatalog()
        .createTopic(
            NameIdentifier.of("default", paymentsTopic),
            "Payments events topic",
            null,
            ImmutableMap.of("partition-count", "1", "replication-factor", "1"));

    SecurableObject catalogObj =
        SecurableObjects.ofCatalog(
            catalogName, Collections.singletonList(Privileges.UseCatalog.allow()));
    SecurableObject schemaObj =
        SecurableObjects.ofSchema(
            catalogObj, "default", Collections.singletonList(Privileges.UseSchema.allow()));

    // 1. Unprivileged alice cannot produce or consume
    assertProduceDenied("alice", "alice-secret", ordersTopic);
    assertConsumeDenied("alice", "alice-secret", ordersTopic);

    // 2. Grant ALLOW PRODUCE_TOPIC (with non-Kafka USE_CATALOG + USE_SCHEMA) on orders_topic to
    // alice
    String produceOrdersRole = GravitinoITUtils.genRandomName("produce_orders");
    metalake.createRole(
        produceOrdersRole,
        Collections.emptyMap(),
        Arrays.asList(
            catalogObj,
            schemaObj,
            SecurableObjects.ofTopic(
                schemaObj,
                ordersTopic,
                Collections.singletonList(Privileges.ProduceTopic.allow()))));
    metalake.grantRolesToUser(Collections.singletonList(produceOrdersRole), "alice");

    Awaitility.await()
        .atMost(15, TimeUnit.SECONDS)
        .untilAsserted(() -> produceOnce("alice", "alice-secret", ordersTopic, "k1", "v1"));
    assertProduceDenied("bob", "bob-secret", ordersTopic);

    // 3. Grant ALLOW CONSUME_TOPIC on orders_topic to alice and consume the produced record
    String consumeOrdersRole = GravitinoITUtils.genRandomName("consume_orders");
    metalake.createRole(
        consumeOrdersRole,
        Collections.emptyMap(),
        Collections.singletonList(
            SecurableObjects.ofTopic(
                schemaObj,
                ordersTopic,
                Collections.singletonList(Privileges.ConsumeTopic.allow()))));
    metalake.grantRolesToUser(Collections.singletonList(consumeOrdersRole), "alice");

    Awaitility.await()
        .atMost(15, TimeUnit.SECONDS)
        .untilAsserted(
            () -> {
              List<String> values =
                  consumeViaSubscribe("alice", "alice-secret", ordersTopic, "alice-group");
              Assertions.assertTrue(values.contains("v1"));
            });

    // 4. Schema-wide ALLOW PRODUCE + DENY PRODUCE on orders + ALLOW CONSUME on orders for bob
    String bobRole = GravitinoITUtils.genRandomName("bob_mixed");
    metalake.createRole(
        bobRole,
        Collections.emptyMap(),
        Arrays.asList(
            SecurableObjects.ofSchema(
                catalogObj, "default", Collections.singletonList(Privileges.ProduceTopic.allow())),
            SecurableObjects.ofTopic(
                schemaObj,
                ordersTopic,
                Arrays.asList(Privileges.ProduceTopic.deny(), Privileges.ConsumeTopic.allow()))));
    metalake.grantRolesToUser(Collections.singletonList(bobRole), "bob");

    Awaitility.await()
        .atMost(15, TimeUnit.SECONDS)
        .untilAsserted(
            () -> {
              produceOnce("bob", "bob-secret", paymentsTopic, "pk1", "pv1");
              List<String> values = consumeViaAssign("bob", "bob-secret", ordersTopic);
              Assertions.assertTrue(values.contains("v1"));
            });
    assertProduceDenied("bob", "bob-secret", ordersTopic);

    // 5. Revoke produce and consume roles from alice -> rejected again
    metalake.revokeRolesFromUser(Arrays.asList(produceOrdersRole, consumeOrdersRole), "alice");
    Awaitility.await()
        .atMost(15, TimeUnit.SECONDS)
        .untilAsserted(() -> assertProduceDenied("alice", "alice-secret", ordersTopic));
    assertConsumeDenied("alice", "alice-secret", ordersTopic);

    // 6. Grant CREATE_TOPIC on schema/catalog to creator and verify AdminClient.createTopics
    assertCreateTopicDenied("bob", "bob-secret", "bob_blocked_topic");
    String creatorRole = GravitinoITUtils.genRandomName("topic_creator");
    metalake.createRole(
        creatorRole,
        Collections.emptyMap(),
        Collections.singletonList(
            SecurableObjects.ofSchema(
                catalogObj, "default", Collections.singletonList(Privileges.CreateTopic.allow()))));
    metalake.grantRolesToUser(Collections.singletonList(creatorRole), "creator");

    String createdTopic = "creator_created_topic";
    Awaitility.await()
        .atMost(15, TimeUnit.SECONDS)
        .untilAsserted(() -> createKafkaTopicAs("creator", "creator-secret", createdTopic));

    // 7. Set bob as Gravitino owner of orders_topic -> bob can produce, consume, and alterConfigs;
    //    Wait: first revoke bobRole DENY on orders_topic so ALLOW from owner takes effect clearly.
    metalake.revokeRolesFromUser(Collections.singletonList(bobRole), "bob");
    MetadataObject ordersMetadataObject =
        MetadataObjects.of(
            Arrays.asList(catalogName, "default", ordersTopic), MetadataObject.Type.TOPIC);
    metalake.setOwner(ordersMetadataObject, "bob", Owner.Type.USER);

    Awaitility.await()
        .atMost(15, TimeUnit.SECONDS)
        .untilAsserted(
            () -> {
              produceOnce("bob", "bob-secret", ordersTopic, "k2", "v2");
              alterTopicConfigAs("bob", "bob-secret", ordersTopic, "retention.ms", "86400000");
            });

    // Drop topic via Gravitino and verify topic-level ACL bindings are cleaned up in Kafka
    Assertions.assertTrue(
        catalog.asTopicCatalog().dropTopic(NameIdentifier.of("default", ordersTopic)));

    AclBindingFilter ordersAclFilter =
        new AclBindingFilter(
            new ResourcePatternFilter(ResourceType.TOPIC, ordersTopic, PatternType.LITERAL),
            AccessControlEntryFilter.ANY);
    Awaitility.await()
        .atMost(15, TimeUnit.SECONDS)
        .untilAsserted(
            () -> {
              Collection<AclBinding> remaining =
                  superAdminClient.describeAcls(ordersAclFilter).values().get();
              Assertions.assertTrue(
                  remaining.isEmpty(),
                  "Expected no remaining ACLs for dropped topic, found " + remaining);
            });
  }

  private Properties saslClientProperties(String username, String password) {
    Properties props = new Properties();
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    props.put(AdminClientConfig.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
    props.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
    props.put(SaslConfigs.SASL_JAAS_CONFIG, plainJaasConfig(username, password));
    props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "5000");
    props.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "5000");
    return props;
  }

  private void produceOnce(String username, String password, String topic, String key, String value)
      throws Exception {
    Properties props = saslClientProperties(username, password);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    props.put(ProducerConfig.ACKS_CONFIG, "all");
    props.put(ProducerConfig.RETRIES_CONFIG, 0);
    props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 4000);
    props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 4000);
    props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 3000);

    try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
      producer.send(new ProducerRecord<>(topic, key, value)).get(5, TimeUnit.SECONDS);
    }
  }

  private void assertProduceDenied(String username, String password, String topic) {
    ExecutionException ex =
        Assertions.assertThrows(
            ExecutionException.class, () -> produceOnce(username, password, topic, "k", "v"));
    Assertions.assertInstanceOf(TopicAuthorizationException.class, ex.getCause());
  }

  private List<String> consumeViaSubscribe(
      String username, String password, String topic, String groupId) {
    Properties props = saslClientProperties(username, password);
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    List<String> values = new ArrayList<>();
    try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
      consumer.subscribe(Collections.singletonList(topic));
      ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(3));
      for (ConsumerRecord<String, String> record : records) {
        values.add(record.value());
      }
    }
    Assertions.assertFalse(
        values.isEmpty(), "Expected to consume at least one record from " + topic);
    return values;
  }

  private List<String> consumeViaAssign(String username, String password, String topic) {
    Properties props = saslClientProperties(username, password);
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

    TopicPartition tp = new TopicPartition(topic, 0);
    List<String> values = new ArrayList<>();
    try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
      consumer.assign(Collections.singletonList(tp));
      consumer.seekToBeginning(Collections.singletonList(tp));
      ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(3));
      for (ConsumerRecord<String, String> record : records) {
        values.add(record.value());
      }
    }
    return values;
  }

  private void assertConsumeDenied(String username, String password, String topic) {
    TopicPartition tp = new TopicPartition(topic, 0);
    Properties props = saslClientProperties(username, password);
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

    AtomicReference<Throwable> thrown = new AtomicReference<>();
    try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
      consumer.assign(Collections.singletonList(tp));
      consumer.seekToBeginning(Collections.singletonList(tp));
      consumer.poll(Duration.ofSeconds(3));
    } catch (Throwable t) {
      thrown.set(t);
    }
    Assertions.assertInstanceOf(TopicAuthorizationException.class, thrown.get());
  }

  private void createKafkaTopicAs(String username, String password, String topic) throws Exception {
    try (AdminClient client = AdminClient.create(saslClientProperties(username, password))) {
      client.createTopics(Collections.singletonList(new NewTopic(topic, 1, (short) 1))).all().get();
    }
  }

  private void assertCreateTopicDenied(String username, String password, String topic) {
    ExecutionException ex =
        Assertions.assertThrows(
            ExecutionException.class, () -> createKafkaTopicAs(username, password, topic));
    Throwable cause = ex.getCause();
    Assertions.assertTrue(
        cause instanceof TopicAuthorizationException
            || cause instanceof ClusterAuthorizationException,
        "Expected TopicAuthorizationException or ClusterAuthorizationException, got " + cause);
  }

  private void alterTopicConfigAs(
      String username, String password, String topic, String key, String value) throws Exception {
    ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
    AlterConfigOp op = new AlterConfigOp(new ConfigEntry(key, value), AlterConfigOp.OpType.SET);
    try (AdminClient client = AdminClient.create(saslClientProperties(username, password))) {
      client
          .incrementalAlterConfigs(
              Collections.singletonMap(resource, Collections.singletonList(op)))
          .all()
          .get();
    }
  }

  private static String plainJaasConfig(String username, String password) {
    return String.format(
        "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";",
        username, password);
  }

  private static int findFreePort() throws IOException {
    try (ServerSocket socket = new ServerSocket(0)) {
      socket.setReuseAddress(true);
      return socket.getLocalPort();
    }
  }
}
