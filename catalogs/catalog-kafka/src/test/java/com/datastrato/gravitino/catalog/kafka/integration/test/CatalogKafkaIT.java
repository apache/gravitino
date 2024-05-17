/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.kafka.integration.test;

import static com.datastrato.gravitino.catalog.kafka.KafkaCatalogPropertiesMetadata.BOOTSTRAP_SERVERS;
import static com.datastrato.gravitino.catalog.kafka.KafkaTopicPropertiesMetadata.PARTITION_COUNT;
import static com.datastrato.gravitino.catalog.kafka.KafkaTopicPropertiesMetadata.REPLICATION_FACTOR;
import static com.datastrato.gravitino.connector.BaseCatalog.CATALOG_BYPASS_PREFIX;
import static com.datastrato.gravitino.integration.test.container.KafkaContainer.DEFAULT_BROKER_PORT;

import com.datastrato.gravitino.Catalog;
import com.datastrato.gravitino.CatalogChange;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.client.GravitinoMetalake;
import com.datastrato.gravitino.exceptions.NoSuchCatalogException;
import com.datastrato.gravitino.integration.test.container.ContainerSuite;
import com.datastrato.gravitino.integration.test.util.AbstractIT;
import com.datastrato.gravitino.integration.test.util.GravitinoITUtils;
import com.datastrato.gravitino.messaging.Topic;
import com.datastrato.gravitino.messaging.TopicChange;
import com.datastrato.gravitino.rel.Schema;
import com.datastrato.gravitino.rel.SchemaChange;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Tag("gravitino-docker-it")
public class CatalogKafkaIT extends AbstractIT {
  private static final Logger LOG = LoggerFactory.getLogger(CatalogKafkaIT.class);
  private static final ContainerSuite CONTAINER_SUITE = ContainerSuite.getInstance();
  private static final String METALAKE_NAME =
      GravitinoITUtils.genRandomName("catalogKafkaIT_metalake");
  private static final String CATALOG_NAME =
      GravitinoITUtils.genRandomName("catalogKafkaIT_catalog");
  private static final String DEFAULT_SCHEMA_NAME = "default";
  private static final String PROVIDER = "kafka";
  private static GravitinoMetalake metalake;
  private static Catalog catalog;
  private static String kafkaBootstrapServers;
  private static AdminClient adminClient;

  @BeforeAll
  public static void startUp() throws ExecutionException, InterruptedException {
    CONTAINER_SUITE.startKafkaContainer();
    kafkaBootstrapServers =
        String.format(
            "%s:%d",
            CONTAINER_SUITE.getKafkaContainer().getContainerIpAddress(), DEFAULT_BROKER_PORT);
    adminClient = AdminClient.create(ImmutableMap.of(BOOTSTRAP_SERVERS, kafkaBootstrapServers));

    // create topics for testing
    adminClient
        .createTopics(
            ImmutableList.of(
                new NewTopic("topic1", 1, (short) 1),
                new NewTopic("topic2", 1, (short) 1),
                new NewTopic("topic3", 1, (short) 1)))
        .all()
        .get();

    createMetalake();
    Map<String, String> properties = Maps.newHashMap();
    properties.put(BOOTSTRAP_SERVERS, kafkaBootstrapServers);
    catalog = createCatalog(CATALOG_NAME, "Kafka catalog for IT", properties);
  }

  @AfterAll
  public static void shutdown() {
    client.dropMetalake(METALAKE_NAME);
    if (adminClient != null) {
      adminClient.close();
    }

    try {
      closer.close();
    } catch (Exception e) {
      LOG.error("Failed to close CloseableGroup", e);
    }
  }

  @Test
  public void testCatalog() throws ExecutionException, InterruptedException {
    // test create catalog
    String catalogName = GravitinoITUtils.genRandomName("test-catalog");
    String comment = "test catalog";
    Map<String, String> properties =
        ImmutableMap.of(BOOTSTRAP_SERVERS, kafkaBootstrapServers, "key1", "value1");
    Catalog createdCatalog = createCatalog(catalogName, comment, properties);
    Assertions.assertEquals(catalogName, createdCatalog.name());
    Assertions.assertEquals(comment, createdCatalog.comment());
    Assertions.assertEquals(
        kafkaBootstrapServers, createdCatalog.properties().get(BOOTSTRAP_SERVERS));

    // test load catalog
    Catalog loadedCatalog = metalake.loadCatalog(catalogName);
    Assertions.assertEquals(createdCatalog, loadedCatalog);

    // test alter catalog
    Catalog alteredCatalog =
        metalake.alterCatalog(
            catalogName,
            CatalogChange.updateComment("new comment"),
            CatalogChange.removeProperty("key1"));
    Assertions.assertEquals("new comment", alteredCatalog.comment());
    Assertions.assertFalse(alteredCatalog.properties().containsKey("key1"));

    // test drop catalog
    boolean dropped = metalake.dropCatalog(catalogName);
    Assertions.assertTrue(dropped);
    Exception exception =
        Assertions.assertThrows(
            NoSuchCatalogException.class, () -> metalake.loadCatalog(catalogName));
    Assertions.assertTrue(exception.getMessage().contains(catalogName));
    // assert topic exists in Kafka after catalog dropped
    Assertions.assertFalse(adminClient.listTopics().names().get().isEmpty());
  }

  @Test
  public void testCatalogException() {
    String catalogName = GravitinoITUtils.genRandomName("test-catalog");
    Exception exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                metalake.createCatalog(
                    catalogName,
                    Catalog.Type.MESSAGING,
                    PROVIDER,
                    "comment",
                    ImmutableMap.of(BOOTSTRAP_SERVERS, "2")));
    Assertions.assertTrue(exception.getMessage().contains("Invalid url in bootstrap.servers: 2"));

    exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                metalake.createCatalog(
                    catalogName,
                    Catalog.Type.MESSAGING,
                    PROVIDER,
                    "comment",
                    ImmutableMap.of("abc", "2")));
    Assertions.assertTrue(
        exception.getMessage().contains("Missing configuration: bootstrap.servers"));

    // Test BOOTSTRAP_SERVERS that cannot be linked
    Catalog kafka =
        metalake.createCatalog(
            NameIdentifier.of(METALAKE_NAME, catalogName),
            Catalog.Type.MESSAGING,
            PROVIDER,
            "comment",
            ImmutableMap.of(
                BOOTSTRAP_SERVERS,
                "192.0.2.1:9999",
                CATALOG_BYPASS_PREFIX + AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG,
                "3000",
                CATALOG_BYPASS_PREFIX + AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG,
                "3000"));
    exception =
        Assertions.assertThrows(
            RuntimeException.class,
            () ->
                kafka
                    .asTopicCatalog()
                    .listTopics(
                        Namespace.ofTopic(METALAKE_NAME, catalogName, DEFAULT_SCHEMA_NAME)));
    Assertions.assertTrue(
        exception
            .getMessage()
            .contains("Timed out waiting for a node assignment. Call: listTopics"),
        exception.getMessage());
  }

  @Test
  public void testDefaultSchema() {
    NameIdentifier[] schemas =
        catalog.asSchemas().listSchemas(Namespace.ofSchema(METALAKE_NAME, CATALOG_NAME));
    Assertions.assertEquals(1, schemas.length);
    Assertions.assertEquals(DEFAULT_SCHEMA_NAME, schemas[0].name());

    Schema loadSchema =
        catalog
            .asSchemas()
            .loadSchema(NameIdentifier.ofSchema(METALAKE_NAME, CATALOG_NAME, DEFAULT_SCHEMA_NAME));
    Assertions.assertEquals(
        "The default schema of Kafka catalog including all topics", loadSchema.comment());

    // test alter default schema
    Exception exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                catalog
                    .asSchemas()
                    .alterSchema(
                        NameIdentifier.ofSchema(METALAKE_NAME, CATALOG_NAME, DEFAULT_SCHEMA_NAME),
                        SchemaChange.removeProperty("key1")));
    Assertions.assertTrue(exception.getMessage().contains("Cannot alter the default schema"));

    // test drop default schema
    boolean dropped =
        catalog
            .asSchemas()
            .dropSchema(
                NameIdentifier.ofSchema(METALAKE_NAME, CATALOG_NAME, DEFAULT_SCHEMA_NAME), true);
    Assertions.assertFalse(dropped);
  }

  @Test
  public void testCreateSchema() {
    String schemaName = "test-schema";
    Exception ex =
        Assertions.assertThrows(
            UnsupportedOperationException.class,
            () ->
                catalog
                    .asSchemas()
                    .createSchema(
                        NameIdentifier.of(METALAKE_NAME, CATALOG_NAME, schemaName),
                        "comment",
                        Collections.emptyMap()));
    Assertions.assertTrue(
        ex.getMessage().contains("Kafka catalog does not support schema creation"));
  }

  @Test
  public void testListSchema() {
    NameIdentifier[] schemas =
        catalog.asSchemas().listSchemas(Namespace.ofSchema(METALAKE_NAME, CATALOG_NAME));
    Assertions.assertEquals(1, schemas.length);
    Assertions.assertEquals(DEFAULT_SCHEMA_NAME, schemas[0].name());
  }

  @Test
  public void testCreateAndListTopic() throws ExecutionException, InterruptedException {
    // test create topic
    String topicName = GravitinoITUtils.genRandomName("test-topic");
    Topic createdTopic =
        catalog
            .asTopicCatalog()
            .createTopic(
                NameIdentifier.of(METALAKE_NAME, CATALOG_NAME, DEFAULT_SCHEMA_NAME, topicName),
                "comment",
                null,
                Collections.emptyMap());
    Topic loadedTopic =
        catalog
            .asTopicCatalog()
            .loadTopic(
                NameIdentifier.of(METALAKE_NAME, CATALOG_NAME, DEFAULT_SCHEMA_NAME, topicName));

    Assertions.assertEquals(createdTopic, loadedTopic);
    assertTopicWithKafka(createdTopic);
    checkTopicReadWrite(topicName);

    // test list topics
    NameIdentifier[] topics =
        catalog
            .asTopicCatalog()
            .listTopics(Namespace.ofTopic(METALAKE_NAME, CATALOG_NAME, DEFAULT_SCHEMA_NAME));
    Assertions.assertTrue(topics.length > 0);
    Assertions.assertTrue(
        ImmutableList.copyOf(topics).stream().anyMatch(topic -> topic.name().equals(topicName)));
  }

  @Test
  public void testAlterTopic() {
    String topicName = GravitinoITUtils.genRandomName("test-topic");
    Topic createdTopic =
        catalog
            .asTopicCatalog()
            .createTopic(
                NameIdentifier.of(METALAKE_NAME, CATALOG_NAME, DEFAULT_SCHEMA_NAME, topicName),
                "comment",
                null,
                ImmutableMap.of(TopicConfig.RETENTION_MS_CONFIG, "43200000"));

    Assertions.assertEquals("comment", createdTopic.comment());
    Assertions.assertEquals("1", createdTopic.properties().get(PARTITION_COUNT));
    Assertions.assertEquals("1", createdTopic.properties().get(REPLICATION_FACTOR));
    Assertions.assertEquals(
        "43200000", createdTopic.properties().get(TopicConfig.RETENTION_MS_CONFIG));

    // alter topic
    Topic alteredTopic =
        catalog
            .asTopicCatalog()
            .alterTopic(
                NameIdentifier.of(METALAKE_NAME, CATALOG_NAME, DEFAULT_SCHEMA_NAME, topicName),
                TopicChange.updateComment("new comment"),
                TopicChange.setProperty(PARTITION_COUNT, "3"),
                TopicChange.removeProperty(TopicConfig.RETENTION_MS_CONFIG));
    Topic loadedTopic =
        catalog
            .asTopicCatalog()
            .loadTopic(
                NameIdentifier.of(METALAKE_NAME, CATALOG_NAME, DEFAULT_SCHEMA_NAME, topicName));

    Assertions.assertEquals(alteredTopic, loadedTopic);
    Assertions.assertEquals("new comment", alteredTopic.comment());
    Assertions.assertEquals("3", alteredTopic.properties().get(PARTITION_COUNT));
    // retention.ms overridden was removed, so it should be the default value
    Assertions.assertEquals(
        "604800000", alteredTopic.properties().get(TopicConfig.RETENTION_MS_CONFIG));
    checkTopicReadWrite(topicName);
  }

  @Test
  public void testDropTopic() throws ExecutionException, InterruptedException {
    String topicName = GravitinoITUtils.genRandomName("test-topic");
    Topic createdTopic =
        catalog
            .asTopicCatalog()
            .createTopic(
                NameIdentifier.of(METALAKE_NAME, CATALOG_NAME, DEFAULT_SCHEMA_NAME, topicName),
                "comment",
                null,
                Collections.emptyMap());

    boolean dropped =
        catalog
            .asTopicCatalog()
            .dropTopic(
                NameIdentifier.of(METALAKE_NAME, CATALOG_NAME, DEFAULT_SCHEMA_NAME, topicName));
    Assertions.assertTrue(dropped);

    // verify topic not exist in Kafka
    Exception ex =
        Assertions.assertThrows(ExecutionException.class, () -> getTopicDesc(createdTopic.name()));
    Assertions.assertTrue(
        ex.getMessage().contains("This server does not host this topic-partition"));

    // verify dropping non-exist topic
    String topicName1 = GravitinoITUtils.genRandomName("test-topic");
    catalog
        .asTopicCatalog()
        .createTopic(
            NameIdentifier.of(METALAKE_NAME, CATALOG_NAME, DEFAULT_SCHEMA_NAME, topicName1),
            "comment",
            null,
            Collections.emptyMap());

    adminClient.deleteTopics(Collections.singleton(topicName1)).all().get();
    boolean dropped1 =
        catalog
            .asTopicCatalog()
            .dropTopic(
                NameIdentifier.of(METALAKE_NAME, CATALOG_NAME, DEFAULT_SCHEMA_NAME, topicName1));
    Assertions.assertFalse(dropped1, "Should return false when dropping non-exist topic");
    Assertions.assertFalse(
        catalog
            .asTopicCatalog()
            .topicExists(
                NameIdentifier.of(METALAKE_NAME, CATALOG_NAME, DEFAULT_SCHEMA_NAME, topicName1)),
        "Topic should not exist after dropping");
  }

  @Test
  public void testNameSpec() throws ExecutionException, InterruptedException {
    // create topic in Kafka with special characters
    String illegalName = "test.topic";
    adminClient.createTopics(ImmutableList.of(new NewTopic(illegalName, 1, (short) 1))).all().get();

    NameIdentifier ident =
        NameIdentifier.of(METALAKE_NAME, CATALOG_NAME, DEFAULT_SCHEMA_NAME, illegalName);
    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                catalog
                    .asTopicCatalog()
                    .createTopic(ident, "comment", null, Collections.emptyMap()));
    Assertions.assertTrue(exception.getMessage().contains("Illegal name: test.topic"));

    Topic loadedTopic = catalog.asTopicCatalog().loadTopic(ident);
    Assertions.assertEquals(illegalName, loadedTopic.name());

    NameIdentifier[] topics =
        catalog
            .asTopicCatalog()
            .listTopics(Namespace.ofTopic(METALAKE_NAME, CATALOG_NAME, DEFAULT_SCHEMA_NAME));
    Assertions.assertTrue(
        Arrays.stream(topics).anyMatch(topic -> topic.name().equals(illegalName)));

    Assertions.assertTrue(catalog.asTopicCatalog().dropTopic(ident));
    Assertions.assertFalse(catalog.asTopicCatalog().topicExists(ident));
  }

  private void assertTopicWithKafka(Topic createdTopic)
      throws ExecutionException, InterruptedException {
    // get topic from Kafka directly
    TopicDescription topicDesc = getTopicDesc(createdTopic.name());
    Assertions.assertEquals(
        Integer.parseInt(createdTopic.properties().get(PARTITION_COUNT)),
        topicDesc.partitions().size());
    Assertions.assertEquals(
        Integer.parseInt(createdTopic.properties().get(REPLICATION_FACTOR)),
        topicDesc.partitions().get(0).replicas().size());

    // get properties from Kafka directly
    ConfigResource configResource =
        new ConfigResource(ConfigResource.Type.TOPIC, createdTopic.name());
    Config topicConfigs =
        adminClient
            .describeConfigs(Collections.singleton(configResource))
            .all()
            .get()
            .get(configResource);
    topicConfigs
        .entries()
        .forEach(
            entry ->
                Assertions.assertEquals(
                    entry.value(), createdTopic.properties().get(entry.name())));
  }

  private void checkTopicReadWrite(String topicName) {
    String randomMessage = GravitinoITUtils.genRandomName("message");
    int partition = -1;
    long offset = -1;
    // write message to topic
    try (KafkaProducer<String, String> producer =
        new KafkaProducer<>(
            ImmutableMap.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                kafkaBootstrapServers,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName(),
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName()))) {
      ProducerRecord<String, String> record =
          new ProducerRecord<>(topicName, randomMessage, randomMessage);
      RecordMetadata metadata = producer.send(record).get();
      Assertions.assertEquals(topicName, metadata.topic());
      offset = metadata.offset();
      partition = metadata.partition();
      Assertions.assertTrue(offset >= 0);
      Assertions.assertTrue(partition >= 0);
    } catch (Exception e) {
      throw new RuntimeException("Failed to write message to Kafka", e);
    }

    // read message from topic
    try (KafkaConsumer<String, String> consumer =
        new KafkaConsumer<>(
            ImmutableMap.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                kafkaBootstrapServers,
                ConsumerConfig.GROUP_ID_CONFIG,
                topicName,
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName(),
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName(),
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,
                "false"))) {
      TopicPartition topicPartition = new TopicPartition(topicName, partition);
      consumer.assign(ImmutableList.of(topicPartition));
      consumer.seek(topicPartition, offset);
      ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(30));

      Assertions.assertTrue(records.count() > 0);
      ConsumerRecord<String, String> record = records.iterator().next();
      Assertions.assertEquals(randomMessage, record.key());
      Assertions.assertEquals(randomMessage, record.value());
    } catch (Exception e) {
      throw new RuntimeException("Failed to read message from Kafka", e);
    }
  }

  private TopicDescription getTopicDesc(String topicName)
      throws ExecutionException, InterruptedException {
    return adminClient
        .describeTopics(Collections.singleton(topicName))
        .topicNameValues()
        .get(topicName)
        .get();
  }

  private static void createMetalake() {
    GravitinoMetalake createdMetalake =
        client.createMetalake(METALAKE_NAME, "comment", Collections.emptyMap());
    GravitinoMetalake loadMetalake = client.loadMetalake(METALAKE_NAME);
    Assertions.assertEquals(createdMetalake, loadMetalake);

    metalake = loadMetalake;
  }

  private static Catalog createCatalog(
      String catalogName, String comment, Map<String, String> properties) {
    metalake.createCatalog(catalogName, Catalog.Type.MESSAGING, PROVIDER, comment, properties);
    return metalake.loadCatalog(catalogName);
  }
}
