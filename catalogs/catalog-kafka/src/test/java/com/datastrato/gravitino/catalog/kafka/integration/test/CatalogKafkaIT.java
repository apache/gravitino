/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.kafka.integration.test;

import static com.datastrato.gravitino.catalog.kafka.KafkaCatalogPropertiesMetadata.BOOTSTRAP_SERVERS;
import static com.datastrato.gravitino.catalog.kafka.KafkaTopicPropertiesMetadata.PARTITION_COUNT;
import static com.datastrato.gravitino.catalog.kafka.KafkaTopicPropertiesMetadata.REPLICATION_FACTOR;
import static com.datastrato.gravitino.integration.test.container.KafkaContainer.DEFAULT_BROKER_PORT;

import com.datastrato.gravitino.Catalog;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.client.GravitinoMetalake;
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
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.Config;
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
  public static void startUp() {
    CONTAINER_SUITE.startKafkaContainer();
    kafkaBootstrapServers =
        String.format(
            "%s:%d",
            CONTAINER_SUITE.getKafkaContainer().getContainerIpAddress(), DEFAULT_BROKER_PORT);
    adminClient = AdminClient.create(ImmutableMap.of(BOOTSTRAP_SERVERS, kafkaBootstrapServers));

    createMetalake();
    createCatalog();
  }

  @AfterAll
  public static void shutdown() {
    // todo: add drop catalog after it's supported
    client.dropMetalake(NameIdentifier.of(METALAKE_NAME));
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
    Assertions.assertEquals(1, topics.length);
    Assertions.assertEquals(topicName, topics[0].name());
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
  public void testDropTopic() {
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
        client.createMetalake(NameIdentifier.of(METALAKE_NAME), "comment", Collections.emptyMap());
    GravitinoMetalake loadMetalake = client.loadMetalake(NameIdentifier.of(METALAKE_NAME));
    Assertions.assertEquals(createdMetalake, loadMetalake);

    metalake = loadMetalake;
  }

  private static void createCatalog() {
    Map<String, String> properties = Maps.newHashMap();
    properties.put(BOOTSTRAP_SERVERS, kafkaBootstrapServers);
    metalake.createCatalog(
        NameIdentifier.of(METALAKE_NAME, CATALOG_NAME),
        Catalog.Type.MESSAGING,
        PROVIDER,
        "comment",
        properties);
    catalog = metalake.loadCatalog(NameIdentifier.of(METALAKE_NAME, CATALOG_NAME));
  }
}
