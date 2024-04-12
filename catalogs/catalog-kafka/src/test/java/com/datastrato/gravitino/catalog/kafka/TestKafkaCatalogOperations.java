/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.kafka;

import static com.datastrato.gravitino.Catalog.Type.MESSAGING;
import static com.datastrato.gravitino.Configs.DEFAULT_ENTITY_KV_STORE;
import static com.datastrato.gravitino.Configs.ENTITY_KV_STORE;
import static com.datastrato.gravitino.Configs.ENTITY_STORE;
import static com.datastrato.gravitino.Configs.ENTRY_KV_ROCKSDB_BACKEND_PATH;
import static com.datastrato.gravitino.Configs.STORE_DELETE_AFTER_TIME;
import static com.datastrato.gravitino.Configs.STORE_TRANSACTION_MAX_SKEW_TIME;
import static com.datastrato.gravitino.StringIdentifier.ID_KEY;
import static com.datastrato.gravitino.catalog.kafka.KafkaCatalogOperations.CLIENT_ID_TEMPLATE;
import static com.datastrato.gravitino.catalog.kafka.KafkaCatalogPropertiesMetadata.BOOTSTRAP_SERVERS;
import static com.datastrato.gravitino.catalog.kafka.KafkaTopicPropertiesMetadata.PARTITION_COUNT;
import static com.datastrato.gravitino.catalog.kafka.KafkaTopicPropertiesMetadata.REPLICATION_FACTOR;

import com.datastrato.gravitino.Config;
import com.datastrato.gravitino.Configs;
import com.datastrato.gravitino.EntitySerDeFactory;
import com.datastrato.gravitino.EntityStore;
import com.datastrato.gravitino.EntityStoreFactory;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.catalog.kafka.embedded.KafkaClusterEmbedded;
import com.datastrato.gravitino.connector.BasePropertiesMetadata;
import com.datastrato.gravitino.exceptions.NoSuchSchemaException;
import com.datastrato.gravitino.exceptions.NoSuchTopicException;
import com.datastrato.gravitino.exceptions.TopicAlreadyExistsException;
import com.datastrato.gravitino.messaging.Topic;
import com.datastrato.gravitino.messaging.TopicChange;
import com.datastrato.gravitino.meta.AuditInfo;
import com.datastrato.gravitino.meta.CatalogEntity;
import com.datastrato.gravitino.rel.Schema;
import com.datastrato.gravitino.rel.SchemaChange;
import com.datastrato.gravitino.storage.IdGenerator;
import com.datastrato.gravitino.storage.RandomIdGenerator;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.time.Instant;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.kafka.common.config.TopicConfig;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TestKafkaCatalogOperations extends KafkaClusterEmbedded {

  private static final String ROCKS_DB_STORE_PATH =
      "/tmp/gravitino_test_entityStore_" + genRandomString();
  private static final String METALAKE_NAME = "metalake";
  private static final String CATALOG_NAME = "test_kafka_catalog";
  private static final String DEFAULT_SCHEMA_NAME = "default";
  private static final Map<String, String> MOCK_CATALOG_PROPERTIES =
      ImmutableMap.of(BOOTSTRAP_SERVERS, brokerList(), ID_KEY, "gravitino.v1.uid33220758755757000");
  private static EntityStore store;
  private static IdGenerator idGenerator;
  private static CatalogEntity kafkaCatalogEntity;
  private static KafkaCatalogOperations kafkaCatalogOperations;

  @BeforeAll
  public static void setUp() {
    Config config = Mockito.mock(Config.class);
    Mockito.when(config.get(ENTITY_STORE)).thenReturn("kv");
    Mockito.when(config.get(ENTITY_KV_STORE)).thenReturn(DEFAULT_ENTITY_KV_STORE);
    Mockito.when(config.get(Configs.ENTITY_SERDE)).thenReturn("proto");
    Mockito.when(config.get(ENTRY_KV_ROCKSDB_BACKEND_PATH)).thenReturn(ROCKS_DB_STORE_PATH);

    Assertions.assertEquals(ROCKS_DB_STORE_PATH, config.get(ENTRY_KV_ROCKSDB_BACKEND_PATH));
    Mockito.when(config.get(STORE_TRANSACTION_MAX_SKEW_TIME)).thenReturn(1000L);
    Mockito.when(config.get(STORE_DELETE_AFTER_TIME)).thenReturn(20 * 60 * 1000L);

    store = EntityStoreFactory.createEntityStore(config);
    store.initialize(config);
    store.setSerDe(EntitySerDeFactory.createEntitySerDe(config));
    idGenerator = new RandomIdGenerator();
    kafkaCatalogEntity =
        CatalogEntity.builder()
            .withId(1L)
            .withName(CATALOG_NAME)
            .withNamespace(Namespace.of(METALAKE_NAME))
            .withType(MESSAGING)
            .withProvider("kafka")
            .withAuditInfo(
                AuditInfo.builder()
                    .withCreator("testKafkaUser")
                    .withCreateTime(Instant.now())
                    .build())
            .build();

    kafkaCatalogOperations = new KafkaCatalogOperations(store, idGenerator);
    kafkaCatalogOperations.initialize(MOCK_CATALOG_PROPERTIES, kafkaCatalogEntity.toCatalogInfo());
  }

  @AfterAll
  public static void tearDown() throws IOException {
    if (store != null) {
      store.close();
      FileUtils.deleteDirectory(FileUtils.getFile(ROCKS_DB_STORE_PATH));
    }
  }

  @Test
  public void testKafkaCatalogConfiguration() {
    String catalogName = "test_kafka_catalog_configuration";
    CatalogEntity catalogEntity =
        CatalogEntity.builder()
            .withId(2L)
            .withName(catalogName)
            .withNamespace(Namespace.of(METALAKE_NAME))
            .withType(MESSAGING)
            .withProvider("kafka")
            .withAuditInfo(
                AuditInfo.builder()
                    .withCreator("testKafkaUser")
                    .withCreateTime(Instant.now())
                    .build())
            .build();
    KafkaCatalogOperations ops = new KafkaCatalogOperations(store, idGenerator);
    Assertions.assertNull(ops.adminClientConfig);

    ops.initialize(MOCK_CATALOG_PROPERTIES, catalogEntity.toCatalogInfo());
    Assertions.assertNotNull(ops.adminClientConfig);
    Assertions.assertEquals(2, ops.adminClientConfig.size());
    Assertions.assertEquals(
        MOCK_CATALOG_PROPERTIES.get(BOOTSTRAP_SERVERS),
        ops.adminClientConfig.get(BOOTSTRAP_SERVERS));
    Assertions.assertEquals(
        String.format(
            CLIENT_ID_TEMPLATE,
            MOCK_CATALOG_PROPERTIES.get(ID_KEY),
            catalogEntity.namespace(),
            catalogName),
        ops.adminClientConfig.get("client.id"));
  }

  @Test
  public void testInitialization() {
    String catalogName = "test_kafka_catalog_initialization";
    CatalogEntity catalogEntity =
        CatalogEntity.builder()
            .withId(2L)
            .withName(catalogName)
            .withNamespace(Namespace.of(METALAKE_NAME))
            .withType(MESSAGING)
            .withProvider("kafka")
            .withAuditInfo(
                AuditInfo.builder()
                    .withCreator("testKafkaUser")
                    .withCreateTime(Instant.now())
                    .build())
            .build();
    KafkaCatalogOperations ops = new KafkaCatalogOperations(store, idGenerator);
    ops.initialize(MOCK_CATALOG_PROPERTIES, catalogEntity.toCatalogInfo());

    Assertions.assertNotNull(ops.defaultSchemaIdent);
    Assertions.assertEquals(DEFAULT_SCHEMA_NAME, ops.defaultSchemaIdent.name());
    Assertions.assertEquals(
        METALAKE_NAME + "." + catalogName, ops.defaultSchemaIdent.namespace().toString());

    Assertions.assertTrue(ops.schemaExists(ops.defaultSchemaIdent));
    Schema schema = ops.loadSchema(ops.defaultSchemaIdent);
    Assertions.assertEquals(DEFAULT_SCHEMA_NAME, schema.name());
  }

  @Test
  public void testCreateSchema() {
    NameIdentifier ident = NameIdentifier.of(METALAKE_NAME, CATALOG_NAME, "test_schema");

    UnsupportedOperationException exception =
        Assertions.assertThrows(
            UnsupportedOperationException.class,
            () -> kafkaCatalogOperations.createSchema(ident, null, null));
    Assertions.assertEquals(
        "Kafka catalog does not support schema creation because the \"default\" schema already includes all topics",
        exception.getMessage());
  }

  @Test
  public void testLoadSchema() {
    NameIdentifier ident = NameIdentifier.of(METALAKE_NAME, CATALOG_NAME, DEFAULT_SCHEMA_NAME);
    Schema schema = kafkaCatalogOperations.loadSchema(ident);

    Assertions.assertEquals(DEFAULT_SCHEMA_NAME, schema.name());
    Assertions.assertEquals(
        "The default schema of Kafka catalog including all topics", schema.comment());
    Assertions.assertEquals(2, schema.properties().size());
    Assertions.assertTrue(
        schema.properties().containsKey(BasePropertiesMetadata.GRAVITINO_MANAGED_ENTITY));
    Assertions.assertEquals("true", schema.properties().get("gravitino.managed.entity"));
  }

  @Test
  public void testAlterSchema() {
    Exception exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                kafkaCatalogOperations.alterSchema(
                    NameIdentifier.of(METALAKE_NAME, CATALOG_NAME, DEFAULT_SCHEMA_NAME),
                    SchemaChange.removeProperty("key1")));
    Assertions.assertEquals("Cannot alter the default schema", exception.getMessage());

    exception =
        Assertions.assertThrows(
            UnsupportedOperationException.class,
            () ->
                kafkaCatalogOperations.alterSchema(
                    NameIdentifier.of(METALAKE_NAME, CATALOG_NAME, "test_schema"),
                    SchemaChange.removeProperty("key1")));
    Assertions.assertEquals(
        "Kafka catalog does not support schema alteration", exception.getMessage());
  }

  @Test
  public void testDropSchema() {
    Exception exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                kafkaCatalogOperations.dropSchema(
                    NameIdentifier.of(METALAKE_NAME, CATALOG_NAME, DEFAULT_SCHEMA_NAME), true));
    Assertions.assertEquals("Cannot drop the default schema", exception.getMessage());

    NameIdentifier ident = NameIdentifier.of(METALAKE_NAME, CATALOG_NAME, "test_schema");
    exception =
        Assertions.assertThrows(
            UnsupportedOperationException.class,
            () -> kafkaCatalogOperations.dropSchema(ident, true));
    Assertions.assertEquals(
        "Kafka catalog does not support schema deletion", exception.getMessage());
  }

  @Test
  public void testCreateTopic() {
    NameIdentifier ident =
        NameIdentifier.of(METALAKE_NAME, CATALOG_NAME, DEFAULT_SCHEMA_NAME, "test_create_topic");
    String comment = "test comment";
    Map<String, String> properties =
        ImmutableMap.of(
            PARTITION_COUNT,
            "3",
            REPLICATION_FACTOR,
            "1",
            TopicConfig.COMPRESSION_TYPE_CONFIG,
            "producer");
    Topic createdTopic = kafkaCatalogOperations.createTopic(ident, comment, null, properties);
    Assertions.assertNotNull(createdTopic);
    Assertions.assertEquals(ident.name(), createdTopic.name());
    Assertions.assertEquals("3", createdTopic.properties().get(PARTITION_COUNT));
    Assertions.assertEquals("1", createdTopic.properties().get(REPLICATION_FACTOR));
    Assertions.assertEquals(
        "producer", createdTopic.properties().get(TopicConfig.COMPRESSION_TYPE_CONFIG));
    Assertions.assertNotNull(createdTopic.properties().get(ID_KEY));
  }

  @Test
  public void testCreateTopicException() {
    Map<String, String> properties = ImmutableMap.of(PARTITION_COUNT, "3", REPLICATION_FACTOR, "1");

    // test topic already exists
    Exception exception =
        Assertions.assertThrows(
            TopicAlreadyExistsException.class,
            () ->
                kafkaCatalogOperations.createTopic(
                    NameIdentifier.of(METALAKE_NAME, CATALOG_NAME, DEFAULT_SCHEMA_NAME, TOPIC_1),
                    null,
                    null,
                    properties));
    Assertions.assertEquals(
        "Topic metalake.test_kafka_catalog.default.kafka-test-topic-1 already exists",
        exception.getMessage());

    // test schema not exists
    exception =
        Assertions.assertThrows(
            NoSuchSchemaException.class,
            () ->
                kafkaCatalogOperations.createTopic(
                    NameIdentifier.of(METALAKE_NAME, CATALOG_NAME, "test_schema", "error_topic"),
                    null,
                    null,
                    properties));
    Assertions.assertEquals(
        "Schema metalake.test_kafka_catalog.test_schema does not exist", exception.getMessage());

    Map<String, String> wrongProperties =
        ImmutableMap.of(PARTITION_COUNT, "3", REPLICATION_FACTOR, "3");
    exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                kafkaCatalogOperations.createTopic(
                    NameIdentifier.of(
                        METALAKE_NAME, CATALOG_NAME, DEFAULT_SCHEMA_NAME, "error_topic"),
                    null,
                    null,
                    wrongProperties));
    Assertions.assertTrue(
        exception.getMessage().contains("Invalid replication factor for topic"),
        exception.getMessage());
  }

  @Test
  public void testLoadTopic() {
    Topic topic =
        kafkaCatalogOperations.loadTopic(
            NameIdentifier.of(METALAKE_NAME, CATALOG_NAME, DEFAULT_SCHEMA_NAME, TOPIC_1));
    Assertions.assertNotNull(topic);
    Assertions.assertEquals(TOPIC_1, topic.name());
    Assertions.assertEquals("1", topic.properties().get(PARTITION_COUNT));
    Assertions.assertEquals("1", topic.properties().get(REPLICATION_FACTOR));
    Assertions.assertNotNull(topic.properties().get(ID_KEY));
    Assertions.assertTrue(topic.properties().size() > 2);
  }

  @Test
  public void testLoadTopicException() {
    Exception exception =
        Assertions.assertThrows(
            NoSuchTopicException.class,
            () ->
                kafkaCatalogOperations.loadTopic(
                    NameIdentifier.of(
                        METALAKE_NAME, CATALOG_NAME, DEFAULT_SCHEMA_NAME, "error_topic")));
    Assertions.assertEquals(
        "Topic metalake.test_kafka_catalog.default.error_topic does not exist",
        exception.getMessage());
  }

  @Test
  public void testListTopics() {
    NameIdentifier[] topics =
        kafkaCatalogOperations.listTopics(
            Namespace.of(METALAKE_NAME, CATALOG_NAME, DEFAULT_SCHEMA_NAME));
    Assertions.assertTrue(topics.length > 0);

    Exception exception =
        Assertions.assertThrows(
            NoSuchSchemaException.class,
            () ->
                kafkaCatalogOperations.listTopics(
                    Namespace.of(METALAKE_NAME, CATALOG_NAME, "error_schema")));
    Assertions.assertEquals(
        "Schema metalake.test_kafka_catalog.error_schema does not exist", exception.getMessage());
  }

  @Test
  public void testDropTopic() {
    NameIdentifier ident =
        NameIdentifier.of(METALAKE_NAME, CATALOG_NAME, DEFAULT_SCHEMA_NAME, "test_drop_topic");
    Map<String, String> properties = ImmutableMap.of(PARTITION_COUNT, "3", REPLICATION_FACTOR, "1");
    kafkaCatalogOperations.createTopic(ident, null, null, properties);
    Assertions.assertNotNull(kafkaCatalogOperations.loadTopic(ident));

    Assertions.assertTrue(kafkaCatalogOperations.dropTopic(ident));
    Exception exception =
        Assertions.assertThrows(
            NoSuchTopicException.class, () -> kafkaCatalogOperations.loadTopic(ident));
    Assertions.assertEquals(
        "Topic metalake.test_kafka_catalog.default.test_drop_topic does not exist",
        exception.getMessage());

    Assertions.assertFalse(kafkaCatalogOperations.dropTopic(ident));
  }

  @Test
  public void testAlterTopic() {
    NameIdentifier ident =
        NameIdentifier.of(METALAKE_NAME, CATALOG_NAME, DEFAULT_SCHEMA_NAME, "test_alter_topic");
    Map<String, String> properties =
        ImmutableMap.of(
            PARTITION_COUNT,
            "2",
            REPLICATION_FACTOR,
            "1",
            TopicConfig.COMPRESSION_TYPE_CONFIG,
            "gzip",
            TopicConfig.RETENTION_MS_CONFIG,
            "43200000");
    Topic createdTopic = kafkaCatalogOperations.createTopic(ident, null, null, properties);

    Topic alteredTopic =
        kafkaCatalogOperations.alterTopic(
            ident,
            TopicChange.updateComment("new comment"),
            TopicChange.setProperty(PARTITION_COUNT, "3"),
            TopicChange.setProperty(TopicConfig.COMPRESSION_TYPE_CONFIG, "producer"),
            TopicChange.removeProperty(TopicConfig.RETENTION_MS_CONFIG));
    Assertions.assertEquals(createdTopic.name(), alteredTopic.name());
    Assertions.assertEquals("new comment", alteredTopic.comment());
    Assertions.assertEquals("3", alteredTopic.properties().get(PARTITION_COUNT));
    Assertions.assertEquals("1", alteredTopic.properties().get(REPLICATION_FACTOR));
    Assertions.assertEquals(
        "producer", alteredTopic.properties().get(TopicConfig.COMPRESSION_TYPE_CONFIG));
    Assertions.assertNull(alteredTopic.properties().get(TopicConfig.RETENTION_MS_CONFIG));

    // reload topic and check if the changes are applied
    alteredTopic = kafkaCatalogOperations.loadTopic(ident);
    Assertions.assertEquals(createdTopic.name(), alteredTopic.name());
    // comment is null because it is not stored in the topic
    Assertions.assertNull(alteredTopic.comment());
    Assertions.assertEquals("3", alteredTopic.properties().get(PARTITION_COUNT));
    Assertions.assertEquals("1", alteredTopic.properties().get(REPLICATION_FACTOR));
    Assertions.assertNotNull(alteredTopic.properties().get(ID_KEY));
    Assertions.assertEquals(
        "producer", alteredTopic.properties().get(TopicConfig.COMPRESSION_TYPE_CONFIG));
    // retention.ms overridden was removed, so it should be the default value
    Assertions.assertEquals(
        "604800000", alteredTopic.properties().get(TopicConfig.RETENTION_MS_CONFIG));

    // test exception
    Exception exception =
        Assertions.assertThrows(
            NoSuchTopicException.class,
            () ->
                kafkaCatalogOperations.alterTopic(
                    NameIdentifier.of(
                        METALAKE_NAME, CATALOG_NAME, DEFAULT_SCHEMA_NAME, "error_topic"),
                    TopicChange.updateComment("new comment")));
    Assertions.assertEquals(
        "Topic metalake.test_kafka_catalog.default.error_topic does not exist",
        exception.getMessage());

    exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                kafkaCatalogOperations.alterTopic(
                    ident, TopicChange.removeProperty(PARTITION_COUNT)));
    Assertions.assertEquals("Cannot remove partition count", exception.getMessage());

    exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                kafkaCatalogOperations.alterTopic(
                    ident, TopicChange.setProperty(PARTITION_COUNT, "1")));
    Assertions.assertEquals("Cannot reduce partition count from 3 to 1", exception.getMessage());
  }
}
