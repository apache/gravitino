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
import static com.datastrato.gravitino.Configs.KV_DELETE_AFTER_TIME;
import static com.datastrato.gravitino.Configs.STORE_TRANSACTION_MAX_SKEW_TIME;
import static com.datastrato.gravitino.StringIdentifier.ID_KEY;
import static com.datastrato.gravitino.catalog.kafka.KafkaCatalogPropertiesMetadata.BOOTSTRAP_SERVERS;

import com.datastrato.gravitino.Config;
import com.datastrato.gravitino.Configs;
import com.datastrato.gravitino.EntitySerDeFactory;
import com.datastrato.gravitino.EntityStore;
import com.datastrato.gravitino.EntityStoreFactory;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.connector.BasePropertiesMetadata;
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
import java.util.UUID;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TestKafkaCatalogOperations {

  private static final String ROCKS_DB_STORE_PATH =
      "/tmp/gravitino_test_entityStore_" + UUID.randomUUID().toString().replace("-", "");
  private static final String METALAKE_NAME = "metalake";
  private static final String CATALOG_NAME = "test_kafka_catalog";
  private static final Map<String, String> MOCK_CATALOG_PROPERTIES =
      ImmutableMap.of(
          BOOTSTRAP_SERVERS, "localhost:9092", ID_KEY, "gravitino.v1.uid33220758755757000");
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
    Mockito.when(config.get(KV_DELETE_AFTER_TIME)).thenReturn(20 * 60 * 1000L);

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
    store.close();
    FileUtils.deleteDirectory(FileUtils.getFile(ROCKS_DB_STORE_PATH));
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
        MOCK_CATALOG_PROPERTIES.get(ID_KEY), ops.adminClientConfig.get("client.id"));
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
    Assertions.assertEquals("default", ops.defaultSchemaIdent.name());
    Assertions.assertEquals(
        METALAKE_NAME + "." + catalogName, ops.defaultSchemaIdent.namespace().toString());

    Assertions.assertTrue(ops.schemaExists(ops.defaultSchemaIdent));
    Schema schema = ops.loadSchema(ops.defaultSchemaIdent);
    Assertions.assertEquals("default", schema.name());
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
    NameIdentifier ident = NameIdentifier.of(METALAKE_NAME, CATALOG_NAME, "default");
    Schema schema = kafkaCatalogOperations.loadSchema(ident);

    Assertions.assertEquals("default", schema.name());
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
                    NameIdentifier.of(METALAKE_NAME, CATALOG_NAME, "default"),
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
                    NameIdentifier.of(METALAKE_NAME, CATALOG_NAME, "default"), true));
    Assertions.assertEquals("Cannot drop the default schema", exception.getMessage());

    NameIdentifier ident = NameIdentifier.of(METALAKE_NAME, CATALOG_NAME, "test_schema");
    exception =
        Assertions.assertThrows(
            UnsupportedOperationException.class,
            () -> kafkaCatalogOperations.dropSchema(ident, true));
    Assertions.assertEquals(
        "Kafka catalog does not support schema deletion", exception.getMessage());
  }
}
