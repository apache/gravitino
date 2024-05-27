/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.integration.test.web.ui;

import com.datastrato.gravitino.Catalog;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.client.GravitinoAdminClient;
import com.datastrato.gravitino.client.GravitinoMetalake;
import com.datastrato.gravitino.integration.test.container.ContainerSuite;
import com.datastrato.gravitino.integration.test.util.AbstractIT;
import com.datastrato.gravitino.integration.test.web.ui.pages.CatalogsPage;
import com.datastrato.gravitino.integration.test.web.ui.pages.MetalakePage;
import com.datastrato.gravitino.integration.test.web.ui.utils.AbstractWebIT;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

@Tag("gravitino-docker-it")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class CatalogsPageKafkaTest extends AbstractWebIT {
  MetalakePage metalakePage = new MetalakePage();
  CatalogsPage catalogsPage = new CatalogsPage();

  private static final ContainerSuite containerSuite = ContainerSuite.getInstance();
  protected static GravitinoAdminClient gravitinoClient;
  private static GravitinoMetalake metalake;

  protected static String gravitinoUri = "http://127.0.0.1:8090";
  protected static String kafkaUri = "http://127.0.0.1:9092";

  private static final String CATALOG_TABLE_TITLE = "Schemas";
  private static final String SCHEMA_TOPIC_TITLE = "Topics";
  private static final String METALAKE_NAME = "test";
  private static final String CATALOG_TYPE_MESSAGING = "messaging";

  private static final String KAFKA_CATALOG_NAME = "catalog_kafka";
  private static final String SCHEMA_NAME = "default";
  private static final String TOPIC_NAME = "topic1";
  public static final int DEFAULT_BROKER_PORT = 9092;

  @BeforeAll
  public static void before() throws Exception {
    gravitinoClient = AbstractIT.getGravitinoClient();

    gravitinoUri = String.format("http://127.0.0.1:%d", AbstractIT.getGravitinoServerPort());

    containerSuite.startKafkaContainer();

    String address = containerSuite.getKafkaContainer().getContainerIpAddress();
    kafkaUri = String.format("%s:%d", address, DEFAULT_BROKER_PORT);
  }

  /**
   * Creates a Kafka topic within the specified Metalake, Catalog, Schema, and Topic names.
   *
   * @param metalakeName The name of the Metalake.
   * @param catalogName The name of the Catalog.
   * @param schemaName The name of the Schema.
   * @param topicName The name of the Kafka topic.
   */
  void createTopic(String metalakeName, String catalogName, String schemaName, String topicName) {
    Catalog catalog_kafka =
        metalake.loadCatalog(NameIdentifier.ofCatalog(metalakeName, catalogName));
    catalog_kafka
        .asTopicCatalog()
        .createTopic(
            NameIdentifier.of(metalakeName, catalogName, schemaName, topicName),
            "comment",
            null,
            Collections.emptyMap());
  }

  /**
   * Drops a Kafka topic from the specified Metalake, Catalog, and Schema.
   *
   * @param metalakeName The name of the Metalake where the topic resides.
   * @param catalogName The name of the Catalog that contains the topic.
   * @param schemaName The name of the Schema under which the topic exists.
   * @param topicName The name of the Kafka topic to be dropped.
   */
  void dropTopic(String metalakeName, String catalogName, String schemaName, String topicName) {
    Catalog catalog_kafka =
        metalake.loadCatalog(NameIdentifier.ofCatalog(metalakeName, catalogName));
    catalog_kafka
        .asTopicCatalog()
        .dropTopic(NameIdentifier.of(metalakeName, catalogName, schemaName, topicName));
  }

  @Test
  @Order(0)
  public void testCreateKafkaCatalog() throws InterruptedException {
    // create metalake
    clickAndWait(metalakePage.createMetalakeBtn);
    metalakePage.setMetalakeNameField(METALAKE_NAME);
    clickAndWait(metalakePage.submitHandleMetalakeBtn);
    // load metalake
    metalake = gravitinoClient.loadMetalake(NameIdentifier.of(METALAKE_NAME));
    metalakePage.clickMetalakeLink(METALAKE_NAME);
    // create kafka catalog actions
    clickAndWait(catalogsPage.createCatalogBtn);
    catalogsPage.setCatalogNameField(KAFKA_CATALOG_NAME);
    clickAndWait(catalogsPage.catalogTypeSelector);
    catalogsPage.clickSelectType(CATALOG_TYPE_MESSAGING);
    catalogsPage.setCatalogCommentField("kafka catalog comment");
    // set kafka catalog props
    catalogsPage.setCatalogFixedProp("bootstrap.servers", kafkaUri);
    clickAndWait(catalogsPage.handleSubmitCatalogBtn);
    Assertions.assertTrue(catalogsPage.verifyGetCatalog(KAFKA_CATALOG_NAME));
  }

  @Test
  @Order(1)
  public void testKafkaSchemaTreeNode() throws InterruptedException {
    // click kafka catalog tree node
    String kafkaCatalogNode =
        String.format(
            "{{%s}}{{%s}}{{%s}}", METALAKE_NAME, KAFKA_CATALOG_NAME, CATALOG_TYPE_MESSAGING);
    catalogsPage.clickTreeNode(kafkaCatalogNode);
    // verify show table title、 schema name and tree node
    Assertions.assertTrue(catalogsPage.verifyShowTableTitle(CATALOG_TABLE_TITLE));
    Assertions.assertTrue(catalogsPage.verifyShowDataItemInList(SCHEMA_NAME, false));
    List<String> treeNodes = Arrays.asList(KAFKA_CATALOG_NAME, SCHEMA_NAME);
    Assertions.assertTrue(catalogsPage.verifyTreeNodes(treeNodes));
  }

  @Test
  @Order(2)
  public void testKafkaTopicTreeNode() throws InterruptedException {
    // 1. create topic of kafka catalog
    createTopic(METALAKE_NAME, KAFKA_CATALOG_NAME, SCHEMA_NAME, TOPIC_NAME);
    // 2. click schema tree node
    String kafkaSchemaNode =
        String.format(
            "{{%s}}{{%s}}{{%s}}{{%s}}",
            METALAKE_NAME, KAFKA_CATALOG_NAME, CATALOG_TYPE_MESSAGING, SCHEMA_NAME);
    catalogsPage.clickTreeNode(kafkaSchemaNode);
    // 3. verify show table title、 default schema name and tree node
    Assertions.assertTrue(catalogsPage.verifyShowTableTitle(SCHEMA_TOPIC_TITLE));
    Assertions.assertTrue(catalogsPage.verifyShowDataItemInList(TOPIC_NAME, false));
    List<String> treeNodes = Arrays.asList(KAFKA_CATALOG_NAME, SCHEMA_NAME, TOPIC_NAME);
    Assertions.assertTrue(catalogsPage.verifyTreeNodes(treeNodes));
  }

  @Test
  @Order(3)
  public void testKafkaTopicDetail() throws InterruptedException {
    // 1. click topic tree node
    String topicNode =
        String.format(
            "{{%s}}{{%s}}{{%s}}{{%s}}{{%s}}",
            METALAKE_NAME, KAFKA_CATALOG_NAME, CATALOG_TYPE_MESSAGING, SCHEMA_NAME, TOPIC_NAME);
    catalogsPage.clickTreeNode(topicNode);
    // 2. verify show tab details
    Assertions.assertTrue(catalogsPage.verifyShowDetailsContent());
    // 3. verify show highlight properties
    Assertions.assertTrue(
        catalogsPage.verifyShowPropertiesItemInList(
            "key", "partition-count", "partition-count", true));
    Assertions.assertTrue(
        catalogsPage.verifyShowPropertiesItemInList("value", "partition-count", "1", true));
    Assertions.assertTrue(
        catalogsPage.verifyShowPropertiesItemInList(
            "key", "replication-factor", "replication-factor", true));
    Assertions.assertTrue(
        catalogsPage.verifyShowPropertiesItemInList("value", "replication-factor", "1", true));
  }

  @Test
  @Order(4)
  public void testDropKafkaTopic() throws InterruptedException {
    // delete topic of kafka catalog
    dropTopic(METALAKE_NAME, KAFKA_CATALOG_NAME, SCHEMA_NAME, TOPIC_NAME);
    // click schema tree node
    String kafkaSchemaNode =
        String.format(
            "{{%s}}{{%s}}{{%s}}{{%s}}",
            METALAKE_NAME, KAFKA_CATALOG_NAME, CATALOG_TYPE_MESSAGING, SCHEMA_NAME);
    catalogsPage.clickTreeNode(kafkaSchemaNode);
    // verify empty topic list
    Assertions.assertTrue(catalogsPage.verifyEmptyTableData());
  }

  @Test
  @Order(5)
  public void testBackHomePage() throws InterruptedException {
    clickAndWait(catalogsPage.backHomeBtn);
    Assertions.assertTrue(catalogsPage.verifyBackHomePage());
  }
}
