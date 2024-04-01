/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.integration.test.web.ui;

import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.client.GravitinoAdminClient;
import com.datastrato.gravitino.integration.test.container.ContainerSuite;
import com.datastrato.gravitino.integration.test.container.TrinoITContainers;
import com.datastrato.gravitino.integration.test.util.AbstractIT;
import com.datastrato.gravitino.integration.test.web.ui.pages.CatalogsPage;
import com.datastrato.gravitino.integration.test.web.ui.pages.MetalakePage;
import com.datastrato.gravitino.integration.test.web.ui.utils.AbstractWebIT;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

@Tag("gravitino-docker-it")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class CatalogsPageTest extends AbstractWebIT {
  protected static String gravitinoUri = "http://127.0.0.1:8090";
  protected static String trinoUri = "http://127.0.0.1:8080";
  protected static String hiveMetastoreUri = "thrift://127.0.0.1:9083";
  protected static String hdfsUri = "hdfs://127.0.0.1:9000";
  protected static String mysqlUri = "jdbc:mysql://127.0.0.1";
  protected static String postgresqlUri = "jdbc:postgresql://127.0.0.1";
  protected static TrinoITContainers trinoITContainers;
  protected static GravitinoAdminClient gravitinoClient;
  static MetalakePage metalakePage = new MetalakePage();
  CatalogsPage catalogsPage = new CatalogsPage();

  private static final String metalakeName = "test";
  private static final String metalakeSelectName = "metalake_select_name";
  String catalogType = "relational";
  String catalogName = "catalog_name";
  String modifiedCatalogName = catalogName + "_edited";
  String schemaName = "default";
  String tableName = "table";
  String icebergCatalogName = "catalog_iceberg";
  String mysqlCatalogName = "catalog_mysql";
  String pgCatalogName = "catalog_pg";

  String filesetCatalogName = "catalog_fileset";

  @BeforeAll
  public static void before() throws Exception {
    gravitinoClient = AbstractIT.getGravitinoClient();

    gravitinoUri = String.format("http://127.0.0.1:%d", AbstractIT.getGravitinoServerPort());

    trinoITContainers = ContainerSuite.getTrinoITContainers();
    trinoITContainers.launch(AbstractIT.getGravitinoServerPort());

    trinoUri = trinoITContainers.getTrinoUri();
    hiveMetastoreUri = trinoITContainers.getHiveMetastoreUri();
    hdfsUri = trinoITContainers.getHdfsUri();
    mysqlUri = trinoITContainers.getMysqlUri();
    postgresqlUri = trinoITContainers.getPostgresqlUri();
  }

  @AfterAll
  public static void after() {
    try {
      if (trinoITContainers != null) trinoITContainers.shutdown();
    } catch (Exception e) {
      LOG.error("shutdown trino containers error", e);
    }
  }

  @Test
  @Order(0)
  public void testDeleteCatalog() throws InterruptedException {
    // create metalake
    clickAndWait(metalakePage.createMetalakeBtn);
    metalakePage.setMetalakeNameField(metalakeName);
    clickAndWait(metalakePage.submitHandleMetalakeBtn);
    // Create another metalake for select option
    clickAndWait(metalakePage.createMetalakeBtn);
    metalakePage.setMetalakeNameField(metalakeSelectName);
    clickAndWait(metalakePage.submitHandleMetalakeBtn);

    gravitinoClient.loadMetalake(NameIdentifier.of(metalakeName));
    metalakePage.clickMetalakeLink(metalakeName);
    // create catalog
    String defaultCatalogName = "default_catalog";
    clickAndWait(catalogsPage.createCatalogBtn);
    catalogsPage.setCatalogNameField(defaultCatalogName);
    catalogsPage.setCatalogPropInput("metastore.uris", hiveMetastoreUri);
    clickAndWait(catalogsPage.handleSubmitCatalogBtn);
    // delete catalog
    catalogsPage.clickDeleteCatalogBtn(defaultCatalogName);
    clickAndWait(catalogsPage.confirmDeleteBtn);
    Assertions.assertTrue(catalogsPage.verifyEmptyCatalog());
  }

  @Test
  @Order(1)
  public void testCreateHiveCatalog() throws InterruptedException {
    // Create catalog
    clickAndWait(catalogsPage.createCatalogBtn);
    catalogsPage.setCatalogNameField(catalogName);
    catalogsPage.setCatalogCommentField("catalog comment");
    catalogsPage.setCatalogPropInput("metastore.uris", hiveMetastoreUri);
    catalogsPage.addCatalogPropsBtn.click();
    catalogsPage.setCatalogProps(1, "key1", "value1");
    catalogsPage.addCatalogPropsBtn.click();
    catalogsPage.setCatalogProps(2, "key2", "value2");
    clickAndWait(catalogsPage.handleSubmitCatalogBtn);
    Assertions.assertTrue(catalogsPage.verifyCreateCatalog(catalogName));
  }

  @Test
  @Order(2)
  public void testCreateIcebergCatalog() throws InterruptedException {
    clickAndWait(catalogsPage.createCatalogBtn);
    catalogsPage.setCatalogNameField(icebergCatalogName);
    // select provider as iceberg
    clickAndWait(catalogsPage.catalogProviderSelector);
    catalogsPage.clickSelectProvider("lakehouse-iceberg");
    catalogsPage.setCatalogCommentField("iceberg catalog comment");
    // set iceberg uri
    catalogsPage.setCatalogPropInput("uri", hiveMetastoreUri);
    // set iceberg warehouse
    catalogsPage.setCatalogPropInput("warehouse", hdfsUri);
    clickAndWait(catalogsPage.handleSubmitCatalogBtn);
    Assertions.assertTrue(catalogsPage.verifyCreateCatalog(icebergCatalogName));
  }

  @Test
  @Order(3)
  public void testCreateMysqlCatalog() throws InterruptedException {
    // create mysql catalog actions
    clickAndWait(catalogsPage.createCatalogBtn);
    catalogsPage.setCatalogNameField(mysqlCatalogName);
    // select provider as mysql
    clickAndWait(catalogsPage.catalogProviderSelector);
    catalogsPage.clickSelectProvider("jdbc-mysql");
    catalogsPage.setCatalogCommentField("mysql catalog comment");
    // set mysql catalog props
    catalogsPage.setCatalogPropInput("jdbc-driver", "com.mysql.cj.jdbc.Driver");
    catalogsPage.setCatalogPropInput("jdbc-url", mysqlUri);
    catalogsPage.setCatalogPropInput("jdbc-user", "trino");
    catalogsPage.setCatalogPropInput("jdbc-password", "ds123");
    clickAndWait(catalogsPage.handleSubmitCatalogBtn);
    Assertions.assertTrue(catalogsPage.verifyCreateCatalog(mysqlCatalogName));
  }

  @Test
  @Order(4)
  public void testCreatePgCatalog() throws InterruptedException {
    // create postgresql catalog actions
    clickAndWait(catalogsPage.createCatalogBtn);
    catalogsPage.setCatalogNameField(pgCatalogName);
    // select provider as mysql
    clickAndWait(catalogsPage.catalogProviderSelector);
    catalogsPage.clickSelectProvider("jdbc-postgresql");
    catalogsPage.setCatalogCommentField("postgresql catalog comment");
    // set mysql catalog props
    catalogsPage.setCatalogPropInput("jdbc-driver", "org.postgresql.Driver");
    catalogsPage.setCatalogPropInput("jdbc-url", postgresqlUri + ":5432/gt_db");
    catalogsPage.setCatalogPropInput("jdbc-user", "trino");
    catalogsPage.setCatalogPropInput("jdbc-password", "ds123");
    catalogsPage.setCatalogPropInput("jdbc-database", "gt_db");

    clickAndWait(catalogsPage.handleSubmitCatalogBtn);
    Assertions.assertTrue(catalogsPage.verifyCreateCatalog(pgCatalogName));
  }

  @Test
  @Order(5)
  public void testCreateFilesetCatalog() throws InterruptedException {
    clickAndWait(catalogsPage.createCatalogBtn);
    catalogsPage.setCatalogNameField(filesetCatalogName);
    clickAndWait(catalogsPage.catalogTypeSelector);
    catalogsPage.clickSelectType("fileset");
    catalogsPage.setCatalogCommentField("fileset catalog comment");
    clickAndWait(catalogsPage.handleSubmitCatalogBtn);
    Assertions.assertTrue(catalogsPage.verifyCreateCatalog(filesetCatalogName));
  }

  @Test
  @Order(6)
  public void testRefreshPage() {
    driver.navigate().refresh();
    Assertions.assertEquals(driver.getTitle(), "Gravitino");
    Assertions.assertTrue(catalogsPage.verifyRefreshPage());
  }

  @Test
  @Order(7)
  public void testViewTabMetalakeDetails() throws InterruptedException {
    clickAndWait(catalogsPage.tabDetailsBtn);
    Assertions.assertTrue(catalogsPage.verifyShowDetailsContent());
    clickAndWait(catalogsPage.tabTableBtn);
    Assertions.assertTrue(catalogsPage.verifyShowTableContent());
  }

  @Test
  @Order(8)
  public void testViewCatalogDetails() throws InterruptedException {
    catalogsPage.createTable(gravitinoUri, metalakeName, catalogName, schemaName);
    catalogsPage.clickViewCatalogBtn(catalogName);
    Assertions.assertTrue(catalogsPage.verifyShowCatalogDetails(catalogName, hiveMetastoreUri));
  }

  @Test
  @Order(9)
  public void testEditCatalog() throws InterruptedException {
    catalogsPage.clickEditCatalogBtn(catalogName);
    catalogsPage.setCatalogNameField(modifiedCatalogName);
    clickAndWait(catalogsPage.handleSubmitCatalogBtn);
    Assertions.assertTrue(catalogsPage.verifyEditedCatalog(modifiedCatalogName));
  }

  @Test
  @Order(10)
  public void testClickCatalogLink() {
    catalogsPage.clickCatalogLink(metalakeName, modifiedCatalogName, catalogType);
    Assertions.assertTrue(catalogsPage.verifyShowTableTitle("Schemas"));
  }

  @Test
  @Order(11)
  public void testClickSchemaLink() {
    catalogsPage.clickSchemaLink(metalakeName, modifiedCatalogName, catalogType, schemaName);
    Assertions.assertTrue(catalogsPage.verifyShowTableTitle("Tables"));
  }

  @Test
  @Order(12)
  public void testClickTableLink() {
    catalogsPage.clickTableLink(
        metalakeName, modifiedCatalogName, catalogType, schemaName, tableName);
    Assertions.assertTrue(catalogsPage.verifyShowTableTitle("Columns"));
    Assertions.assertTrue(catalogsPage.verifyTableColumns());
  }

  @Test
  @Order(13)
  public void testSelectMetalake() throws InterruptedException {
    catalogsPage.metalakeSelectChange(metalakeSelectName);
    Assertions.assertTrue(catalogsPage.verifyEmptyCatalog());

    catalogsPage.metalakeSelectChange(metalakeName);
    Assertions.assertTrue(catalogsPage.verifyCreateCatalog(modifiedCatalogName));
  }

  @Test
  @Order(14)
  public void testBackHomePage() throws InterruptedException {
    clickAndWait(catalogsPage.backHomeBtn);
    Assertions.assertTrue(catalogsPage.verifyBackHomePage());
  }
}
