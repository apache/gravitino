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
import com.datastrato.gravitino.integration.test.container.TrinoITContainers;
import com.datastrato.gravitino.integration.test.util.AbstractIT;
import com.datastrato.gravitino.integration.test.web.ui.pages.CatalogsPage;
import com.datastrato.gravitino.integration.test.web.ui.pages.MetalakePage;
import com.datastrato.gravitino.integration.test.web.ui.utils.AbstractWebIT;
import com.datastrato.gravitino.rel.Column;
import com.datastrato.gravitino.rel.types.Types;
import com.google.common.collect.Maps;
import java.util.Map;
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
  MetalakePage metalakePage = new MetalakePage();
  CatalogsPage catalogsPage = new CatalogsPage();

  protected static TrinoITContainers trinoITContainers;
  protected static GravitinoAdminClient gravitinoClient;
  private static GravitinoMetalake metalake;
  private static Catalog catalog;

  protected static String GRAVITINO_URI = "http://127.0.0.1:8090";
  protected static String TRINO_URI = "http://127.0.0.1:8080";
  protected static String HIVE_METASTORE_URI = "thrift://127.0.0.1:9083";
  protected static String HDFS_URI = "hdfs://127.0.0.1:9000";
  protected static String MYSQL_URI = "jdbc:mysql://127.0.0.1";
  protected static String PG_URI = "jdbc:postgresql://127.0.0.1";

  private static final String WEB_TITLE = "Gravitino";
  private static final String METALAKE_NAME = "test";
  private static final String METALAKE_SELECT_NAME = "metalake_select_name";
  private static final String CATALOG_TYPE = "relational";
  private static final String DEFAULT_CATALOG_NAME = "default_catalog";
  private static final String HIVE_CATALOG_NAME = "catalog_hive";
  private static final String MODIFIED_CATALOG_NAME = HIVE_CATALOG_NAME + "_edited";
  private static final String SCHEMA_NAME = "default";
  private static final String TABLE_NAME = "table";
  private static final String ICEBERG_CATALOG_NAME = "catalog_iceberg";
  private static final String MYSQL_CATALOG_NAME = "catalog_mysql";
  private static final String PG_CATALOG_NAME = "catalog_pg";
  private static final String FILESET_CATALOG_NAME = "catalog_fileset";

  private static final String MYSQL_JDBC_DRIVER = "com.mysql.cj.jdbc.Driver";
  private static final String PG_JDBC_DRIVER = "org.postgresql.Driver";
  private static final String JDBC_USER = "trino";
  private static final String JDBC_PWD = "ds123";
  private static final String JDBC_DB = "gt_db";

  @BeforeAll
  public static void before() throws Exception {
    gravitinoClient = AbstractIT.getGravitinoClient();

    GRAVITINO_URI = String.format("http://127.0.0.1:%d", AbstractIT.getGravitinoServerPort());

    trinoITContainers = ContainerSuite.getTrinoITContainers();
    trinoITContainers.launch(AbstractIT.getGravitinoServerPort());

    TRINO_URI = trinoITContainers.getTrinoUri();
    HIVE_METASTORE_URI = trinoITContainers.getHiveMetastoreUri();
    HDFS_URI = trinoITContainers.getHdfsUri();
    MYSQL_URI = trinoITContainers.getMysqlUri();
    PG_URI = trinoITContainers.getPostgresqlUri();
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
    metalakePage.setMetalakeNameField(METALAKE_NAME);
    clickAndWait(metalakePage.submitHandleMetalakeBtn);
    // Create another metalake for select option
    clickAndWait(metalakePage.createMetalakeBtn);
    metalakePage.setMetalakeNameField(METALAKE_SELECT_NAME);
    clickAndWait(metalakePage.submitHandleMetalakeBtn);
    // load metalake
    metalake = gravitinoClient.loadMetalake(NameIdentifier.of(METALAKE_NAME));
    metalakePage.clickMetalakeLink(METALAKE_NAME);
    // create catalog
    clickAndWait(catalogsPage.createCatalogBtn);
    catalogsPage.setCatalogNameField(DEFAULT_CATALOG_NAME);
    catalogsPage.setCatalogFixedProp("metastore.uris", HIVE_METASTORE_URI);
    clickAndWait(catalogsPage.handleSubmitCatalogBtn);
    // delete catalog
    catalogsPage.clickDeleteCatalogBtn(DEFAULT_CATALOG_NAME);
    clickAndWait(catalogsPage.confirmDeleteBtn);
    Assertions.assertTrue(catalogsPage.verifyEmptyCatalog());
  }

  @Test
  @Order(1)
  public void testCreateHiveCatalog() throws InterruptedException {
    // Create catalog
    clickAndWait(catalogsPage.createCatalogBtn);
    catalogsPage.setCatalogNameField(HIVE_CATALOG_NAME);
    catalogsPage.setCatalogCommentField("catalog comment");
    catalogsPage.setCatalogFixedProp("metastore.uris", HIVE_METASTORE_URI);
    catalogsPage.addCatalogPropsBtn.click();
    catalogsPage.setCatalogPropsAt(1, "key1", "value1");
    catalogsPage.addCatalogPropsBtn.click();
    catalogsPage.setCatalogPropsAt(2, "key2", "value2");
    clickAndWait(catalogsPage.handleSubmitCatalogBtn);
    // load catalog
    catalog = metalake.loadCatalog(NameIdentifier.of(METALAKE_NAME, HIVE_CATALOG_NAME));

    Assertions.assertTrue(catalogsPage.verifyCreateCatalog(HIVE_CATALOG_NAME));
  }

  @Test
  @Order(2)
  public void testCreateIcebergCatalog() throws InterruptedException {
    clickAndWait(catalogsPage.createCatalogBtn);
    catalogsPage.setCatalogNameField(ICEBERG_CATALOG_NAME);
    // select provider as iceberg
    clickAndWait(catalogsPage.catalogProviderSelector);
    catalogsPage.clickSelectProvider("lakehouse-iceberg");
    catalogsPage.setCatalogCommentField("iceberg catalog comment");
    // set iceberg uri
    catalogsPage.setCatalogFixedProp("uri", HIVE_METASTORE_URI);
    // set iceberg warehouse
    catalogsPage.setCatalogFixedProp("warehouse", HDFS_URI);
    clickAndWait(catalogsPage.handleSubmitCatalogBtn);
    Assertions.assertTrue(catalogsPage.verifyCreateCatalog(ICEBERG_CATALOG_NAME));
  }

  @Test
  @Order(3)
  public void testCreateMysqlCatalog() throws InterruptedException {
    // create mysql catalog actions
    clickAndWait(catalogsPage.createCatalogBtn);
    catalogsPage.setCatalogNameField(MYSQL_CATALOG_NAME);
    // select provider as mysql
    clickAndWait(catalogsPage.catalogProviderSelector);
    catalogsPage.clickSelectProvider("jdbc-mysql");
    catalogsPage.setCatalogCommentField("mysql catalog comment");
    // set mysql catalog props
    catalogsPage.setCatalogFixedProp("jdbc-driver", MYSQL_JDBC_DRIVER);
    catalogsPage.setCatalogFixedProp("jdbc-url", MYSQL_URI);
    catalogsPage.setCatalogFixedProp("jdbc-user", JDBC_USER);
    catalogsPage.setCatalogFixedProp("jdbc-password", JDBC_PWD);
    clickAndWait(catalogsPage.handleSubmitCatalogBtn);
    Assertions.assertTrue(catalogsPage.verifyCreateCatalog(MYSQL_CATALOG_NAME));
  }

  @Test
  @Order(4)
  public void testCreatePgCatalog() throws InterruptedException {
    // create postgresql catalog actions
    clickAndWait(catalogsPage.createCatalogBtn);
    catalogsPage.setCatalogNameField(PG_CATALOG_NAME);
    // select provider as mysql
    clickAndWait(catalogsPage.catalogProviderSelector);
    catalogsPage.clickSelectProvider("jdbc-postgresql");
    catalogsPage.setCatalogCommentField("postgresql catalog comment");
    // set mysql catalog props
    catalogsPage.setCatalogFixedProp("jdbc-driver", PG_JDBC_DRIVER);
    catalogsPage.setCatalogFixedProp("jdbc-url", PG_URI + ":5432/" + JDBC_DB);
    catalogsPage.setCatalogFixedProp("jdbc-user", JDBC_USER);
    catalogsPage.setCatalogFixedProp("jdbc-password", JDBC_PWD);
    catalogsPage.setCatalogFixedProp("jdbc-database", JDBC_DB);

    clickAndWait(catalogsPage.handleSubmitCatalogBtn);
    Assertions.assertTrue(catalogsPage.verifyCreateCatalog(PG_CATALOG_NAME));
  }

  @Test
  @Order(5)
  public void testCreateFilesetCatalog() throws InterruptedException {
    clickAndWait(catalogsPage.createCatalogBtn);
    catalogsPage.setCatalogNameField(FILESET_CATALOG_NAME);
    clickAndWait(catalogsPage.catalogTypeSelector);
    catalogsPage.clickSelectType("fileset");
    catalogsPage.setCatalogCommentField("fileset catalog comment");
    clickAndWait(catalogsPage.handleSubmitCatalogBtn);
    Assertions.assertTrue(catalogsPage.verifyCreateCatalog(FILESET_CATALOG_NAME));
  }

  @Test
  @Order(6)
  public void testRefreshPage() {
    driver.navigate().refresh();
    Assertions.assertEquals(driver.getTitle(), WEB_TITLE);
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
    Map<String, String> properties = Maps.newHashMap();
    Column col1 = Column.of("col_1", Types.IntegerType.get(), "col_1_comment");
    catalog
        .asTableCatalog()
        .createTable(
            NameIdentifier.of(METALAKE_NAME, HIVE_CATALOG_NAME, SCHEMA_NAME, TABLE_NAME),
            new Column[] {col1},
            "comment",
            properties);
    catalogsPage.clickViewCatalogBtn(HIVE_CATALOG_NAME);
    Assertions.assertTrue(
        catalogsPage.verifyShowCatalogDetails(HIVE_CATALOG_NAME, HIVE_METASTORE_URI));
  }

  @Test
  @Order(9)
  public void testEditCatalog() throws InterruptedException {
    catalogsPage.clickEditCatalogBtn(HIVE_CATALOG_NAME);
    catalogsPage.setCatalogNameField(MODIFIED_CATALOG_NAME);
    clickAndWait(catalogsPage.handleSubmitCatalogBtn);
    Assertions.assertTrue(catalogsPage.verifyEditedCatalog(MODIFIED_CATALOG_NAME));
  }

  @Test
  @Order(10)
  public void testClickCatalogLink() {
    catalogsPage.clickCatalogLink(METALAKE_NAME, MODIFIED_CATALOG_NAME, CATALOG_TYPE);
    Assertions.assertTrue(catalogsPage.verifyShowTableTitle("Schemas"));
  }

  @Test
  @Order(11)
  public void testClickSchemaLink() {
    catalogsPage.clickSchemaLink(METALAKE_NAME, MODIFIED_CATALOG_NAME, CATALOG_TYPE, SCHEMA_NAME);
    Assertions.assertTrue(catalogsPage.verifyShowTableTitle("Tables"));
  }

  @Test
  @Order(12)
  public void testClickTableLink() {
    catalogsPage.clickTableLink(
        METALAKE_NAME, MODIFIED_CATALOG_NAME, CATALOG_TYPE, SCHEMA_NAME, TABLE_NAME);
    Assertions.assertTrue(catalogsPage.verifyShowTableTitle("Columns"));
    Assertions.assertTrue(catalogsPage.verifyTableColumns());
  }

  @Test
  @Order(13)
  public void testSelectMetalake() throws InterruptedException {
    catalogsPage.metalakeSelectChange(METALAKE_SELECT_NAME);
    Assertions.assertTrue(catalogsPage.verifyEmptyCatalog());

    catalogsPage.metalakeSelectChange(METALAKE_NAME);
    Assertions.assertTrue(catalogsPage.verifyCreateCatalog(MODIFIED_CATALOG_NAME));
  }

  @Test
  @Order(14)
  public void testBackHomePage() throws InterruptedException {
    clickAndWait(catalogsPage.backHomeBtn);
    Assertions.assertTrue(catalogsPage.verifyBackHomePage());
  }
}
