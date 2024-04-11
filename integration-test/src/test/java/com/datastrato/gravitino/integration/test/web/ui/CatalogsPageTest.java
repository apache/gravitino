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
import java.util.Arrays;
import java.util.List;
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

  protected static String gravitinoUri = "http://127.0.0.1:8090";
  protected static String trinoUri = "http://127.0.0.1:8080";
  protected static String hiveMetastoreUri = "thrift://127.0.0.1:9083";
  protected static String hdfsUri = "hdfs://127.0.0.1:9000";
  protected static String mysqlUri = "jdbc:mysql://127.0.0.1";
  protected static String postgresqlUri = "jdbc:postgresql://127.0.0.1";

  private static final String WEB_TITLE = "Gravitino";
  private static final String CATALOG_TABLE_TITLE = "Schemas";
  private static final String SCHEMA_TABLE_TITLE = "Tables";
  private static final String TABLE_TABLE_TITLE = "Columns";
  private static final String METALAKE_NAME = "test";
  private static final String METALAKE_SELECT_NAME = "metalake_select_name";
  private static final String CATALOG_TYPE = "relational";
  private static final String DEFAULT_CATALOG_NAME = "default_catalog";
  private static final String HIVE_CATALOG_NAME = "catalog_hive";
  private static final String MODIFIED_CATALOG_NAME = HIVE_CATALOG_NAME + "_edited";
  private static final String ICEBERG_CATALOG_NAME = "catalog_iceberg";
  private static final String FILESET_CATALOG_NAME = "catalog_fileset";
  private static final String SCHEMA_NAME = "default";
  private static final String TABLE_NAME = "table1";
  private static final String TABLE_NAME_2 = "table2";
  private static final String COLUMN_NAME = "column";
  private static final String COLUMN_NAME_2 = "column_2";

  private static final String MYSQL_CATALOG_NAME = "catalog_mysql";
  private static final String MYSQL_JDBC_DRIVER = "com.mysql.cj.jdbc.Driver";

  private static final String PG_CATALOG_NAME = "catalog_pg";
  private static final String PG_JDBC_DRIVER = "org.postgresql.Driver";
  private static final String PG_JDBC_DB = "gt_db";

  private static final String COMMON_JDBC_USER = "trino";
  private static final String COMMON_JDBC_PWD = "ds123";

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

  void createTableAndColumn(
      String metalakeName,
      String catalogName,
      String schemaName,
      String tableName,
      String colName) {
    Map<String, String> properties = Maps.newHashMap();
    Column column = Column.of(colName, Types.IntegerType.get(), "column comment");
    catalog
        .asTableCatalog()
        .createTable(
            NameIdentifier.of(metalakeName, catalogName, schemaName, tableName),
            new Column[] {column},
            "comment",
            properties);
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
    catalogsPage.setCatalogFixedProp("metastore.uris", hiveMetastoreUri);
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
    catalogsPage.setCatalogFixedProp("metastore.uris", hiveMetastoreUri);
    catalogsPage.addCatalogPropsBtn.click();
    catalogsPage.setCatalogPropsAt(1, "key1", "value1");
    catalogsPage.addCatalogPropsBtn.click();
    catalogsPage.setCatalogPropsAt(2, "key2", "value2");
    clickAndWait(catalogsPage.handleSubmitCatalogBtn);
    // load catalog
    catalog = metalake.loadCatalog(NameIdentifier.of(METALAKE_NAME, HIVE_CATALOG_NAME));

    Assertions.assertTrue(catalogsPage.verifyGetCatalog(HIVE_CATALOG_NAME));
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
    catalogsPage.setCatalogFixedProp("uri", hiveMetastoreUri);
    // set iceberg warehouse
    catalogsPage.setCatalogFixedProp("warehouse", hdfsUri);
    clickAndWait(catalogsPage.handleSubmitCatalogBtn);
    Assertions.assertTrue(catalogsPage.verifyGetCatalog(ICEBERG_CATALOG_NAME));
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
    catalogsPage.setCatalogFixedProp("jdbc-url", mysqlUri);
    catalogsPage.setCatalogFixedProp("jdbc-user", COMMON_JDBC_USER);
    catalogsPage.setCatalogFixedProp("jdbc-password", COMMON_JDBC_PWD);
    clickAndWait(catalogsPage.handleSubmitCatalogBtn);
    Assertions.assertTrue(catalogsPage.verifyGetCatalog(MYSQL_CATALOG_NAME));
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
    catalogsPage.setCatalogFixedProp("jdbc-url", postgresqlUri + ":5432/" + PG_JDBC_DB);
    catalogsPage.setCatalogFixedProp("jdbc-user", COMMON_JDBC_USER);
    catalogsPage.setCatalogFixedProp("jdbc-password", COMMON_JDBC_PWD);
    catalogsPage.setCatalogFixedProp("jdbc-database", PG_JDBC_DB);

    clickAndWait(catalogsPage.handleSubmitCatalogBtn);
    Assertions.assertTrue(catalogsPage.verifyGetCatalog(PG_CATALOG_NAME));
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
    Assertions.assertTrue(catalogsPage.verifyGetCatalog(FILESET_CATALOG_NAME));
  }

  @Test
  @Order(6)
  public void testRefreshPage() {
    driver.navigate().refresh();
    Assertions.assertEquals(driver.getTitle(), WEB_TITLE);
    Assertions.assertTrue(catalogsPage.verifyRefreshPage());
    List<String> catalogsNames =
        Arrays.asList(
            HIVE_CATALOG_NAME,
            ICEBERG_CATALOG_NAME,
            MYSQL_CATALOG_NAME,
            PG_CATALOG_NAME,
            FILESET_CATALOG_NAME);
    Assertions.assertTrue(catalogsPage.verifyCreatedCatalogs(catalogsNames));
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
    catalogsPage.clickViewCatalogBtn(HIVE_CATALOG_NAME);
    Assertions.assertTrue(
        catalogsPage.verifyShowCatalogDetails(HIVE_CATALOG_NAME, hiveMetastoreUri));
  }

  @Test
  @Order(9)
  public void testEditCatalog() throws InterruptedException {
    catalogsPage.clickEditCatalogBtn(HIVE_CATALOG_NAME);
    catalogsPage.setCatalogNameField(MODIFIED_CATALOG_NAME);
    clickAndWait(catalogsPage.handleSubmitCatalogBtn);
    Assertions.assertTrue(catalogsPage.verifyEditedCatalog(MODIFIED_CATALOG_NAME));
  }

  // test catalog show schema list
  @Test
  @Order(10)
  public void testClickCatalogLink() {
    catalogsPage.clickCatalogLink(METALAKE_NAME, MODIFIED_CATALOG_NAME, CATALOG_TYPE);
    Assertions.assertTrue(catalogsPage.verifyShowTableTitle(CATALOG_TABLE_TITLE));
    Assertions.assertTrue(catalogsPage.verifyShowDataItemInList(SCHEMA_NAME));
    Assertions.assertTrue(catalogsPage.verifySelectedNode(MODIFIED_CATALOG_NAME));
  }

  @Test
  @Order(11)
  public void testRefreshCatalogPage() {
    driver.navigate().refresh();
    Assertions.assertEquals(driver.getTitle(), WEB_TITLE);
    Assertions.assertTrue(catalogsPage.verifyShowTableTitle(CATALOG_TABLE_TITLE));
    Assertions.assertTrue(catalogsPage.verifyShowDataItemInList(SCHEMA_NAME));
    List<String> treeNodes =
        Arrays.asList(
            MODIFIED_CATALOG_NAME,
            SCHEMA_NAME,
            ICEBERG_CATALOG_NAME,
            MYSQL_CATALOG_NAME,
            PG_CATALOG_NAME,
            FILESET_CATALOG_NAME);
    Assertions.assertTrue(catalogsPage.verifyTreeNodes(treeNodes));
    Assertions.assertTrue(catalogsPage.verifySelectedNode(MODIFIED_CATALOG_NAME));
  }

  // test schema show table list
  @Test
  @Order(12)
  public void testClickSchemaLink() {
    // create table
    createTableAndColumn(
        METALAKE_NAME, MODIFIED_CATALOG_NAME, SCHEMA_NAME, TABLE_NAME, COLUMN_NAME);
    catalogsPage.clickSchemaLink(METALAKE_NAME, MODIFIED_CATALOG_NAME, CATALOG_TYPE, SCHEMA_NAME);
    Assertions.assertTrue(catalogsPage.verifyShowTableTitle(SCHEMA_TABLE_TITLE));
    Assertions.assertTrue(catalogsPage.verifyShowDataItemInList(TABLE_NAME));
    Assertions.assertTrue(catalogsPage.verifySelectedNode(SCHEMA_NAME));
  }

  @Test
  @Order(13)
  public void testRefreshSchemaPage() {
    driver.navigate().refresh();
    Assertions.assertEquals(driver.getTitle(), WEB_TITLE);
    Assertions.assertTrue(catalogsPage.verifyShowTableTitle(SCHEMA_TABLE_TITLE));
    Assertions.assertTrue(catalogsPage.verifyShowDataItemInList(TABLE_NAME));
    List<String> treeNodes =
        Arrays.asList(
            MODIFIED_CATALOG_NAME,
            SCHEMA_NAME,
            TABLE_NAME,
            ICEBERG_CATALOG_NAME,
            MYSQL_CATALOG_NAME,
            PG_CATALOG_NAME,
            FILESET_CATALOG_NAME);
    Assertions.assertTrue(catalogsPage.verifyTreeNodes(treeNodes));
    Assertions.assertTrue(catalogsPage.verifySelectedNode(SCHEMA_NAME));
  }

  // test table show column list
  @Test
  @Order(14)
  public void testClickTableLink() {
    catalogsPage.clickTableLink(
        METALAKE_NAME, MODIFIED_CATALOG_NAME, CATALOG_TYPE, SCHEMA_NAME, TABLE_NAME);
    Assertions.assertTrue(catalogsPage.verifyShowTableTitle(TABLE_TABLE_TITLE));
    Assertions.assertTrue(catalogsPage.verifyTableColumns());
    Assertions.assertTrue(catalogsPage.verifyShowDataItemInList(COLUMN_NAME));
    Assertions.assertTrue(catalogsPage.verifySelectedNode(TABLE_NAME));
  }

  @Test
  @Order(15)
  public void testRefreshTablePage() {
    driver.navigate().refresh();
    Assertions.assertEquals(driver.getTitle(), WEB_TITLE);
    Assertions.assertTrue(catalogsPage.verifyRefreshPage());
    Assertions.assertTrue(catalogsPage.verifyShowTableTitle(TABLE_TABLE_TITLE));
    Assertions.assertTrue(catalogsPage.verifyTableColumns());
    Assertions.assertTrue(catalogsPage.verifyShowDataItemInList(COLUMN_NAME));
    List<String> treeNodes =
        Arrays.asList(
            MODIFIED_CATALOG_NAME,
            SCHEMA_NAME,
            TABLE_NAME,
            ICEBERG_CATALOG_NAME,
            MYSQL_CATALOG_NAME,
            PG_CATALOG_NAME,
            FILESET_CATALOG_NAME);
    Assertions.assertTrue(catalogsPage.verifyTreeNodes(treeNodes));
  }

  @Test
  @Order(16)
  public void testSelectMetalake() throws InterruptedException {
    catalogsPage.metalakeSelectChange(METALAKE_SELECT_NAME);
    Assertions.assertTrue(catalogsPage.verifyEmptyCatalog());

    catalogsPage.metalakeSelectChange(METALAKE_NAME);
    Assertions.assertTrue(catalogsPage.verifyGetCatalog(MODIFIED_CATALOG_NAME));
  }

  @Test
  @Order(17)
  public void testClickTreeList() throws InterruptedException {
    String icebergNode =
        String.format("{{%s}}{{%s}}{{%s}}", METALAKE_NAME, ICEBERG_CATALOG_NAME, CATALOG_TYPE);
    catalogsPage.clickTreeNode(icebergNode);
    Assertions.assertTrue(catalogsPage.verifyGetCatalog(ICEBERG_CATALOG_NAME));
    String mysqlNode =
        String.format("{{%s}}{{%s}}{{%s}}", METALAKE_NAME, MYSQL_CATALOG_NAME, CATALOG_TYPE);
    catalogsPage.clickTreeNode(mysqlNode);
    Assertions.assertTrue(catalogsPage.verifyGetCatalog(MYSQL_CATALOG_NAME));
    String pgNode =
        String.format("{{%s}}{{%s}}{{%s}}", METALAKE_NAME, PG_CATALOG_NAME, CATALOG_TYPE);
    catalogsPage.clickTreeNode(pgNode);
    Assertions.assertTrue(catalogsPage.verifyGetCatalog(PG_CATALOG_NAME));
    String filesetNode =
        String.format("{{%s}}{{%s}}{{%s}}", METALAKE_NAME, FILESET_CATALOG_NAME, "fileset");
    catalogsPage.clickTreeNode(filesetNode);
    Assertions.assertTrue(catalogsPage.verifyGetCatalog(FILESET_CATALOG_NAME));
    String hiveNode =
        String.format("{{%s}}{{%s}}{{%s}}", METALAKE_NAME, MODIFIED_CATALOG_NAME, CATALOG_TYPE);
    catalogsPage.clickTreeNode(hiveNode);
    Assertions.assertTrue(catalogsPage.verifyShowTableTitle(CATALOG_TABLE_TITLE));
    Assertions.assertTrue(catalogsPage.verifyGetCatalog(MODIFIED_CATALOG_NAME));
    String schemaNode =
        String.format(
            "{{%s}}{{%s}}{{%s}}{{%s}}",
            METALAKE_NAME, MODIFIED_CATALOG_NAME, CATALOG_TYPE, SCHEMA_NAME);
    catalogsPage.clickTreeNode(schemaNode);
    Assertions.assertTrue(catalogsPage.verifyShowTableTitle(SCHEMA_TABLE_TITLE));
    Assertions.assertTrue(catalogsPage.verifyShowDataItemInList(TABLE_NAME));
    String tableNode =
        String.format(
            "{{%s}}{{%s}}{{%s}}{{%s}}{{%s}}",
            METALAKE_NAME, MODIFIED_CATALOG_NAME, CATALOG_TYPE, SCHEMA_NAME, TABLE_NAME);
    catalogsPage.clickTreeNode(tableNode);
    Assertions.assertTrue(catalogsPage.verifyShowTableTitle(TABLE_TABLE_TITLE));
    Assertions.assertTrue(catalogsPage.verifyShowDataItemInList(COLUMN_NAME));
    Assertions.assertTrue(catalogsPage.verifyTableColumns());
  }

  @Test
  @Order(18)
  public void testTreeNodeRefresh() throws InterruptedException {
    createTableAndColumn(
        METALAKE_NAME, MODIFIED_CATALOG_NAME, SCHEMA_NAME, TABLE_NAME_2, COLUMN_NAME_2);
    String hiveNode =
        String.format("{{%s}}{{%s}}{{%s}}", METALAKE_NAME, MODIFIED_CATALOG_NAME, CATALOG_TYPE);
    catalogsPage.clickTreeNode(hiveNode);
    String schemaNode =
        String.format(
            "{{%s}}{{%s}}{{%s}}{{%s}}",
            METALAKE_NAME, MODIFIED_CATALOG_NAME, CATALOG_TYPE, SCHEMA_NAME);
    catalogsPage.clickTreeNodeRefresh(schemaNode);
    String tableNode =
        String.format(
            "{{%s}}{{%s}}{{%s}}{{%s}}{{%s}}",
            METALAKE_NAME, MODIFIED_CATALOG_NAME, CATALOG_TYPE, SCHEMA_NAME, TABLE_NAME_2);
    catalogsPage.clickTreeNode(tableNode);
    Assertions.assertTrue(catalogsPage.verifyShowTableTitle(TABLE_TABLE_TITLE));
    Assertions.assertTrue(catalogsPage.verifyShowDataItemInList(COLUMN_NAME_2));
    Assertions.assertTrue(catalogsPage.verifyTableColumns());
  }

  @Test
  @Order(19)
  public void testBackHomePage() throws InterruptedException {
    clickAndWait(catalogsPage.backHomeBtn);
    Assertions.assertTrue(catalogsPage.verifyBackHomePage());
  }
}
