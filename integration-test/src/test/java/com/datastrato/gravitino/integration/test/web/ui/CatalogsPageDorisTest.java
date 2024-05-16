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
import com.datastrato.gravitino.integration.test.container.DorisContainer;
import com.datastrato.gravitino.integration.test.util.AbstractIT;
import com.datastrato.gravitino.integration.test.util.ITUtils;
import com.datastrato.gravitino.integration.test.util.JdbcDriverDownloader;
import com.datastrato.gravitino.integration.test.web.ui.pages.CatalogsPage;
import com.datastrato.gravitino.integration.test.web.ui.pages.MetalakePage;
import com.datastrato.gravitino.integration.test.web.ui.utils.AbstractWebIT;
import com.datastrato.gravitino.rel.Column;
import com.datastrato.gravitino.rel.expressions.NamedReference;
import com.datastrato.gravitino.rel.expressions.distributions.Distributions;
import com.datastrato.gravitino.rel.expressions.sorts.SortOrder;
import com.datastrato.gravitino.rel.types.Types;
import com.google.common.collect.Maps;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

@Tag("gravitino-docker-it")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class CatalogsPageDorisTest extends AbstractWebIT {
  MetalakePage metalakePage = new MetalakePage();
  CatalogsPage catalogsPage = new CatalogsPage();

  private static final ContainerSuite containerSuite = ContainerSuite.getInstance();
  protected static GravitinoAdminClient gravitinoClient;
  private static GravitinoMetalake metalake;

  protected static String gravitinoUri = "http://127.0.0.1:8090";
  protected static String dorisJdbcConnectionUri;

  private static final String CATALOG_TABLE_TITLE = "Schemas";
  private static final String SCHEMA_TABLE_TITLE = "Tables";
  private static final String TABLE_TABLE_TITLE = "Columns";
  private static final String METALAKE_NAME = "test";
  private static final String CATALOG_TYPE_RELATIONAL = "relational";

  private static final String DORIS_CATALOG_NAME = "catalog_doris";
  private static final String DORIS_JDBC_DRIVER = "com.mysql.cj.jdbc.Driver";
  private static final String USER_NAME = "root";
  private static final String PASSWORD = "root";

  private static final String SCHEMA_NAME_DORIS = "schema_doris";
  private static final String TABLE_NAME = "table1";
  private static final String COLUMN_NAME = "col1";
  private static final String PROPERTIES_KEY1 = "key1";
  private static final String PROPERTIES_VALUE1 = "val1";
  private static final String DOWNLOAD_JDBC_DRIVER_URL =
      "https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.27/mysql-connector-java-8.0.27.jar";

  @BeforeAll
  public static void before() throws Exception {

    String gravitinoHome = System.getenv("GRAVITINO_HOME");
    if (!ITUtils.EMBEDDED_TEST_MODE.equals(AbstractIT.testMode)) {
      Path tmpPath = Paths.get(gravitinoHome, "/catalogs/jdbc-doris/libs");
      JdbcDriverDownloader.downloadJdbcDriver(DOWNLOAD_JDBC_DRIVER_URL, tmpPath.toString());
    } else {
      Path tmpPath = Paths.get(gravitinoHome, "/catalogs/catalog-jdbc-doris/build/libs");
      JdbcDriverDownloader.downloadJdbcDriver(DOWNLOAD_JDBC_DRIVER_URL, tmpPath.toString());
    }
    gravitinoClient = AbstractIT.getGravitinoClient();
    gravitinoUri = String.format("http://127.0.0.1:%d", AbstractIT.getGravitinoServerPort());

    containerSuite.startDorisContainer();

    dorisJdbcConnectionUri =
        String.format(
            "jdbc:mysql://%s:%d/",
            containerSuite.getDorisContainer().getContainerIpAddress(),
            DorisContainer.FE_MYSQL_PORT);
    LOG.info("Doris jdbc url: {}", dorisJdbcConnectionUri);
  }

  /**
   * Create the specified schema
   *
   * @param metalakeName The name of the Metalake where the schema will be created.
   * @param catalogName The name of the Catalog where the schema will be created.
   * @param schemaName The name of the Schema where the schema will be created.
   */
  void createSchema(String metalakeName, String catalogName, String schemaName) {
    Map<String, String> properties = Maps.newHashMap();
    properties.put(PROPERTIES_KEY1, PROPERTIES_VALUE1);
    Catalog catalog_doris =
        metalake.loadCatalog(NameIdentifier.ofCatalog(metalakeName, catalogName));
    catalog_doris
        .asSchemas()
        .createSchema(
            NameIdentifier.of(metalakeName, catalogName, schemaName), "comment", properties);
  }

  /**
   * Creates a table with a single column in the specified Metalake, Catalog, Schema, and Table.
   *
   * @param metalakeName The name of the Metalake where the table will be created.
   * @param catalogName The name of the Catalog where the table will be created.
   * @param schemaName The name of the Schema where the table will be created.
   * @param tableName The name of the Table to be created.
   * @param colName The name of the Column to be created in the Table.
   */
  void createTableAndColumn(
      String metalakeName,
      String catalogName,
      String schemaName,
      String tableName,
      String colName) {
    Map<String, String> properties = Maps.newHashMap();
    properties.put("replication_allocation", "tag.location.default: 1");
    Column column = Column.of(colName, Types.IntegerType.get(), "column comment");
    Catalog catalog_doris =
        metalake.loadCatalog(NameIdentifier.ofCatalog(metalakeName, catalogName));
    catalog_doris
        .asTableCatalog()
        .createTable(
            NameIdentifier.of(metalakeName, catalogName, schemaName, tableName),
            new Column[] {column},
            "comment",
            properties,
            Distributions.hash(2, NamedReference.field(colName)),
            new SortOrder[0]);
  }

  @Test
  @Order(0)
  public void testCreateDorisCatalog() throws InterruptedException {
    // create metalake
    clickAndWait(metalakePage.createMetalakeBtn);
    metalakePage.setMetalakeNameField(METALAKE_NAME);
    clickAndWait(metalakePage.submitHandleMetalakeBtn);
    // load metalake
    metalake = gravitinoClient.loadMetalake(NameIdentifier.of(METALAKE_NAME));
    metalakePage.clickMetalakeLink(METALAKE_NAME);
    // create doris catalog actions
    clickAndWait(catalogsPage.createCatalogBtn);
    catalogsPage.setCatalogNameField(DORIS_CATALOG_NAME);
    // select provider as doris
    clickAndWait(catalogsPage.catalogProviderSelector);
    catalogsPage.clickSelectProvider("jdbc-doris");
    catalogsPage.setCatalogCommentField("doris catalog comment");
    // set doris catalog props
    catalogsPage.setCatalogFixedProp("jdbc-driver", DORIS_JDBC_DRIVER);
    catalogsPage.setCatalogFixedProp("jdbc-url", dorisJdbcConnectionUri);
    catalogsPage.setCatalogFixedProp("jdbc-user", USER_NAME);
    catalogsPage.setCatalogFixedProp("jdbc-password", PASSWORD);
    clickAndWait(catalogsPage.handleSubmitCatalogBtn);
    Assertions.assertTrue(catalogsPage.verifyGetCatalog(DORIS_CATALOG_NAME));
  }

  @Test
  @Order(1)
  public void testClickCatalogLink() {
    // 1. create schema of doris catalog
    createSchema(METALAKE_NAME, DORIS_CATALOG_NAME, SCHEMA_NAME_DORIS);
    // 2. click link of doris catalog
    catalogsPage.clickCatalogLink(METALAKE_NAME, DORIS_CATALOG_NAME, CATALOG_TYPE_RELATIONAL);
    // 3. verify show table title、 schema name and tree node
    Assertions.assertTrue(catalogsPage.verifyShowTableTitle(CATALOG_TABLE_TITLE));
    Assertions.assertTrue(catalogsPage.verifyShowDataItemInList(SCHEMA_NAME_DORIS, false));
    Assertions.assertTrue(catalogsPage.verifySelectedNode(DORIS_CATALOG_NAME));
  }

  @Test
  @Order(2)
  public void testClickSchemaLink() {
    // create table
    createTableAndColumn(
        METALAKE_NAME, DORIS_CATALOG_NAME, SCHEMA_NAME_DORIS, TABLE_NAME, COLUMN_NAME);
    // 2. click link of doris schema
    catalogsPage.clickSchemaLink(
        METALAKE_NAME, DORIS_CATALOG_NAME, CATALOG_TYPE_RELATIONAL, SCHEMA_NAME_DORIS);
    // verify show table title、 schema name and tree node
    Assertions.assertTrue(catalogsPage.verifyShowTableTitle(SCHEMA_TABLE_TITLE));
    Assertions.assertTrue(catalogsPage.verifyShowDataItemInList(TABLE_NAME, false));
    Assertions.assertTrue(catalogsPage.verifySelectedNode(SCHEMA_NAME_DORIS));
  }

  @Test
  @Order(3)
  public void testClickTableLink() {
    catalogsPage.clickTableLink(
        METALAKE_NAME, DORIS_CATALOG_NAME, CATALOG_TYPE_RELATIONAL, SCHEMA_NAME_DORIS, TABLE_NAME);
    Assertions.assertTrue(catalogsPage.verifyShowTableTitle(TABLE_TABLE_TITLE));
    Assertions.assertTrue(catalogsPage.verifyTableColumns());
    Assertions.assertTrue(catalogsPage.verifyShowDataItemInList(COLUMN_NAME, true));
    Assertions.assertTrue(catalogsPage.verifySelectedNode(TABLE_NAME));
  }

  @Test
  @Order(4)
  public void testDorisSchemaTreeNode() throws InterruptedException {
    catalogsPage.clickBreadCrumbsToCatalogs();
    // click doris catalog tree node
    String dorisCatalogNode =
        String.format(
            "{{%s}}{{%s}}{{%s}}", METALAKE_NAME, DORIS_CATALOG_NAME, CATALOG_TYPE_RELATIONAL);
    catalogsPage.clickTreeNode(dorisCatalogNode);
    // verify show table title、 schema name and tree node
    Assertions.assertTrue(catalogsPage.verifyShowTableTitle(CATALOG_TABLE_TITLE));
    Assertions.assertTrue(catalogsPage.verifyShowDataItemInList(SCHEMA_NAME_DORIS, false));
    List<String> treeNodes = Arrays.asList(DORIS_CATALOG_NAME, SCHEMA_NAME_DORIS);
    Assertions.assertTrue(catalogsPage.verifyTreeNodes(treeNodes));
  }

  @Test
  @Order(5)
  public void testDorisTableTreeNode() throws InterruptedException {
    // 1. click schema tree node
    String dorisSchemaNode =
        String.format(
            "{{%s}}{{%s}}{{%s}}{{%s}}",
            METALAKE_NAME, DORIS_CATALOG_NAME, CATALOG_TYPE_RELATIONAL, SCHEMA_NAME_DORIS);
    catalogsPage.clickTreeNode(dorisSchemaNode);
    // 2. verify show table title、 default schema name and tree node
    Assertions.assertTrue(catalogsPage.verifyShowTableTitle(SCHEMA_TABLE_TITLE));
    Assertions.assertTrue(catalogsPage.verifyShowDataItemInList(TABLE_NAME, false));
    List<String> treeNodes = Arrays.asList(DORIS_CATALOG_NAME, SCHEMA_NAME_DORIS, TABLE_NAME);
    Assertions.assertTrue(catalogsPage.verifyTreeNodes(treeNodes));
  }

  @Test
  @Order(6)
  public void testDorisTableDetail() throws InterruptedException {
    // 1. click doris table tree node
    String tableNode =
        String.format(
            "{{%s}}{{%s}}{{%s}}{{%s}}{{%s}}",
            METALAKE_NAME,
            DORIS_CATALOG_NAME,
            CATALOG_TYPE_RELATIONAL,
            SCHEMA_NAME_DORIS,
            TABLE_NAME);
    catalogsPage.clickTreeNode(tableNode);
    // 2. verify show table column after click table tree node
    Assertions.assertTrue(catalogsPage.verifyShowDataItemInList(COLUMN_NAME, true));
    clickAndWait(catalogsPage.tabDetailsBtn);
    // 3. verify show tab details
    Assertions.assertTrue(catalogsPage.verifyShowDetailsContent());
  }

  @Test
  @Order(7)
  public void testBackHomePage() throws InterruptedException {
    clickAndWait(catalogsPage.backHomeBtn);
    Assertions.assertTrue(catalogsPage.verifyBackHomePage());
  }
}
