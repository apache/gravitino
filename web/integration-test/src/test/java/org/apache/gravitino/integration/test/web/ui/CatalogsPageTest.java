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

package org.apache.gravitino.integration.test.web.ui;

import static org.apache.gravitino.rel.expressions.transforms.Transforms.identity;

import com.google.common.collect.Maps;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.client.GravitinoAdminClient;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.integration.test.container.ContainerSuite;
import org.apache.gravitino.integration.test.container.TrinoITContainers;
import org.apache.gravitino.integration.test.web.ui.pages.CatalogsPage;
import org.apache.gravitino.integration.test.web.ui.pages.MetalakePage;
import org.apache.gravitino.integration.test.web.ui.utils.BaseWebIT;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.expressions.NamedReference;
import org.apache.gravitino.rel.expressions.distributions.Distribution;
import org.apache.gravitino.rel.expressions.distributions.Distributions;
import org.apache.gravitino.rel.expressions.sorts.NullOrdering;
import org.apache.gravitino.rel.expressions.sorts.SortDirection;
import org.apache.gravitino.rel.expressions.sorts.SortOrder;
import org.apache.gravitino.rel.expressions.sorts.SortOrders;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.types.Types;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.openqa.selenium.By;

@Tag("gravitino-docker-test")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class CatalogsPageTest extends BaseWebIT {
  private MetalakePage metalakePage;
  private CatalogsPage catalogsPage;

  protected static TrinoITContainers trinoITContainers;
  protected static GravitinoAdminClient gravitinoClient;
  protected static String gravitinoUri = "http://127.0.0.1:8090";
  protected static String hiveMetastoreUri = "thrift://127.0.0.1:9083";
  protected static String hdfsUri = "hdfs://127.0.0.1:9000";
  protected static String mysqlUri = "jdbc:mysql://127.0.0.1";
  protected static String postgresqlUri = "jdbc:postgresql://127.0.0.1";

  private static final long MAX_WAIT_IN_SECONDS = 5;
  private static final long WAIT_INTERVAL_IN_SECONDS = 1;

  private static final String WEB_TITLE = "Gravitino";
  private static final String CATALOG_TABLE_TITLE = "Schemas";
  private static final String SCHEMA_TABLE_TITLE = "Tables";
  private static final String SCHEMA_FILESET_TITLE = "Filesets";
  private static final String TABLE_TABLE_TITLE = "Columns";
  private static final String METALAKE_NAME = "test";
  private static final String METALAKE_SELECT_NAME = "metalake_select_name";
  private static final String CATALOG_TYPE_RELATIONAL = "relational";
  private static final String CATALOG_TYPE_FILESET = "fileset";
  private static final String DEFAULT_CATALOG_NAME = "default_catalog";
  private static final String HIVE_CATALOG_NAME = "catalog_hive";
  private static final String MODIFIED_HIVE_CATALOG_NAME = HIVE_CATALOG_NAME + "_edited";
  private static final String ICEBERG_CATALOG_NAME = "catalog_iceberg";
  private static final String FILESET_CATALOG_NAME = "catalog_fileset";
  private static final String SCHEMA_NAME = "default";
  private static final String SCHEMA_NAME_FILESET = "schema_fileset";
  private static final String FILESET_DEFAULT_LOCATION = "fileset_location";
  private static final String FILESET_NAME = "fileset1";
  private static final String TABLE_NAME = "table1";
  private static final String TABLE_NAME_2 = "table2";
  private static final String TABLE_NAME_3 = "table3";
  private static final String COLUMN_NAME = "column";
  private static final String COLUMN_NAME_2 = "column_2";
  private static final String PROPERTIES_KEY1 = "key1";
  private static final String PROPERTIES_VALUE1 = "val1";

  private static final String MYSQL_CATALOG_NAME = "catalog_mysql";
  private static final String MYSQL_JDBC_DRIVER = "com.mysql.cj.jdbc.Driver";

  private static final String PG_CATALOG_NAME = "catalog_pg";
  private static final String PG_JDBC_DRIVER = "org.postgresql.Driver";
  private static final String PG_JDBC_DB = "gt_db";

  private static final String COMMON_JDBC_USER = "trino";
  private static final String COMMON_JDBC_PWD = "ds123";

  public static final String DISTRIBUTION = "distribution";
  public static final String SORT_ORDERS = "sortOrders";

  private static String defaultBaseLocation;

  @BeforeAll
  public void before() throws Exception {
    gravitinoClient = getGravitinoClient();

    gravitinoUri = String.format("http://127.0.0.1:%d", getGravitinoServerPort());

    trinoITContainers = ContainerSuite.getTrinoITContainers();
    trinoITContainers.launch(getGravitinoServerPort());

    hiveMetastoreUri = trinoITContainers.getHiveMetastoreUri();
    hdfsUri = trinoITContainers.getHdfsUri();
    mysqlUri = trinoITContainers.getMysqlUri();
    postgresqlUri = trinoITContainers.getPostgresqlUri();

    metalakePage = new MetalakePage(driver);
    catalogsPage = new CatalogsPage(driver);
  }

  /**
   * Creates a table with a single column in the specified Metalake, Catalog, Schema, and Table.
   *
   * @param metalakeName The name of the Metalake where the table will be created.
   * @param catalogName The name of the Catalog where the table will be created.
   * @param schemaName The name of the Schema where the table will be created.
   * @param tableName The name of the Table to be created.
   */
  void createHiveTableAndColumn(
      String metalakeName, String catalogName, String schemaName, String tableName) {
    Map<String, String> properties = Maps.newHashMap();
    Column col1 = Column.of(COLUMN_NAME, Types.ByteType.get(), "col1 comment");
    Column col2 = Column.of(COLUMN_NAME_2, Types.ByteType.get(), "col2 comment");
    Column[] columns = new Column[] {col1, col2};
    Distribution distribution = createDistribution();
    SortOrder[] sortOrders = createSortOrder();
    Transform[] partitions = new Transform[] {identity(col2.name())};
    GravitinoMetalake metalake = gravitinoClient.loadMetalake(metalakeName);
    Catalog catalog = metalake.loadCatalog(catalogName);
    catalog
        .asTableCatalog()
        .createTable(
            NameIdentifier.of(schemaName, tableName),
            columns,
            "comment",
            properties,
            partitions,
            distribution,
            sortOrders);
  }

  private Distribution createDistribution() {
    return Distributions.hash(10, NamedReference.field(COLUMN_NAME));
  }

  private SortOrder[] createSortOrder() {
    return new SortOrders.SortImpl[] {
      SortOrders.of(
          NamedReference.field(COLUMN_NAME), SortDirection.DESCENDING, NullOrdering.NULLS_FIRST),
      SortOrders.of(
          NamedReference.field(COLUMN_NAME_2), SortDirection.DESCENDING, NullOrdering.NULLS_FIRST)
    };
  }

  /**
   * Retrieves the default base location for the given schema name.
   *
   * @param schemaName The name of the schema.
   * @return The default HDFS storage location for the schema.
   */
  private static String defaultBaseLocation(String schemaName) {
    if (defaultBaseLocation == null) {
      defaultBaseLocation =
          String.format("%s/user/hadoop/%s.db", hdfsUri, schemaName.toLowerCase());
    }
    return defaultBaseLocation;
  }

  /**
   * Retrieves the storage location for the given schema name and fileset name.
   *
   * @param schemaName The name of the schema.
   * @param filesetName The name of the fileset.
   * @return The storage path for the combination of schema and fileset.
   */
  private static String storageLocation(String schemaName, String filesetName) {
    return defaultBaseLocation(schemaName) + "/" + filesetName;
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
    gravitinoClient.loadMetalake(METALAKE_NAME);
    metalakePage.clickMetalakeLink(METALAKE_NAME);
    // create catalog
    clickAndWait(catalogsPage.createCatalogBtn);
    catalogsPage.setCatalogNameField(DEFAULT_CATALOG_NAME);
    catalogsPage.setCatalogFixedProp("metastore.uris", hiveMetastoreUri);
    clickAndWait(catalogsPage.handleSubmitCatalogBtn);
    // delete catalog
    catalogsPage.clickInUseSwitch(DEFAULT_CATALOG_NAME);
    catalogsPage.clickDeleteBtn(DEFAULT_CATALOG_NAME);
    clickAndWait(catalogsPage.confirmDeleteBtn);
    Assertions.assertTrue(catalogsPage.verifyEmptyTableData());
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
    catalogsPage.setPropsAt(1, "key1", "value1");
    catalogsPage.addCatalogPropsBtn.click();
    catalogsPage.setPropsAt(2, "key2", "value2");
    clickAndWait(catalogsPage.handleSubmitCatalogBtn);
    // load catalog
    GravitinoMetalake metalake = gravitinoClient.loadMetalake(METALAKE_NAME);
    metalake.loadCatalog(HIVE_CATALOG_NAME);

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
    Assertions.assertEquals(WEB_TITLE, driver.getTitle());
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
    mouseMoveTo(By.xpath(".//*[@data-prev-refer='details-props-key-metastore.uris']"));
    Assertions.assertTrue(
        catalogsPage.verifyShowCatalogDetails(HIVE_CATALOG_NAME, hiveMetastoreUri));
  }

  @Test
  @Order(9)
  public void testEditHiveCatalog() throws InterruptedException {
    catalogsPage.clickEditCatalogBtn(HIVE_CATALOG_NAME);
    catalogsPage.setCatalogNameField(MODIFIED_HIVE_CATALOG_NAME);
    clickAndWait(catalogsPage.handleSubmitCatalogBtn);
    Assertions.assertTrue(catalogsPage.verifyEditedCatalog(MODIFIED_HIVE_CATALOG_NAME));
  }

  // test catalog show schema list
  @Test
  @Order(10)
  public void testClickCatalogLink() {
    catalogsPage.clickCatalogLink(
        METALAKE_NAME, MODIFIED_HIVE_CATALOG_NAME, CATALOG_TYPE_RELATIONAL);
    Assertions.assertTrue(catalogsPage.verifyShowTableTitle(CATALOG_TABLE_TITLE));
    Awaitility.await()
        .atMost(MAX_WAIT_IN_SECONDS, TimeUnit.SECONDS)
        .pollInterval(WAIT_INTERVAL_IN_SECONDS, TimeUnit.SECONDS)
        .untilAsserted(() -> catalogsPage.verifyShowDataItemInList(SCHEMA_NAME, false));
    Assertions.assertTrue(catalogsPage.verifySelectedNode(MODIFIED_HIVE_CATALOG_NAME));
  }

  @Test
  @Order(11)
  public void testRefreshCatalogPage() {
    driver.navigate().refresh();
    Assertions.assertEquals(WEB_TITLE, driver.getTitle());
    Assertions.assertTrue(catalogsPage.verifyShowTableTitle(CATALOG_TABLE_TITLE));
    Assertions.assertTrue(catalogsPage.verifyShowDataItemInList(SCHEMA_NAME, false));
    List<String> treeNodes =
        Arrays.asList(
            MODIFIED_HIVE_CATALOG_NAME,
            SCHEMA_NAME,
            ICEBERG_CATALOG_NAME,
            MYSQL_CATALOG_NAME,
            PG_CATALOG_NAME,
            FILESET_CATALOG_NAME);
    Assertions.assertTrue(catalogsPage.verifyTreeNodes(treeNodes));
    Assertions.assertTrue(catalogsPage.verifySelectedNode(MODIFIED_HIVE_CATALOG_NAME));
  }

  // test schema show table list
  @Test
  @Order(12)
  public void testClickSchemaLink() {
    // create table
    createHiveTableAndColumn(METALAKE_NAME, MODIFIED_HIVE_CATALOG_NAME, SCHEMA_NAME, TABLE_NAME);
    catalogsPage.clickSchemaLink(
        METALAKE_NAME, MODIFIED_HIVE_CATALOG_NAME, CATALOG_TYPE_RELATIONAL, SCHEMA_NAME);
    Assertions.assertTrue(catalogsPage.verifyShowTableTitle(SCHEMA_TABLE_TITLE));
    Assertions.assertTrue(catalogsPage.verifyShowDataItemInList(TABLE_NAME, false));
    Assertions.assertTrue(catalogsPage.verifySelectedNode(SCHEMA_NAME));
  }

  @Test
  @Order(13)
  public void testRefreshSchemaPage() {
    driver.navigate().refresh();
    Assertions.assertEquals(WEB_TITLE, driver.getTitle());
    Assertions.assertTrue(catalogsPage.verifyShowTableTitle(SCHEMA_TABLE_TITLE));
    Assertions.assertTrue(catalogsPage.verifyShowDataItemInList(TABLE_NAME, false));
    List<String> treeNodes =
        Arrays.asList(
            MODIFIED_HIVE_CATALOG_NAME,
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
        METALAKE_NAME,
        MODIFIED_HIVE_CATALOG_NAME,
        CATALOG_TYPE_RELATIONAL,
        SCHEMA_NAME,
        TABLE_NAME);
    Assertions.assertTrue(catalogsPage.verifyShowTableTitle(TABLE_TABLE_TITLE));
    Assertions.assertTrue(catalogsPage.verifyTableColumns());
    Assertions.assertTrue(catalogsPage.verifyShowDataItemInList(COLUMN_NAME, true));
    Assertions.assertTrue(catalogsPage.verifySelectedNode(TABLE_NAME));
  }

  @Test
  @Order(15)
  public void testShowTablePropertiesTooltip() {
    mouseMoveTo(By.xpath("//*[@data-refer='col-icon-" + DISTRIBUTION + "-" + COLUMN_NAME + "']"));
    Assertions.assertTrue(catalogsPage.verifyTableProperties(DISTRIBUTION, COLUMN_NAME));
    mouseMoveTo(By.xpath("//*[@data-refer='col-icon-" + SORT_ORDERS + "-" + COLUMN_NAME_2 + "']"));
    Assertions.assertTrue(catalogsPage.verifyTableProperties(SORT_ORDERS, COLUMN_NAME_2));
    mouseMoveTo(By.xpath("//*[@data-refer='overview-tip-" + SORT_ORDERS + "']"));
    Assertions.assertTrue(
        catalogsPage.verifyTablePropertiesOverview(Arrays.asList(COLUMN_NAME, COLUMN_NAME_2)));
  }

  @Test
  @Order(16)
  public void testRefreshTablePage() {
    driver.navigate().refresh();
    Assertions.assertEquals(WEB_TITLE, driver.getTitle());
    Assertions.assertTrue(catalogsPage.verifyRefreshPage());
    Assertions.assertTrue(catalogsPage.verifyShowTableTitle(TABLE_TABLE_TITLE));
    Assertions.assertTrue(catalogsPage.verifyTableColumns());
    Assertions.assertTrue(catalogsPage.verifyShowDataItemInList(COLUMN_NAME, true));
    List<String> treeNodes =
        Arrays.asList(
            MODIFIED_HIVE_CATALOG_NAME,
            SCHEMA_NAME,
            TABLE_NAME,
            ICEBERG_CATALOG_NAME,
            MYSQL_CATALOG_NAME,
            PG_CATALOG_NAME,
            FILESET_CATALOG_NAME);
    Assertions.assertTrue(catalogsPage.verifyTreeNodes(treeNodes));
  }

  @Test
  @Order(17)
  public void testRelationalHiveCatalogTreeNode() throws InterruptedException {
    String hiveNode =
        String.format(
            "{{%s}}{{%s}}{{%s}}",
            METALAKE_NAME, MODIFIED_HIVE_CATALOG_NAME, CATALOG_TYPE_RELATIONAL);
    catalogsPage.clickTreeNode(hiveNode);
    Assertions.assertTrue(catalogsPage.verifyShowTableTitle(CATALOG_TABLE_TITLE));
    Assertions.assertTrue(catalogsPage.verifyGetCatalog(MODIFIED_HIVE_CATALOG_NAME));
    String schemaNode =
        String.format(
            "{{%s}}{{%s}}{{%s}}{{%s}}",
            METALAKE_NAME, MODIFIED_HIVE_CATALOG_NAME, CATALOG_TYPE_RELATIONAL, SCHEMA_NAME);
    catalogsPage.clickTreeNode(schemaNode);
    Assertions.assertTrue(catalogsPage.verifyShowTableTitle(SCHEMA_TABLE_TITLE));
    Assertions.assertTrue(catalogsPage.verifyShowDataItemInList(TABLE_NAME, false));
    String tableNode =
        String.format(
            "{{%s}}{{%s}}{{%s}}{{%s}}{{%s}}",
            METALAKE_NAME,
            MODIFIED_HIVE_CATALOG_NAME,
            CATALOG_TYPE_RELATIONAL,
            SCHEMA_NAME,
            TABLE_NAME);
    catalogsPage.clickTreeNode(tableNode);
    Assertions.assertTrue(catalogsPage.verifyShowTableTitle(TABLE_TABLE_TITLE));
    Assertions.assertTrue(catalogsPage.verifyShowDataItemInList(COLUMN_NAME, true));
    Assertions.assertTrue(catalogsPage.verifyTableColumns());
  }

  @Test
  @Order(18)
  public void testTreeNodeRefresh() throws InterruptedException {
    createHiveTableAndColumn(METALAKE_NAME, MODIFIED_HIVE_CATALOG_NAME, SCHEMA_NAME, TABLE_NAME_2);
    String hiveNode =
        String.format(
            "{{%s}}{{%s}}{{%s}}",
            METALAKE_NAME, MODIFIED_HIVE_CATALOG_NAME, CATALOG_TYPE_RELATIONAL);
    catalogsPage.clickTreeNode(hiveNode);
    String schemaNode =
        String.format(
            "{{%s}}{{%s}}{{%s}}{{%s}}",
            METALAKE_NAME, MODIFIED_HIVE_CATALOG_NAME, CATALOG_TYPE_RELATIONAL, SCHEMA_NAME);
    catalogsPage.clickTreeNodeRefresh(schemaNode);
    String tableNode =
        String.format(
            "{{%s}}{{%s}}{{%s}}{{%s}}{{%s}}",
            METALAKE_NAME,
            MODIFIED_HIVE_CATALOG_NAME,
            CATALOG_TYPE_RELATIONAL,
            SCHEMA_NAME,
            TABLE_NAME_2);
    catalogsPage.clickTreeNode(tableNode);
    Assertions.assertTrue(catalogsPage.verifyShowTableTitle(TABLE_TABLE_TITLE));
    Assertions.assertTrue(catalogsPage.verifyShowDataItemInList(COLUMN_NAME_2, true));
    Assertions.assertTrue(catalogsPage.verifyTableColumns());
  }

  @Test
  @Order(19)
  public void testCreateTableByUI() throws InterruptedException {
    String schemaNode =
        String.format(
            "{{%s}}{{%s}}{{%s}}{{%s}}",
            METALAKE_NAME, MODIFIED_HIVE_CATALOG_NAME, CATALOG_TYPE_RELATIONAL, SCHEMA_NAME);
    catalogsPage.clickTreeNode(schemaNode);
    // 1. create table in hive catalog without partition/sort order/distribution
    clickAndWait(catalogsPage.createTableBtn);
    catalogsPage.setTableNameField(TABLE_NAME_3);
    catalogsPage.setTableCommentField("table comment for ui");
    catalogsPage.setTableColumnsAt(0, COLUMN_NAME, "byte");
    clickAndWait(catalogsPage.handleSubmitTableBtn);
    Assertions.assertTrue(catalogsPage.verifyShowDataItemInList(TABLE_NAME_3, false));
  }

  @Test
  @Order(20)
  public void testDropTableByUI() throws InterruptedException {
    // delete table of hive catalog
    catalogsPage.clickDeleteBtn(TABLE_NAME_3);
    clickAndWait(catalogsPage.confirmDeleteBtn);
    // verify table list without table name 3
    Assertions.assertTrue(catalogsPage.verifyNoDataItemInList(TABLE_NAME_3, false));
  }

  @Test
  @Order(21)
  public void testOtherRelationalCatalogTreeNode() throws InterruptedException {
    String icebergNode =
        String.format(
            "{{%s}}{{%s}}{{%s}}", METALAKE_NAME, ICEBERG_CATALOG_NAME, CATALOG_TYPE_RELATIONAL);
    catalogsPage.clickTreeNode(icebergNode);
    Assertions.assertTrue(catalogsPage.verifyGetCatalog(ICEBERG_CATALOG_NAME));
    String mysqlNode =
        String.format(
            "{{%s}}{{%s}}{{%s}}", METALAKE_NAME, MYSQL_CATALOG_NAME, CATALOG_TYPE_RELATIONAL);
    catalogsPage.clickTreeNode(mysqlNode);
    Assertions.assertTrue(catalogsPage.verifyGetCatalog(MYSQL_CATALOG_NAME));
    String pgNode =
        String.format(
            "{{%s}}{{%s}}{{%s}}", METALAKE_NAME, PG_CATALOG_NAME, CATALOG_TYPE_RELATIONAL);
    catalogsPage.clickTreeNode(pgNode);
    Assertions.assertTrue(catalogsPage.verifyGetCatalog(PG_CATALOG_NAME));
  }

  @Test
  @Order(22)
  public void testSelectMetalake() throws InterruptedException {
    catalogsPage.metalakeSelectChange(METALAKE_SELECT_NAME);
    Assertions.assertTrue(catalogsPage.verifyEmptyTableData());

    catalogsPage.metalakeSelectChange(METALAKE_NAME);
    driver.navigate().refresh();
  }

  @Test
  @Order(23)
  public void testCreateFilesetSchema() throws InterruptedException {
    // 1. click fileset catalog tree node
    String filesetCatalogNode =
        String.format(
            "{{%s}}{{%s}}{{%s}}", METALAKE_NAME, FILESET_CATALOG_NAME, CATALOG_TYPE_FILESET);
    catalogsPage.clickTreeNode(filesetCatalogNode);
    // 2. click create schema button
    clickAndWait(catalogsPage.createSchemaBtn);
    catalogsPage.setSchemaNameField(SCHEMA_NAME_FILESET);
    catalogsPage.setCatalogCommentField("fileset schema comment");
    clickAndWait(catalogsPage.handleSubmitSchemaBtn);
    // 3. verify show table title、 schema name and tree node
    Assertions.assertTrue(catalogsPage.verifyShowTableTitle(CATALOG_TABLE_TITLE));
    Assertions.assertTrue(catalogsPage.verifyShowDataItemInList(SCHEMA_NAME_FILESET, false));
    List<String> treeNodes =
        Arrays.asList(
            MODIFIED_HIVE_CATALOG_NAME,
            ICEBERG_CATALOG_NAME,
            MYSQL_CATALOG_NAME,
            PG_CATALOG_NAME,
            FILESET_CATALOG_NAME,
            SCHEMA_NAME_FILESET);
    Assertions.assertTrue(catalogsPage.verifyTreeNodes(treeNodes));
  }

  @Test
  @Order(24)
  public void testCreateFileset() throws InterruptedException {
    // 1. click schema tree node
    String filesetSchemaNode =
        String.format(
            "{{%s}}{{%s}}{{%s}}{{%s}}",
            METALAKE_NAME, FILESET_CATALOG_NAME, CATALOG_TYPE_FILESET, SCHEMA_NAME_FILESET);
    catalogsPage.clickTreeNode(filesetSchemaNode);
    // 2. create fileset
    clickAndWait(catalogsPage.createFilesetBtn);
    catalogsPage.setFilesetNameField(FILESET_NAME);
    String storageLocation = storageLocation(SCHEMA_NAME_FILESET, FILESET_NAME);
    catalogsPage.setFilesetStorageLocationField(0, FILESET_DEFAULT_LOCATION, storageLocation);
    catalogsPage.setFilesetCommentField("fileset comment");
    catalogsPage.addFilesetPropsBtn.click();
    catalogsPage.setPropsAt(0, PROPERTIES_KEY1, PROPERTIES_VALUE1);
    clickAndWait(catalogsPage.handleSubmitFilesetBtn);
    // 3. verify show table title、 fileset name and tree node
    Assertions.assertTrue(catalogsPage.verifyShowTableTitle(SCHEMA_FILESET_TITLE));
    Assertions.assertTrue(catalogsPage.verifyShowDataItemInList(FILESET_NAME, false));
    List<String> treeNodes =
        Arrays.asList(
            MODIFIED_HIVE_CATALOG_NAME,
            ICEBERG_CATALOG_NAME,
            MYSQL_CATALOG_NAME,
            PG_CATALOG_NAME,
            FILESET_CATALOG_NAME,
            SCHEMA_NAME_FILESET,
            FILESET_NAME);
    Assertions.assertTrue(catalogsPage.verifyTreeNodes(treeNodes));
    // 4. click fileset tree node
    String filesetNode =
        String.format(
            "{{%s}}{{%s}}{{%s}}{{%s}}{{%s}}",
            METALAKE_NAME,
            FILESET_CATALOG_NAME,
            CATALOG_TYPE_FILESET,
            SCHEMA_NAME_FILESET,
            FILESET_NAME);
    catalogsPage.clickTreeNode(filesetNode);
    // 5. verify Files tab is shown by default, then switch to Details and verify details content
    Assertions.assertTrue(catalogsPage.verifyShowFilesContent());
    clickAndWait(catalogsPage.tabDetailsBtn);
    Assertions.assertTrue(catalogsPage.verifyShowDetailsContent());
    Assertions.assertTrue(
        catalogsPage.verifyShowPropertiesItemInList(
            "key", PROPERTIES_KEY1, PROPERTIES_KEY1, false));
    Assertions.assertTrue(
        catalogsPage.verifyShowPropertiesItemInList(
            "value", PROPERTIES_KEY1, PROPERTIES_VALUE1, false));
  }

  @Test
  @Order(25)
  public void testBackHomePage() throws InterruptedException {
    clickAndWait(catalogsPage.backHomeBtn);
    Assertions.assertTrue(catalogsPage.verifyBackHomePage());
  }
}
