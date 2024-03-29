/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.integration.test.web.ui;

import com.datastrato.gravitino.integration.test.container.ContainerSuite;
import com.datastrato.gravitino.integration.test.container.HiveContainer;
import com.datastrato.gravitino.integration.test.util.ITUtils;
import com.datastrato.gravitino.integration.test.util.JdbcDriverDownloader;
import com.datastrato.gravitino.integration.test.web.ui.pages.CatalogsPage;
import com.datastrato.gravitino.integration.test.web.ui.pages.MetalakePage;
import com.datastrato.gravitino.integration.test.web.ui.utils.AbstractWebIT;
import java.io.IOException;
import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.testcontainers.containers.PostgreSQLContainer;

@Tag("gravitino-docker-it")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class CatalogsPageTest extends AbstractWebIT {
  private static final ContainerSuite containerSuite = ContainerSuite.getInstance();
//  private PostgreSQLContainer<?> pgContainer;
  MetalakePage metalakePage = new MetalakePage();
  CatalogsPage catalogsPage = new CatalogsPage();

  private static final String metalakeName = "metalake_name";
  private static final String metalakeSelectName = "metalake_select_name";
  String catalogName = "catalog_name";
  String catalogType = "relational";
  String modifiedCatalogName = catalogName + "_edited";
  String schemaName = "default";
  String tableName = "employee";
  String icebergCatalogName = "catalog_iceberg";
  String mysqlCatalogName = "catalog_mysql";
  static String pgCatalogName = "catalog_pg";

  @BeforeAll
  public static void before() throws IOException {
    containerSuite.startHiveContainer();

    // Initial hive client
    HiveConf hiveConf = new HiveConf();
    String hiveIpAddress = containerSuite.getHiveContainer().getContainerIpAddress();
    String hiveMetastoreUris =
        String.format("thrift://%s:%d", hiveIpAddress, HiveContainer.HIVE_METASTORE_PORT);
    hiveConf.set(HiveConf.ConfVars.METASTOREURIS.varname, hiveMetastoreUris);

    String testMode =
        System.getProperty(ITUtils.TEST_MODE) == null
            ? ITUtils.EMBEDDED_TEST_MODE
            : System.getProperty(ITUtils.TEST_MODE);

    // Deploy mode, you should download jars to the Gravitino server iceberg lib directory
    if (!ITUtils.EMBEDDED_TEST_MODE.equals(testMode)) {
      String gravitinoHome = System.getenv("GRAVITINO_HOME");
      String destPath = ITUtils.joinPath(gravitinoHome, "catalogs", "lakehouse-iceberg", "libs");
      String mysqlPath = ITUtils.joinPath(gravitinoHome, "catalogs", "jdbc-mysql", "libs");
      String pgPath = ITUtils.joinPath(gravitinoHome, "catalogs", "jdbc-postgresql", "libs");

      JdbcDriverDownloader.downloadJdbcDriver(
          "https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.27/mysql-connector-java-8.0.27.jar",
          destPath,
          mysqlPath);
      JdbcDriverDownloader.downloadJdbcDriver(
          "https://jdbc.postgresql.org/download/postgresql-42.7.0.jar", destPath, pgPath);
    }
  }

  @AfterAll
  public static void after() {
    try {
      closer.close();
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
    }
  }

  @Test
  @Order(1)
  public void testCreateHiveCatalog() throws InterruptedException {
    // Create metalake first
    clickAndWait(metalakePage.createMetalakeBtn);
    metalakePage.setMetalakeNameField(metalakeName);
    clickAndWait(metalakePage.submitHandleMetalakeBtn);
    // Create another metalake for select option
    clickAndWait(metalakePage.createMetalakeBtn);
    metalakePage.setMetalakeNameField(metalakeSelectName);
    clickAndWait(metalakePage.submitHandleMetalakeBtn);
    metalakePage.clickMetalakeLink(metalakeName);
    // Create catalog
    clickAndWait(catalogsPage.createCatalogBtn);
    catalogsPage.setCatalogNameField(catalogName);
    catalogsPage.setCatalogCommentField("catalog comment");
    String ipAddress = containerSuite.getHiveContainer().getContainerIpAddress();
    String hiveMetastoreUris =
        String.format("thrift://%s:%d", ipAddress, HiveContainer.HIVE_METASTORE_PORT);
    catalogsPage.setCatalogPropInput("metastore.uris", hiveMetastoreUris);
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
    String ipAddress = containerSuite.getHiveContainer().getContainerIpAddress();
    String uris = String.format("thrift://%s:%d", ipAddress, HiveContainer.HIVE_METASTORE_PORT);
    catalogsPage.setCatalogPropInput("uri", uris);
    // set iceberg warehouse
    String warehouse = String.format("hdfs://%s:9000/user/hive/warehouse", ipAddress);
    catalogsPage.setCatalogPropInput("warehouse", warehouse);
    clickAndWait(catalogsPage.handleSubmitCatalogBtn);
    Assertions.assertTrue(catalogsPage.verifyCreateCatalog(icebergCatalogName));
  }

  @Test
  @Order(3)
  public void testCreateMysqlCatalog() throws InterruptedException {
    // initial mysql before
    String[] command = {
      "mysql",
      "-h127.0.0.1",
      "-uroot",
      "-pds123",
      "-e",
      "grant all privileges on *.* to root@'%' identified by 'ds123'"
    };
    // There exists a mysql instance in Hive the container.
    containerSuite.getHiveContainer().executeInContainer(command);
    // create mysql catalog actions
    clickAndWait(catalogsPage.createCatalogBtn);
    catalogsPage.setCatalogNameField(mysqlCatalogName);
    // select provider as mysql
    clickAndWait(catalogsPage.catalogProviderSelector);
    catalogsPage.clickSelectProvider("jdbc-mysql");
    catalogsPage.setCatalogCommentField("mysql catalog comment");
    // set mysql catalog props
    catalogsPage.setCatalogPropInput("jdbc-driver", "com.mysql.cj.jdbc.Driver");
    String ipAddress = containerSuite.getHiveContainer().getContainerIpAddress();
    String urlAddress = String.format("jdbc:mysql://%s:3306?useSSL=false", ipAddress);
    catalogsPage.setCatalogPropInput("jdbc-url", urlAddress);
    catalogsPage.setCatalogPropInput("jdbc-user", "root");
    catalogsPage.setCatalogPropInput("jdbc-password", "ds123");
    clickAndWait(catalogsPage.handleSubmitCatalogBtn);
    Assertions.assertTrue(catalogsPage.verifyCreateCatalog(mysqlCatalogName));
  }

  @Test
  @Order(4)
  public void testCreatePgCatalog() throws InterruptedException {
    String trinoConfDir = System.getenv("TRINO_CONF_DIR");
    containerSuite.startTrinoContainer(
            trinoConfDir,
            System.getenv("GRAVITINO_ROOT_DIR") + "/trino-connector/build/libs",
            getGravitinoServerPort(),
            metalakeName);

    // create postgresql catalog actions
    clickAndWait(catalogsPage.createCatalogBtn);
    catalogsPage.setCatalogNameField(pgCatalogName);
    // select provider as mysql
    clickAndWait(catalogsPage.catalogProviderSelector);
    catalogsPage.clickSelectProvider("jdbc-postgresql");
    catalogsPage.setCatalogCommentField("postgresql catalog comment");
    // set mysql catalog props
    catalogsPage.setCatalogPropInput("jdbc-driver", "org.postgresql.Driver");
    String ipAddress = containerSuite.getTrinoContainer().getContainerIpAddress();
    LOG.info(ipAddress);
    String urlAddress = String.format("jdbc:postgresql://%s:5432/gt_db", ipAddress);
    LOG.info(urlAddress);
    catalogsPage.setCatalogPropInput("jdbc-url", urlAddress);
    catalogsPage.setCatalogPropInput("jdbc-user", "trino");
    catalogsPage.setCatalogPropInput("jdbc-password", "ds123");
    catalogsPage.setCatalogPropInput("jdbc-database", "gt_db");

    clickAndWait(catalogsPage.handleSubmitCatalogBtn);
    containerSuite
            .getTrinoContainer()
            .checkSyncCatalogFromGravitino(5, metalakeName, pgCatalogName);
    Assertions.assertTrue(catalogsPage.verifyCreateCatalog(pgCatalogName));

    Thread.sleep(100_000);
  }

  @Test
  @Order(5)
  public void testRefreshPage() {
    driver.navigate().refresh();
    Assertions.assertEquals(driver.getTitle(), "Gravitino");
    Assertions.assertTrue(catalogsPage.verifyRefreshPage());
  }

  @Test
  @Order(6)
  public void testViewTabMetalakeDetails() throws InterruptedException {
    clickAndWait(catalogsPage.tabDetailsBtn);
    Assertions.assertTrue(catalogsPage.verifyShowDetailsContent());
    clickAndWait(catalogsPage.tabTableBtn);
    Assertions.assertTrue(catalogsPage.verifyShowTableContent());
  }

  @Test
  @Order(7)
  public void testViewCatalogDetails() throws InterruptedException {
    catalogsPage.clickViewCatalogBtn(catalogName);
    String ipAddress = containerSuite.getHiveContainer().getContainerIpAddress();
    String uris = String.format("thrift://%s:%d", ipAddress, HiveContainer.HIVE_METASTORE_PORT);
    Assertions.assertTrue(catalogsPage.verifyShowCatalogDetails(catalogName, uris));
  }

  @Test
  @Order(8)
  public void testEditCatalog() throws InterruptedException {
    catalogsPage.clickEditCatalogBtn(catalogName);
    catalogsPage.setCatalogNameField(modifiedCatalogName);
    clickAndWait(catalogsPage.handleSubmitCatalogBtn);
    Assertions.assertTrue(catalogsPage.verifyEditedCatalog(modifiedCatalogName));
  }

  @Test
  @Order(9)
  public void testClickCatalogLink() {
    catalogsPage.clickCatalogLink(metalakeName, modifiedCatalogName, catalogType);
    Assertions.assertTrue(catalogsPage.verifyShowTableTitle("Schemas"));
  }

  @Test
  @Order(10)
  public void testClickSchemaLink() {
    catalogsPage.clickSchemaLink(metalakeName, modifiedCatalogName, catalogType, schemaName);
    Assertions.assertTrue(catalogsPage.verifyShowTableTitle("Tables"));
  }

  @Test
  @Order(11)
  public void testClickTableLink() {
    catalogsPage.clickTableLink(
        metalakeName, modifiedCatalogName, catalogType, schemaName, tableName);
    Assertions.assertTrue(catalogsPage.verifyShowTableTitle("Columns"));
    Assertions.assertTrue(catalogsPage.verifyTableColumns());
  }

  @Test
  @Order(12)
  public void testSelectMetalake() throws InterruptedException {
    catalogsPage.metalakeSelectChange(metalakeSelectName);
    Assertions.assertTrue(catalogsPage.verifyEmptyCatalog());

    catalogsPage.metalakeSelectChange(metalakeName);
    Assertions.assertTrue(catalogsPage.verifyCreateCatalog(modifiedCatalogName));
  }

  @Test
  @Order(13)
  public void testDeleteCatalog() throws InterruptedException {
    catalogsPage.clickBreadCrumbsToCatalogs();
    catalogsPage.clickDeleteCatalogBtn(modifiedCatalogName);
    clickAndWait(catalogsPage.confirmDeleteBtn);
    Assertions.assertTrue(catalogsPage.verifyEmptyCatalog());
  }

  @Test
  @Order(14)
  public void testBackHomePage() throws InterruptedException {
    clickAndWait(catalogsPage.backHomeBtn);
    Assertions.assertTrue(catalogsPage.verifyBackHomePage());
  }
}
