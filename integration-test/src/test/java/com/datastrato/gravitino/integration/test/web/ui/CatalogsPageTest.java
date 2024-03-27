/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.integration.test.web.ui;

import com.datastrato.gravitino.integration.test.container.ContainerSuite;
import com.datastrato.gravitino.integration.test.container.HiveContainer;
import com.datastrato.gravitino.integration.test.web.ui.pages.CatalogsPage;
import com.datastrato.gravitino.integration.test.web.ui.pages.MetalakePage;
import com.datastrato.gravitino.integration.test.web.ui.utils.AbstractWebIT;
import org.apache.hadoop.hive.conf.HiveConf;
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
  private static final ContainerSuite containerSuite = ContainerSuite.getInstance();
  MetalakePage metalakePage = new MetalakePage();
  CatalogsPage catalogsPage = new CatalogsPage();

  private static final String metalakeName = "metalake_name";
  private static final String metalakeSelectName = "metalake_select_name";
  String catalogName = "catalog_name";
  String modifiedCatalogName = catalogName + "_edited";
  String schemaName = "default";
  String tableName = "employee";

  @BeforeAll
  public static void before() {
    containerSuite.startHiveContainer();

    // Initial hive client
    HiveConf hiveConf = new HiveConf();
    String hiveMetastoreUris =
        String.format(
            "thrift://%s:%d",
            containerSuite.getHiveContainer().getContainerIpAddress(),
            HiveContainer.HIVE_METASTORE_PORT);
    hiveConf.set(HiveConf.ConfVars.METASTOREURIS.varname, hiveMetastoreUris);
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
    String hiveMetastoreUris =
        String.format(
            "thrift://%s:%d",
            containerSuite.getHiveContainer().getContainerIpAddress(),
            HiveContainer.HIVE_METASTORE_PORT);
    catalogsPage.setHiveCatalogURI(hiveMetastoreUris);
    catalogsPage.addCatalogPropsBtn.click();
    catalogsPage.setCatalogProps(1, "key1", "value1");
    catalogsPage.addCatalogPropsBtn.click();
    catalogsPage.setCatalogProps(2, "key2", "value2");
    clickAndWait(catalogsPage.handleSubmitCatalogBtn);

    Assertions.assertTrue(catalogsPage.verifyCreateHiveCatalog(catalogName));
  }

  @Test
  @Order(2)
  public void testRefreshPage() {
    driver.navigate().refresh();
    Assertions.assertEquals(driver.getTitle(), "Gravitino");
    Assertions.assertTrue(catalogsPage.verifyRefreshPage());
  }

  @Test
  @Order(3)
  public void testViewTabMetalakeDetails() throws InterruptedException {
    clickAndWait(catalogsPage.tabDetailsBtn);
    Assertions.assertTrue(catalogsPage.verifyShowDetailsContent());
    clickAndWait(catalogsPage.tabTableBtn);
    Assertions.assertTrue(catalogsPage.verifyShowTableContent());
  }

  @Test
  @Order(4)
  public void testViewCatalogDetails() throws InterruptedException {
    catalogsPage.clickViewCatalogBtn(catalogName);
    String hiveMetastoreUris =
        String.format(
            "thrift://%s:%d",
            containerSuite.getHiveContainer().getContainerIpAddress(),
            HiveContainer.HIVE_METASTORE_PORT);
    Assertions.assertTrue(catalogsPage.verifyShowCatalogDetails(catalogName, hiveMetastoreUris));
  }

  @Test
  @Order(5)
  public void testEditCatalog() throws InterruptedException {
    catalogsPage.clickEditCatalogBtn(catalogName);
    catalogsPage.setCatalogNameField(modifiedCatalogName);
    clickAndWait(catalogsPage.handleSubmitCatalogBtn);
    Assertions.assertTrue(catalogsPage.verifyEditedCatalog(modifiedCatalogName));
  }

  @Test
  @Order(6)
  public void testClickCatalogLink() {
    catalogsPage.clickCatalogLink(metalakeName, modifiedCatalogName);
    Assertions.assertTrue(catalogsPage.verifyShowTableTitle("Schemas"));
  }

  @Test
  @Order(7)
  public void testClickSchemaLink() {
    catalogsPage.clickSchemaLink(metalakeName, modifiedCatalogName, schemaName);
    Assertions.assertTrue(catalogsPage.verifyShowTableTitle("Tables"));
  }

  @Test
  @Order(8)
  public void testClickTableLink() {
    catalogsPage.clickTableLink(metalakeName, modifiedCatalogName, schemaName, tableName);
    Assertions.assertTrue(catalogsPage.verifyShowTableTitle("Columns"));
    Assertions.assertTrue(catalogsPage.verifyTableColumns());
  }

  @Test
  @Order(9)
  public void testSelectMetalake() throws InterruptedException {
    catalogsPage.metalakeSelectChange(metalakeSelectName);
    Assertions.assertTrue(catalogsPage.verifyEmptyCatalog());

    catalogsPage.metalakeSelectChange(metalakeName);
    Assertions.assertTrue(catalogsPage.verifyCreateHiveCatalog(modifiedCatalogName));
  }

  @Test
  @Order(10)
  public void testDeleteCatalog() throws InterruptedException {
    catalogsPage.clickBreadCrumbsToCatalogs();
    catalogsPage.clickDeleteCatalogBtn(modifiedCatalogName);
    clickAndWait(catalogsPage.confirmDeleteBtn);
    Assertions.assertTrue(catalogsPage.verifyEmptyCatalog());
  }


  @Test
  @Order(11)
  public void testBackHomePage() throws InterruptedException {
    clickAndWait(catalogsPage.backHomeBtn);
    Assertions.assertTrue(catalogsPage.verifyBackHomePage());
  }
}
