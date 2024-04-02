/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.integration.test.web.ui;

import com.datastrato.gravitino.integration.test.web.ui.pages.MetalakePage;
import com.datastrato.gravitino.integration.test.web.ui.utils.AbstractWebIT;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class MetalakePageTest extends AbstractWebIT {
  private static final String WEB_TITLE = "Gravitino";
  private static final String METALAKE_NAME = "metalake_name";
  private static final String EDITED_METALAKE_NAME = METALAKE_NAME + "_edited";
  private static final String FOOTER_LINK_DATASTRATO = "https://datastrato.ai/";
  private static final String FOOTER_LINK_DOCS = "https://datastrato.ai/docs/";
  private static final String FOOTER_LINK_LICENSE =
      "https://github.com/datastrato/gravitino/blob/main/LICENSE";
  private static final String FOOTER_LINK_SUPPORT =
      "https://github.com/datastrato/gravitino/issues";
  MetalakePage metalakePage = new MetalakePage();

  // Create a metalake by name, set the default comment and properties.
  public void createMetalakeAction(String name) throws InterruptedException {
    clickAndWait(metalakePage.createMetalakeBtn);
    metalakePage.setMetalakeNameField(name);
    metalakePage.setMetalakeCommentField("metalake comment");
    metalakePage.addMetalakePropsBtn.click();
    metalakePage.setMetalakeProps(0, "key1", "value1");
    metalakePage.addMetalakePropsBtn.click();
    metalakePage.setMetalakeProps(1, "key2", "value2");
    clickAndWait(metalakePage.submitHandleMetalakeBtn);
  }

  @Test
  @Order(0)
  public void homePage() {
    String title = driver.getTitle();
    Assertions.assertEquals(WEB_TITLE, title);
  }

  @Test
  @Order(1)
  public void testCreateMetalake() throws InterruptedException {
    createMetalakeAction(METALAKE_NAME);
    Assertions.assertTrue(metalakePage.verifyCreateMetalake(METALAKE_NAME));
  }

  @Test
  @Order(2)
  public void testViewMetalakeDetails() throws InterruptedException {
    metalakePage.clickViewMetalakeBtn(METALAKE_NAME);
    Assertions.assertTrue(metalakePage.verifyShowMetalakeDetails(METALAKE_NAME));
  }

  @Test
  @Order(3)
  public void testEditMetalake() {
    metalakePage.clickEditMetalakeBtn(METALAKE_NAME);
    metalakePage.setMetalakeNameField(EDITED_METALAKE_NAME);
    metalakePage.submitHandleMetalakeBtn.click();
    Assertions.assertTrue(metalakePage.verifyEditedMetalake(EDITED_METALAKE_NAME));
  }

  @Test
  @Order(4)
  public void testDeleteMetalake() {
    metalakePage.clickDeleteMetalakeBtn(EDITED_METALAKE_NAME);
    metalakePage.confirmDeleteBtn.click();
    Assertions.assertTrue(metalakePage.verifyEmptyMetalake());
  }

  @Test
  @Order(5)
  public void testCreateMultipleMetalakes() throws InterruptedException {
    int twoPagesCount = 11;

    for (int i = 0; i < twoPagesCount; i++) {
      try {
        Thread.sleep(ACTION_SLEEP_MILLIS);
      } catch (Exception e) {
        LOG.error(e.getMessage(), e);
      }
      String name = "metalake_" + (i + 1);
      createMetalakeAction(name);
    }

    Assertions.assertTrue(metalakePage.verifyChangePagination());
  }

  @Test
  @Order(6)
  public void testQueryMetalake() throws InterruptedException {
    String name = "query";
    createMetalakeAction(name);
    Assertions.assertTrue(metalakePage.verifyQueryMetalake(name));
  }

  @Test
  @Order(7)
  public void testCreateInvalidMetalake() throws InterruptedException {
    String name = "1!@#$";
    metalakePage.createMetalakeBtn.click();
    metalakePage.setMetalakeNameField(name);
    metalakePage.submitHandleMetalakeBtn.click();
    Assertions.assertTrue(metalakePage.checkIsErrorName());
  }

  @Test
  @Order(8)
  public void testLinkToCatalogsPage() throws InterruptedException {
    String name = "a_test_link";
    createMetalakeAction(name);
    metalakePage.clickMetalakeLink(name);
    Assertions.assertTrue(metalakePage.verifyLinkToCatalogsPage(name));
  }

  @Test
  @Order(9)
  public void testRefreshPage() {
    driver.navigate().refresh();

    Assertions.assertEquals(driver.getTitle(), WEB_TITLE);
    Assertions.assertTrue(metalakePage.verifyRefreshPage());
  }

  @Test
  @Order(10)
  public void testCheckLinkDatastrato() {
    String originalWindowHandle = driver.getWindowHandle();
    metalakePage.footerLinkDatastrato.click();
    Assertions.assertTrue(
        metalakePage.verifyLinkInNewWindow(originalWindowHandle, FOOTER_LINK_DATASTRATO, false));
  }

  @Test
  @Order(11)
  public void testCheckLinkLicense() {
    String originalWindowHandle = driver.getWindowHandle();
    metalakePage.footerLinkLicense.click();
    Assertions.assertTrue(
        metalakePage.verifyLinkInNewWindow(originalWindowHandle, FOOTER_LINK_LICENSE, false));
  }

  @Test
  @Order(12)
  public void testCheckLinkDocs() {
    String originalWindowHandle = driver.getWindowHandle();
    metalakePage.footerLinkDocs.click();
    Assertions.assertTrue(
        metalakePage.verifyLinkInNewWindow(originalWindowHandle, FOOTER_LINK_DOCS, true));
  }

  @Test
  @Order(13)
  public void testCheckLinkSupport() {
    String originalWindowHandle = driver.getWindowHandle();
    metalakePage.footerLinkSupport.click();
    Assertions.assertTrue(
        metalakePage.verifyLinkInNewWindow(originalWindowHandle, FOOTER_LINK_SUPPORT, false));
  }
}
