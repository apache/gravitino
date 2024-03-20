/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.integration.test.web.ui;

import com.datastrato.gravitino.integration.test.web.ui.pages.MetalakePage;
import com.datastrato.gravitino.integration.test.web.ui.utils.AbstractWebIT;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class MetalakePageTest extends AbstractWebIT {
  MetalakePage metalakePage = new MetalakePage();

  String metalakeName = "metalake_name";
  String editedMetalakeName = metalakeName + "_edited";

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
  @Disabled
  @Order(0)
  public void homePage() {
    String title = driver.getTitle();
    Assertions.assertEquals("Gravitino", title);
  }

  @Test
  @Disabled
  @Order(1)
  public void testCreateMetalake() throws InterruptedException {
    createMetalakeAction(metalakeName);
    Assertions.assertTrue(metalakePage.verifyCreateMetalake(metalakeName));
  }

  @Test
  @Disabled
  @Order(2)
  public void testViewMetalakeDetails() throws InterruptedException {
    metalakePage.clickViewMetalakeBtn(metalakeName);
    Assertions.assertTrue(metalakePage.verifyShowMetalakeDetails(metalakeName));
  }

  @Test
  @Disabled
  @Order(3)
  public void testEditMetalake() {
    metalakePage.clickEditMetalakeBtn(metalakeName);
    metalakePage.setMetalakeNameField(editedMetalakeName);
    metalakePage.submitHandleMetalakeBtn.click();
    Assertions.assertTrue(metalakePage.verifyEditedMetalake(editedMetalakeName));
  }

  @Test
  @Disabled
  @Order(4)
  public void testDeleteMetalake() {
    metalakePage.clickDeleteMetalakeBtn(editedMetalakeName);
    metalakePage.confirmDeleteBtn.click();
    Assertions.assertTrue(metalakePage.verifyEmptyMetalake());
  }

  @Test
  @Disabled
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
  @Disabled
  @Order(6)
  public void testQueryMetalake() throws InterruptedException {
    String name = "query";
    createMetalakeAction(name);
    Assertions.assertTrue(metalakePage.verifyQueryMetalake(name));
  }

  @Test
  @Disabled
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
}
