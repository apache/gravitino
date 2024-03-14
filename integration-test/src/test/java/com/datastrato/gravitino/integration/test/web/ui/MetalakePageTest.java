/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.integration.test.web.ui;

import com.datastrato.gravitino.integration.test.web.ui.Pages.MetalakePage;
import com.datastrato.gravitino.integration.test.web.ui.utils.AbstractWebIT;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.BeforeEach;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class MetalakePageTest extends AbstractWebIT {
  MetalakePage metalakePage = new MetalakePage();

  String metalakeName = "metalake_name";
  String editedMetalakeName = metalakeName + "_edited";

  @BeforeEach
  public void beforeEachTest() {
    try {
      Thread.sleep(metalakePage.sleepTimeMillis);
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
    }
  }

  // Create a metalake by name, set the default comment and properties.
  public void createMetalakeAction(String name) {
    metalakePage.createMetalakeBtn.click();
    metalakePage.setMetalakeNameField(name);
    metalakePage.setMetalakeCommentField("metalake comment");
    metalakePage.addMetalakePropsBtn.click();
    metalakePage.setMetalakeProps(0, "key1", "value1");
    metalakePage.addMetalakePropsBtn.click();
    metalakePage.setMetalakeProps(1, "key2", "value2");
    metalakePage.submitHandleMetalakeBtn.click();
  }

  @Test
  @Order(0)
  public void homePage() {
    String title = driver.getTitle();
    Assertions.assertEquals("Gravitino", title);
  }

  @Test
  @Order(1)
  public void testCreateMetalake() {
    createMetalakeAction(metalakeName);
    Assertions.assertTrue(metalakePage.verifyCreateMetalake(metalakeName));
  }

  @Test
  @Order(2)
  public void testViewMetalakeDetails() {
    metalakePage.clickViewMetalakeBtn(metalakeName);
    Assertions.assertTrue(metalakePage.verifyShowMetalakeDetails(metalakeName));
  }

  @Test
  @Order(3)
  public void testEditMetalake() {
    metalakePage.clickEditMetalakeBtn(metalakeName);
    metalakePage.setMetalakeNameField(editedMetalakeName);
    metalakePage.submitHandleMetalakeBtn.click();
    Assertions.assertTrue(metalakePage.verifyEditedMetalake(editedMetalakeName));
  }

  @Test
  @Order(4)
  public void testDeleteMetalake() {
    metalakePage.clickDeleteMetalakeBtn(editedMetalakeName);
    metalakePage.confirmDeleteBtn.click();
    Assertions.assertTrue(metalakePage.verifyEmptyMetalake());
  }

  @Test
  @Order(5)
  public void testCreateMultipleMetalakes() {
    int twoPagesCount = 11;

    for (int i = 0; i < twoPagesCount; i++) {
      try {
        Thread.sleep(800);
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
  public void testQueryMetalake() {
    String name = "query";
    createMetalakeAction(name);
    Assertions.assertTrue(metalakePage.verifyQueryMetalake(name));
  }

  @Test
  @Order(7)
  public void testCreateInvalidMetalake() {
    String name = "1!@#$";
    metalakePage.createMetalakeBtn.click();
    metalakePage.setMetalakeNameField(name);
    metalakePage.submitHandleMetalakeBtn.click();
    Assertions.assertTrue(metalakePage.checkIsErrorName());
  }

  //  https://github.com/datastrato/gravitino/issues/2512
  //  @Test
  //  @Order(7)
  //  public void testLinkToCatalogsPage() {
  //    String name = "a_link";
  //    createMetalakeAction(true, name);
  //    metalakePage.clickMetalakeLink(name);
  //    metalakePage.waitLinkElementDisplayed(name);
  //    Assertions.assertTrue(metalakePage.verifyLinkToCatalogsPage(name));
  //  }
}
