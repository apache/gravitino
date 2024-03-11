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

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class MetalakePageTest extends AbstractWebIT {
  MetalakePage metalakePage = new MetalakePage();

  // Create a metalake by name, set the default comment and properties.
  public void createMetalakeAction(boolean valid, String name) {
    metalakePage.createMetalakeBtn.click();
    String invalidPrefix = "1!@#$";
    String concatName = valid ? name : invalidPrefix + name;
    metalakePage.setMetalakeNameField(concatName);
    metalakePage.setMetalakeCommentField("metalake comment");
    metalakePage.addMetalakePropsBtn.click();
    metalakePage.setMetalakeProps(0, "key1", "value1");
    metalakePage.addMetalakePropsBtn.click();
    metalakePage.setMetalakeProps(1, "key2", "value2");

    try {
      if (!valid && metalakePage.checkIsErrorName()) {
        metalakePage.setMetalakeNameField(name);
      }
      metalakePage.submitHandleMetalakeBtn.click();
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
    }
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
    String name = "metalake_name";
    createMetalakeAction(false, name);
    Assertions.assertTrue(metalakePage.verifyCreateMetalake(name));
  }

  @Test
  @Order(2)
  public void testViewMetalakeDetails() {
    metalakePage.clickViewMetalakeBtn("metalake_name");
    Assertions.assertTrue(metalakePage.verifyShowMetalakeDetails());
  }

  @Test
  @Order(3)
  public void testEditMetalake() {
    metalakePage.clickEditMetalakeBtn("metalake_name");
    metalakePage.setMetalakeNameField("metalake_name_edited");
    metalakePage.submitHandleMetalakeBtn.click();
    Assertions.assertTrue(metalakePage.verifyEditedMetalake("metalake_name_edited"));
  }

  @Test
  @Order(4)
  public void testDeleteMetalake() {
    metalakePage.clickDeleteMetalakeBtn("metalake_name_edited");
    metalakePage.confirmDeleteBtn.click();
    Assertions.assertTrue(metalakePage.verifyEmptyMetalake());
  }

  @Test
  @Order(5)
  public void testCreateMultipleMetalakes() {
    int towPagesCount = 11;

    for (int i = 0; i < towPagesCount; i++) {
      String name = "metalake_" + (i + 1);
      createMetalakeAction(true, name);
    }

    Assertions.assertTrue(metalakePage.verifyChangePagination());
  }

  @Test
  @Order(6)
  public void testQueryMetalake() {
    String name = "query";
    createMetalakeAction(true, name);
    Assertions.assertTrue(metalakePage.verifyQueryMetalake(name));
  }

  @Test
  @Order(8)
  public void testLinkToCatalogsPage() {
    String name = "a_link";
    createMetalakeAction(true, name);
    metalakePage.waitLinkElementDisplayed(name);
    metalakePage.findElementByLink(name).click();
    Assertions.assertTrue(metalakePage.verifyLinkToCatalogsPage(name));
  }
}
