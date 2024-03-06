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
  MetalakePage metalakePage = new MetalakePage(driver);

  @Test
  public void homePage() {
    String title = driver.getTitle();
    Assertions.assertEquals("Gravitino", title);
  }

  @Test
  @Order(1)
  public void testCreateMetalake() {
    metalakePage.createMetalakeAction("test", "test", true);

    boolean status = metalakePage.verifyIsCreatedMetalake();

    if (status) {
      LOG.info("create metalake successful");
    } else {
      Assertions.fail("create metalake failed");
    }
  }

  @Test
  @Order(2)
  public void testQueryMetalake() {
    metalakePage.queryMetalakeAction();

    boolean status = metalakePage.verifyQueryMetalake();

    if (status) {
      LOG.info("query metalake successful");
    } else {
      Assertions.fail("query metalake failed");
    }
  }

  @Test
  @Order(3)
  public void testViewMetalakeDetails() {
    metalakePage.viewMetalakeAction();

    boolean status = metalakePage.verifyIsShowDetails();

    if (status) {
      LOG.info("view metalake details successful");
    } else {
      Assertions.fail("view metalake details failed");
    }
  }

  @Test
  @Order(4)
  public void testEditMetalake() {
    metalakePage.editMetalakeAction();

    boolean status = metalakePage.verifyIsEditedMetalake();

    if (status) {
      LOG.info("edit metalake successful");
    } else {
      Assertions.fail("edit metalake failed");
    }
  }

  @Test
  @Order(5)
  public void testDeleteMetalake() {
    metalakePage.deleteMetalakeAction();

    boolean status = metalakePage.verifyIsDeletedMetalake();

    if (status) {
      LOG.info("delete metalake successful");
    } else {
      Assertions.fail("delete metalake failed");
    }
  }

  @Test
  @Order(6)
  public void testCreateManyMetalakes() {
    metalakePage.createManyMetalakesAction();

    boolean status = metalakePage.verifyIsCreatedManyMetalakes();

    if (status) {
      LOG.info("create many metalakes successful");
    } else {
      Assertions.fail("create many metalakes failed");
    }
  }

  @Test
  @Order(7)
  public void testCreateMetalakeWithProperty() {
    metalakePage.createMetalakeWithPropertyAction();

    boolean status = metalakePage.verifyIsCreatedMetalakeWithProperty();

    if (status) {
      LOG.info("create metalake with property successful");
    } else {
      Assertions.fail("create metalake with property failed");
    }
  }

  @Test
  @Order(8)
  public void testLinkToCatalogsPage() {
    metalakePage.linkToCatalogsPageAction();

    boolean status = metalakePage.verifyIsLinkedToCatalogsPage();

    if (status) {
      LOG.info("link to catalogs page successful");
    } else {
      Assertions.fail("link to catalogs page failed");
    }
  }
}
