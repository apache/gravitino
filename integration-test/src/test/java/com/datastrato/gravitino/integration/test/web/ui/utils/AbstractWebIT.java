/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.integration.test.web.ui.utils;

import com.datastrato.gravitino.integration.test.util.AbstractIT;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.openqa.selenium.WebDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// AbstractWebIT provides a WebDriver instance for WEB UI tests.
public class AbstractWebIT extends AbstractIT {
  protected static final Logger LOG = LoggerFactory.getLogger(AbstractWebIT.class);
  protected static WebDriver driver;

  // https://www.selenium.dev/documentation/webdriver/waits/#implicit-waits
  protected static final long MAX_IMPLICIT_WAIT = 10;

  @BeforeAll
  public static void startUp() {
    driver = WebDriverManager.getWebDriver(getGravitinoServerPort());
  }

  @AfterAll
  public static void tearDown() {
    driver.quit();
  }
}
