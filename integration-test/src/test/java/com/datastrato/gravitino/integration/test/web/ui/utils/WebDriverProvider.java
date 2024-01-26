/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.integration.test.web.ui.utils;

import org.openqa.selenium.WebDriver;

// WebDriverProvider is an interface for WEB Driver provider.
public interface WebDriverProvider {

  /** Download the browser web driver. */
  public void downloadWebDriver();

  /** create a new browser web driver */
  public WebDriver createWebDriver();
}
