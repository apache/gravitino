/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.integration.test.web.ui.utils;

import com.datastrato.gravitino.integration.test.util.AbstractIT;
import com.google.common.base.Function;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.openqa.selenium.By;
import org.openqa.selenium.ElementClickInterceptedException;
import org.openqa.selenium.InvalidArgumentException;
import org.openqa.selenium.NoSuchElementException;
import org.openqa.selenium.TimeoutException;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.interactions.Actions;
import org.openqa.selenium.support.ui.ExpectedConditions;
import org.openqa.selenium.support.ui.FluentWait;
import org.openqa.selenium.support.ui.Wait;
import org.openqa.selenium.support.ui.WebDriverWait;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// AbstractWebIT provides a WebDriver instance for WEB UI tests.
public class AbstractWebIT extends AbstractIT {
  protected static final Logger LOG = LoggerFactory.getLogger(AbstractWebIT.class);
  protected static WebDriver driver;

  // https://www.selenium.dev/documentation/webdriver/waits/#implicit-waits
  protected static final long MAX_IMPLICIT_WAIT = 30;
  protected static final long MAX_TIMEOUT = 20;
  protected static final long SLEEP_MILLIS = 1_000;

  protected boolean waitShowText(final String text, final By locator) {
    try {
      WebElement element = pollingWait(locator, MAX_TIMEOUT);
      return text.equals(element.getText());
    } catch (TimeoutException e) {
      return false;
    }
  }

  protected WebElement pollingWait(final By locator, final long maxTimeout) {
    Wait<WebDriver> wait =
        new FluentWait<>(driver)
            .withTimeout(Duration.of(maxTimeout, ChronoUnit.SECONDS))
            .pollingEvery(Duration.of(1, ChronoUnit.SECONDS))
            .ignoring(NoSuchElementException.class);

    return wait.until((Function<WebDriver, WebElement>) driver -> driver.findElement(locator));
  }

  protected void clickAndWait(final Object locatorOrElement) throws InterruptedException {
    WebElement element;
    if (locatorOrElement instanceof By) {
      element = pollingWait((By) locatorOrElement, MAX_IMPLICIT_WAIT);
    } else if (locatorOrElement instanceof WebElement) {
      element = (WebElement) locatorOrElement;
    } else {
      throw new InvalidArgumentException("The provided argument is neither a By nor a WebElement");
    }
    try {
      WebDriverWait wait = new WebDriverWait(driver, MAX_TIMEOUT);
      wait.until(ExpectedConditions.visibilityOf(element));
      wait.until(ExpectedConditions.elementToBeClickable(element));

      element.click();
      Thread.sleep(SLEEP_MILLIS);
    } catch (ElementClickInterceptedException e) {
      Actions action = new Actions(driver);
      action.moveToElement(element).click().build().perform();
      Thread.sleep(SLEEP_MILLIS);
      LOG.error(e.getMessage(), e);
    }
  }

  @BeforeAll
  public static void startUp() {
    driver = WebDriverManager.getWebDriver(getGravitinoServerPort());
  }

  @AfterAll
  public static void tearDown() {
    driver.quit();
  }
}
