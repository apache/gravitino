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
import org.junit.jupiter.api.BeforeEach;
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
  protected static final long EACH_TEST_SLEEP_MILLIS = 1_000;
  protected static final long ACTION_SLEEP_MILLIS = 1_000;

  protected boolean waitShowText(final String text, final Object locator) {
    try {
      return text.equals(locatorElement(locator).getText());
    } catch (TimeoutException e) {
      return false;
    }
  }

  protected void mouseMoveTo(final Object locator) {
    Actions action = new Actions(driver);
    action.moveToElement(locatorElement(locator)).perform();
  }

  protected WebElement pollingWait(final By locator, final long maxTimeout) {
    Wait<WebDriver> wait =
        new FluentWait<>(driver)
            .withTimeout(Duration.of(maxTimeout, ChronoUnit.SECONDS))
            .pollingEvery(Duration.of(1, ChronoUnit.SECONDS))
            .ignoring(NoSuchElementException.class);

    return wait.until((Function<WebDriver, WebElement>) driver -> driver.findElement(locator));
  }

  protected void clickAndWait(final Object locator) throws InterruptedException {
    try {
      // wait the element is available
      WebDriverWait wait = new WebDriverWait(driver, MAX_TIMEOUT);
      wait.until(ExpectedConditions.visibilityOf(locatorElement(locator)));
      wait.until(ExpectedConditions.elementToBeClickable(locatorElement(locator)));

      locatorElement(locator).click();
      Thread.sleep(ACTION_SLEEP_MILLIS);
    } catch (ElementClickInterceptedException e) {
      // if the previous click did not effected then try clicking in another way
      Actions action = new Actions(driver);
      action.moveToElement(locatorElement(locator)).click().build().perform();
      Thread.sleep(ACTION_SLEEP_MILLIS);
    }
  }

  WebElement locatorElement(final Object locatorOrElement) {
    WebElement element;
    if (locatorOrElement instanceof By) {
      element = pollingWait((By) locatorOrElement, MAX_IMPLICIT_WAIT);
    } else if (locatorOrElement instanceof WebElement) {
      element = (WebElement) locatorOrElement;
    } else {
      throw new InvalidArgumentException("The provided argument is neither a By nor a WebElement");
    }
    return element;
  }

  @BeforeEach
  public void beforeEachTest() {
    try {
      Thread.sleep(EACH_TEST_SLEEP_MILLIS);
    } catch (Exception e) {
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
