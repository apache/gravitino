/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.integration.test.web.ui.utils;

import com.google.common.base.Function;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import org.apache.gravitino.integration.test.util.BaseIT;
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
public class BaseWebIT extends BaseIT {
  protected static final Logger LOG = LoggerFactory.getLogger(BaseWebIT.class);
  protected WebDriver driver;

  // https://www.selenium.dev/documentation/webdriver/waits/#implicit-waits
  protected static final long MAX_IMPLICIT_WAIT = 30;
  protected static final long MAX_TIMEOUT = 60;
  protected static final long EACH_TEST_SLEEP = 1;
  protected static final long ACTION_SLEEP = 1;

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
      Thread.sleep(ACTION_SLEEP * 1000);
    } catch (ElementClickInterceptedException e) {
      // if the previous click did not effected then try clicking in another way
      Actions action = new Actions(driver);
      action.moveToElement(locatorElement(locator)).click().build().perform();
      Thread.sleep(ACTION_SLEEP * 1000);
    }
  }

  protected void reloadPageAndWait() {
    int maxRetries = 5;
    int retryCount = 0;
    boolean success = false;

    while (retryCount < maxRetries && !success) {
      try {
        driver.navigate().refresh();

        // Add a small delay to let the page start reloading
        try {
          Thread.sleep(500);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }

        // Use FluentWait with multiple conditions for more robust waiting
        Wait<WebDriver> wait =
            new FluentWait<>(driver)
                .withTimeout(Duration.of(MAX_TIMEOUT, ChronoUnit.SECONDS))
                .pollingEvery(Duration.of(500, ChronoUnit.MILLIS))
                .ignoring(NoSuchElementException.class)
                .ignoring(org.openqa.selenium.JavascriptException.class);

        // Wait for document ready state
        wait.until(
            d -> {
              try {
                Object readyState = js.executeScript("return document.readyState");
                return "complete".equals(readyState);
              } catch (Exception e) {
                LOG.warn("Error checking document.readyState: {}", e.getMessage());
                return false;
              }
            });

        // Additional wait to ensure page is fully interactive
        wait.until(
            d -> {
              try {
                // Check if body element is present and has content
                Object bodyExists =
                    js.executeScript(
                        "return document.body != null && document.body.innerHTML.length > 0");
                return Boolean.TRUE.equals(bodyExists);
              } catch (Exception e) {
                LOG.warn("Error checking body element: {}", e.getMessage());
                return false;
              }
            });

        // Extra stability wait for JavaScript to finish executing
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }

        success = true;
        if (retryCount > 0) {
          LOG.info("Page reload succeeded after {} retries", retryCount);
        }

      } catch (TimeoutException e) {
        retryCount++;
        LOG.warn(
            "Page reload timed out (attempt {}/{}). {}",
            retryCount,
            maxRetries,
            retryCount < maxRetries ? "Retrying..." : "Max retries reached");

        if (retryCount >= maxRetries) {
          throw new TimeoutException(
              "Failed to reload page after " + maxRetries + " attempts: " + e.getMessage(), e);
        }

        // Wait before retrying
        try {
          Thread.sleep(2000);
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
        }
      }
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
      Thread.sleep(EACH_TEST_SLEEP * 1000);
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
    }
  }

  @BeforeAll
  public void startUp() {
    driver = WebDriverManager.getWebDriver(getGravitinoServerPort());
  }

  @AfterAll
  public void tearDown() {
    driver.quit();
  }
}
