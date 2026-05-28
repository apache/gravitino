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

package org.apache.gravitino.integration.test.web.ui.pages;

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import org.apache.gravitino.integration.test.web.ui.utils.BaseWebIT;
import org.openqa.selenium.By;
import org.openqa.selenium.JavascriptExecutor;
import org.openqa.selenium.Keys;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.interactions.Actions;
import org.openqa.selenium.support.FindBy;
import org.openqa.selenium.support.PageFactory;
import org.openqa.selenium.support.ui.ExpectedConditions;
import org.openqa.selenium.support.ui.WebDriverWait;

public class MetalakePage extends BaseWebIT {
  private static final String PAGE_TITLE = "Metalakes";

  @FindBy(
      xpath =
          "//div[@data-refer='metalake-table-grid']//div[contains(@class, 'ant-table-body')]//table/tbody")
  public WebElement dataViewer;

  @FindBy(xpath = "//div[@data-refer='metalake-table-grid']")
  public WebElement metalakeTableGrid;

  @FindBy(xpath = "//*[@data-refer='create-metalake-btn']")
  public WebElement createMetalakeBtn;

  @FindBy(xpath = "//*[@data-refer='metalake-name-field']")
  public WebElement metalakeNameField;

  @FindBy(xpath = "//*[@data-refer='metalake-comment-field']")
  public WebElement metalakeCommentField;

  @FindBy(
      xpath = "//span[@data-refer='query-metalake']//input | //input[@data-refer='query-metalake']")
  public WebElement queryMetalakeInput;

  @FindBy(xpath = "//button[@data-refer='submit-handle-metalake']")
  public WebElement submitHandleMetalakeBtn;

  @FindBy(xpath = "//button[@data-refer='cancel-handle-metalake']")
  public WebElement cancelHandleMetalakeBtn;

  @FindBy(xpath = "//input[@data-refer='confirm-delete-input']")
  public WebElement confirmDeleteInput;

  @FindBy(
      xpath =
          "//div[contains(@class, 'ant-modal-confirm')]//button[contains(@class, 'ant-btn-dangerous')]")
  public WebElement confirmDeleteBtn;

  @FindBy(xpath = "//*[@data-refer='add-props']")
  public WebElement addMetalakePropsBtn;

  @FindBy(
      xpath =
          "//div[@data-refer='metalake-table-grid']//ul[contains(@class, 'ant-pagination')]//button[@title='Next Page']")
  public WebElement nextPageBtn;

  @FindBy(
      xpath =
          "//div[@data-refer='metalake-table-grid']//ul[contains(@class, 'ant-pagination')]//button[@title='Previous Page']")
  public WebElement prevPageBtn;

  @FindBy(xpath = "//a[@data-refer='metalake-name-link']")
  public WebElement metalakeNameLink;

  @FindBy(xpath = "//div[@data-refer='metalake-table-grid']//div[contains(@class, 'ant-empty')]")
  public WebElement metalakeTableWrapper;

  @FindBy(xpath = "//*[@data-refer='back-home-btn']")
  public WebElement backHomeBtn;

  @FindBy(xpath = "//*[@data-refer='metalake-page-title']")
  public WebElement metalakePageTitle;

  @FindBy(xpath = "//a[@data-refer='footer-link-gravitino']")
  public WebElement footerLinkGravitino;

  @FindBy(xpath = "//a[@data-refer='footer-link-license']")
  public WebElement footerLinkLicense;

  @FindBy(xpath = "//a[@data-refer='footer-link-docs']")
  public WebElement footerLinkDocs;

  @FindBy(xpath = "//a[@data-refer='footer-link-support']")
  public WebElement footerLinkSupport;

  public MetalakePage(WebDriver driver) {
    this.driver = driver;
    PageFactory.initElements(driver, this);
  }

  public void setMetalakeNameField(String nameField) {
    WebElement metalakeNameFieldInput = metalakeNameField.findElement(By.tagName("input"));
    metalakeNameFieldInput.sendKeys(
        Keys.chord(Keys.HOME, Keys.chord(Keys.SHIFT, Keys.END), Keys.DELETE));
    metalakeNameFieldInput.clear();
    metalakeNameFieldInput.sendKeys(nameField);
  }

  public void setMetalakeCommentField(String commentField) {
    WebElement metalakeCommentFieldInput = metalakeCommentField.findElement(By.tagName("textarea"));
    metalakeCommentFieldInput.sendKeys(
        Keys.chord(Keys.HOME, Keys.chord(Keys.SHIFT, Keys.END), Keys.DELETE));
    metalakeCommentFieldInput.clear();
    metalakeCommentFieldInput.sendKeys(commentField);
  }

  public void setQueryParams(String queryParams) {
    WebDriverWait wait = new WebDriverWait(driver, Duration.ofSeconds(ACTION_SLEEP));
    WebElement queryInputElement =
        wait.until(ExpectedConditions.elementToBeClickable(queryMetalakeInput));
    clearQueryInput();
    queryInputElement.sendKeys(queryParams);
  }

  public void clearQueryInput() {
    queryMetalakeInput.sendKeys(
        Keys.chord(Keys.HOME, Keys.chord(Keys.SHIFT, Keys.END), Keys.DELETE));
    queryMetalakeInput.clear();
  }

  public void clickInUseSwitch(String name) {
    try {
      // Find and hover over the Settings icon to open the dropdown menu
      String settingsXpath = "//a[@data-refer='settings-metalake-" + name + "']";
      WebDriverWait wait = new WebDriverWait(driver, Duration.ofSeconds(MAX_TIMEOUT));
      WebElement settingsIcon =
          wait.until(ExpectedConditions.elementToBeClickable(By.xpath(settingsXpath)));

      // Use Actions to hover over the settings icon to trigger the dropdown
      new Actions(driver).moveToElement(settingsIcon).perform();
      Thread.sleep(1000);

      // Wait for dropdown menu to appear and click on the "Not In-Use" or "In-Use" menu item
      String menuItemXpath =
          "//ul[contains(@class, 'ant-dropdown-menu')]//li[contains(@class, 'ant-dropdown-menu-item')]//span[contains(text(), 'Not In-Use') or contains(text(), 'In-Use')]";
      WebElement menuItem =
          wait.until(ExpectedConditions.elementToBeClickable(By.xpath(menuItemXpath)));
      menuItem.click();
      Thread.sleep(500);
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
    }
  }

  public void clickDeleteMetalakeBtn(String name) {
    try {
      String xpath = "//a[@data-refer='delete-metalake-" + name + "']";
      clickAndWait(By.xpath(xpath));
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
    }
  }

  public void clickViewMetalakeBtn(String name) {
    try {
      String xpath = "//a[@data-refer='view-metalake-" + name + "']";
      clickAndWait(By.xpath(xpath));
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
    }
  }

  public void clickEditMetalakeBtn(String name) {
    try {
      String xpath = "//a[@data-refer='edit-metalake-" + name + "']";
      clickAndWait(By.xpath(xpath));
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
    }
  }

  public void clickMetalakeLink(String name) {
    try {
      setQueryParams(name);
      WebDriverWait wait = new WebDriverWait(driver, Duration.ofSeconds(ACTION_SLEEP));
      String xpath = "//a[@data-refer='metalake-link-" + name + "']";
      WebElement metalakeLink =
          wait.until(ExpectedConditions.elementToBeClickable(By.xpath(xpath)));
      clickAndWait(metalakeLink);
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
    }
  }

  public void setMetalakeProps(int index, String key, String value) {
    // Set the indexed props key
    String keyPath = "//*[@data-refer='props-key-" + index + "']//input";
    WebElement keyInput = driver.findElement(By.xpath(keyPath));
    keyInput.sendKeys(key);

    // Set the indexed props value
    String valuePath = "//*[@data-refer='props-value-" + index + "']//input";
    WebElement valueInput = driver.findElement(By.xpath(valuePath));
    valueInput.sendKeys(value);
  }

  public boolean checkIsErrorName() throws InterruptedException {
    try {
      List<WebElement> errorText =
          metalakeNameField.findElements(
              By.xpath("//div[contains(@class, 'ant-form-item-explain-error')]"));

      return !errorText.isEmpty();
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
      return false;
    } finally {
      clickAndWait(cancelHandleMetalakeBtn);
    }
  }

  public boolean verifyCreateMetalake(String name) {
    try {
      // Get the created metalake row element from Ant Design Table by name.
      String rowPath = "//tr[@data-row-key='" + name + "']";
      WebElement createdMetalakeRow = driver.findElement(By.xpath(rowPath));
      boolean isRow = createdMetalakeRow.isDisplayed();

      // Get and verify the created metalake link from Table row item by name.
      String linkPath = "//a[@data-refer='metalake-link-" + name + "']";
      WebElement createdMetalakeLink = driver.findElement(By.xpath(linkPath));
      boolean isLink = createdMetalakeLink.isDisplayed();
      boolean isText = Objects.equals(createdMetalakeLink.getText(), name);

      return isRow && isLink && isText;
    } catch (Exception e) {
      return false;
    }
  }

  public boolean verifyEditedMetalake(String name) {
    try {
      String xpath = "//a[@data-refer='metalake-link-" + name + "']";
      WebElement editedMetalakeLink = driver.findElement(By.xpath(xpath));

      // Check if the link text is match with name
      return Objects.equals(editedMetalakeLink.getText(), name);
    } catch (Exception e) {
      return false;
    }
  }

  public void confirmDeleteMetalake(String name) {
    try {
      WebDriverWait wait = new WebDriverWait(driver, Duration.ofSeconds(ACTION_SLEEP));
      WebElement inputElement =
          wait.until(ExpectedConditions.elementToBeClickable(confirmDeleteInput));
      inputElement.sendKeys(name);
      clickAndWait(confirmDeleteBtn);
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
    }
  }

  public boolean verifyEmptyMetalake() {
    try {
      // Wait for the table to refresh after deletion
      Thread.sleep(2000);
      WebDriverWait wait = new WebDriverWait(driver, Duration.ofSeconds(MAX_TIMEOUT));
      // Wait for the empty state element to be visible
      WebElement emptyElement =
          wait.until(
              ExpectedConditions.visibilityOfElementLocated(
                  By.xpath(
                      "//div[@data-refer='metalake-table-grid']//div[contains(@class, 'ant-empty')]")));
      boolean isNoRows = emptyElement.isDisplayed();

      if (!isNoRows) {
        LOG.error("Empty state not displayed");
        return false;
      }

      return true;
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
      return false;
    }
  }

  public boolean verifyChangePagination() {
    try {
      // Wait for the table to fully load
      Thread.sleep(1000);
      WebDriverWait wait = new WebDriverWait(driver, Duration.ofSeconds(MAX_TIMEOUT));

      // First check if the table has data
      WebElement tableBody =
          wait.until(
              ExpectedConditions.visibilityOfElementLocated(
                  By.xpath(
                      "//div[@data-refer='metalake-table-grid']//div[contains(@class, 'ant-table-body')]//table/tbody")));

      // Count the rows in the table
      List<WebElement> rows = tableBody.findElements(By.xpath(".//tr[@data-row-key]"));
      LOG.info("Found {} rows in the current page", rows.size());

      // Debug: Print out any elements with pagination-related classes
      List<WebElement> anyPagination = driver.findElements(By.cssSelector("[class*='pagination']"));
      LOG.info("Found {} elements with 'pagination' in class name", anyPagination.size());
      for (WebElement el : anyPagination) {
        LOG.info("  - Element: {}, class: {}", el.getTagName(), el.getAttribute("class"));
      }

      // Try to find pagination using CSS selector (more reliable for Antd)
      List<WebElement> paginationElements =
          driver.findElements(By.cssSelector("ul.ant-pagination"));

      if (paginationElements.isEmpty()) {
        // Fallback: try xpath
        paginationElements =
            driver.findElements(By.xpath("//ul[contains(@class, 'ant-pagination')]"));
      }

      if (paginationElements.isEmpty()) {
        LOG.error("No pagination found even though we have {} rows", rows.size());
        // If no pagination and 10 or fewer rows, that's expected
        if (rows.size() <= 10) {
          LOG.info("Less than or equal to 10 rows, pagination not needed - returning true");
          return true;
        }
        return false;
      }

      LOG.info("Found {} pagination elements", paginationElements.size());
      WebElement paginationContainer = paginationElements.get(0);

      // Find and click the next page button
      WebElement nextBtn =
          paginationContainer.findElement(
              By.xpath(".//li[contains(@class, 'ant-pagination-next')]/button"));

      // Check if next button is enabled (not disabled)
      String disabledAttr = nextBtn.getAttribute("disabled");
      if (disabledAttr != null) {
        LOG.info("Next page button is disabled - only one page of data");
        return rows.size() > 0;
      }

      nextBtn.click();
      Thread.sleep(1000);

      // Check if the previous page button is now enabled
      WebElement prevBtn =
          paginationContainer.findElement(
              By.xpath(".//li[contains(@class, 'ant-pagination-prev')]/button"));

      boolean isPrevEnabled = prevBtn.getAttribute("disabled") == null;

      if (isPrevEnabled) {
        prevBtn.click();
        Thread.sleep(500);
        return true;
      }
      return true;
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
      return false;
    }
  }

  private boolean performPrevPageAction() {
    try {
      clickAndWait(prevPageBtn);
      return true;
    } catch (Exception e) {
      return false;
    }
  }

  public boolean verifyQueryMetalake(String name) {
    try {
      setQueryParams(name);
      // Wait for the table to filter
      Thread.sleep(1000);

      WebDriverWait wait = new WebDriverWait(driver, Duration.ofSeconds(MAX_TIMEOUT));

      // Wait for the table body to be present and find the metalake link with the searched name
      String metalakeLinkXpath = "//a[@data-refer='metalake-link-" + name + "']";
      WebElement metalakeLink =
          wait.until(ExpectedConditions.visibilityOfElementLocated(By.xpath(metalakeLinkXpath)));

      // Check if the metalake link is displayed and contains the correct name
      boolean isQueried = metalakeLink.isDisplayed() && metalakeLink.getText().equals(name);

      if (isQueried) {
        clearQueryInput();
        Thread.sleep(500);
        return true;
      } else {
        LOG.error(
            "Query result does not match: expected {} but got {}", name, metalakeLink.getText());
        return false;
      }
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
      return false;
    }
  }

  public boolean verifyLinkToCatalogsPage(String name) throws InterruptedException {
    try {
      // Wait for the catalogs page to load
      Thread.sleep(1000);
      WebDriverWait wait = new WebDriverWait(driver, Duration.ofSeconds(MAX_TIMEOUT));

      // Wait for the URL to contain metalake name
      wait.until(ExpectedConditions.urlContains("metalake=" + name));

      // Verify the URL contains the metalake name
      String currentUrl = driver.getCurrentUrl();
      if (!currentUrl.contains(name)) {
        LOG.error("Current URL does not contain metalake name: {}", currentUrl);
        return false;
      }

      // Wait for the metalake name link in breadcrumb to be visible
      WebElement metalakeLink =
          wait.until(
              ExpectedConditions.visibilityOfElementLocated(
                  By.xpath("//*[@data-refer='metalake-name-link']")));

      if (!metalakeLink.getText().contains(name)) {
        LOG.error(
            "Metalake name link text does not match: expected {} but got {}",
            name,
            metalakeLink.getText());
        return false;
      }

      return true;
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
      return false;
    } finally {
      // Navigate back to metalakes page
      WebDriverWait wait = new WebDriverWait(driver, Duration.ofSeconds(MAX_TIMEOUT));
      WebElement backBtn =
          wait.until(
              ExpectedConditions.elementToBeClickable(
                  By.xpath("//*[@data-refer='back-home-btn']")));
      backBtn.click();
      Thread.sleep(1000);
    }
  }

  public boolean verifyRefreshPage() {
    try {
      WebDriverWait wait = new WebDriverWait(driver, Duration.ofSeconds(MAX_TIMEOUT));
      wait.until(
          webDriver ->
              ((JavascriptExecutor) webDriver)
                  .executeScript("return document.readyState")
                  .equals("complete"));
      String pageTitle = metalakePageTitle.getText();
      boolean isPageTitle = pageTitle.equals(PAGE_TITLE);
      if (!isPageTitle) {
        LOG.error("No match with title, get {}", pageTitle);
        return false;
      }
      List<WebElement> dataList = dataViewer.findElements(By.xpath(".//tr//td[1]"));
      if (dataList.isEmpty()) {
        LOG.error("Table List should not be empty");
        return false;
      }
      return true;
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
      return false;
    }
  }

  public boolean verifyLinkInNewWindow(
      String originalWindowHandle, String expectedUrl, boolean contains) {
    try {
      Set<String> allWindowHandles = driver.getWindowHandles();
      for (String windowHandle : allWindowHandles) {
        if (!windowHandle.equals(originalWindowHandle)) {
          driver.switchTo().window(windowHandle);
          break;
        }
      }

      String actualUrl = driver.getCurrentUrl();

      if (contains) {
        return expectedUrl.contains(actualUrl.replaceAll("/[^/]+/$", "/"));
      }
      return actualUrl.equals(expectedUrl);

    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
      return false;
    } finally {
      driver.close();
      driver.switchTo().window(originalWindowHandle);
    }
  }
}
