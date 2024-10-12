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

import java.util.List;
import java.util.Objects;
import java.util.Set;
import org.apache.gravitino.integration.test.web.ui.utils.BaseWebIT;
import org.openqa.selenium.By;
import org.openqa.selenium.JavascriptExecutor;
import org.openqa.selenium.Keys;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.support.FindBy;
import org.openqa.selenium.support.PageFactory;
import org.openqa.selenium.support.ui.ExpectedConditions;
import org.openqa.selenium.support.ui.WebDriverWait;

public class MetalakePage extends BaseWebIT {
  private static final String PAGE_TITLE = "Metalakes";

  @FindBy(
      xpath =
          "//div[contains(@class, 'MuiDataGrid-main')]//div[contains(@class, 'MuiDataGrid-virtualScroller')]//div[@role='rowgroup']")
  public WebElement dataViewer;

  @FindBy(xpath = "//div[@data-refer='metalake-table-grid']")
  public WebElement metalakeTableGrid;

  @FindBy(xpath = "//*[@data-refer='create-metalake-btn']")
  public WebElement createMetalakeBtn;

  @FindBy(xpath = "//*[@data-refer='metalake-name-field']")
  public WebElement metalakeNameField;

  @FindBy(xpath = "//*[@data-refer='metalake-comment-field']")
  public WebElement metalakeCommentField;

  @FindBy(xpath = "//*[@data-refer='query-metalake']//input")
  public WebElement queryMetalakeInput;

  @FindBy(xpath = "//*[@data-refer='submit-handle-metalake']")
  public WebElement submitHandleMetalakeBtn;

  @FindBy(xpath = "//*[@data-refer='cancel-handle-metalake']")
  public WebElement cancelHandleMetalakeBtn;

  @FindBy(xpath = "//button[@data-refer='confirm-delete']")
  public WebElement confirmDeleteBtn;

  @FindBy(xpath = "//div[@data-refer='details-drawer']")
  public WebElement detailsDrawer;

  @FindBy(xpath = "//h6[@data-refer='details-title']")
  public WebElement detailsTitle;

  @FindBy(xpath = "//button[@data-refer='close-details-btn']")
  public WebElement closeDetailsBtn;

  @FindBy(xpath = "//button[@data-refer='add-metalake-props']")
  public WebElement addMetalakePropsBtn;

  @FindBy(xpath = "//div[@data-refer='metalake-table-grid']//button[@aria-label='Go to next page']")
  public WebElement nextPageBtn;

  @FindBy(
      xpath = "//div[@data-refer='metalake-table-grid']//button[@aria-label='Go to previous page']")
  public WebElement prevPageBtn;

  @FindBy(xpath = "//a[@data-refer='metalake-name-link']")
  public WebElement metalakeNameLink;

  @FindBy(
      xpath =
          "//div[@data-refer='metalake-table-grid']//div[contains(@class, 'MuiDataGrid-overlay')]")
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
    try {
      Thread.sleep(ACTION_SLEEP_MILLIS);
      clearQueryInput();
      queryMetalakeInput.sendKeys(queryParams);
      Thread.sleep(ACTION_SLEEP_MILLIS);
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
    }
  }

  public void clearQueryInput() {
    queryMetalakeInput.sendKeys(
        Keys.chord(Keys.HOME, Keys.chord(Keys.SHIFT, Keys.END), Keys.DELETE));
    queryMetalakeInput.clear();
  }

  public void clickDeleteMetalakeBtn(String name) {
    try {
      String xpath = "//button[@data-refer='delete-metalake-" + name + "']";
      clickAndWait(By.xpath(xpath));
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
    }
  }

  public void clickViewMetalakeBtn(String name) {
    try {
      String xpath = "//button[@data-refer='view-metalake-" + name + "']";
      clickAndWait(By.xpath(xpath));
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
    }
  }

  public void clickEditMetalakeBtn(String name) {
    try {
      String xpath = "//button[@data-refer='edit-metalake-" + name + "']";
      clickAndWait(By.xpath(xpath));
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
    }
  }

  public void clickMetalakeLink(String name) {
    try {
      setQueryParams(name);
      Thread.sleep(ACTION_SLEEP_MILLIS);
      String xpath = "//a[@data-refer='metalake-link-" + name + "']";
      WebElement metalakeLink = metalakeTableGrid.findElement(By.xpath(xpath));
      clickAndWait(metalakeLink);
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
    }
  }

  public void setMetalakeProps(int index, String key, String value) {
    // Set the indexed props key
    String keyPath = "//div[@data-refer='add-props-key-" + index + "']//input[@name='key']";
    WebElement keyInput = driver.findElement(By.xpath(keyPath));
    keyInput.sendKeys(key);

    // Set the indexed props value
    String valuePath = "//div[@data-refer='add-props-value-" + index + "']//input[@name='value']";
    WebElement valueInput = driver.findElement(By.xpath(valuePath));
    valueInput.sendKeys(value);
  }

  public boolean checkIsErrorName() throws InterruptedException {
    try {
      List<WebElement> errorText =
          metalakeNameField.findElements(By.xpath("//div[contains(@class, 'Mui-error')]"));

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
      // Get the created metalake row element from DataGrid by name.
      String rowPath = "//div[@data-id='" + name + "']";
      WebElement createdMetalakeRow = driver.findElement(By.xpath(rowPath));
      boolean isRow = createdMetalakeRow.isDisplayed();

      // Get and verify the created metalake link from DataGrid row item by name.
      String linkPath = "//div[@data-field='name']//a[@href='/ui/metalakes?metalake=" + name + "']";
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
      String xpath = "//div[@data-field='name']//a[@href='/ui/metalakes?metalake=" + name + "']";
      WebElement editedMetalakeLink = driver.findElement(By.xpath(xpath));

      // Check if the link text is match with name
      return Objects.equals(editedMetalakeLink.getText(), name);
    } catch (Exception e) {
      return false;
    }
  }

  public boolean verifyShowMetalakeDetails(String name) throws InterruptedException {
    try {
      // Check the drawer css property value
      detailsDrawer.isDisplayed();
      String drawerVisible = detailsDrawer.getCssValue("visibility");
      boolean isVisible = Objects.equals(drawerVisible, "visible");

      // Check the created metalake name
      String drawerTitle = detailsTitle.getText();
      boolean isText = Objects.equals(drawerTitle, name);

      return isVisible && isText;
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
      return false;
    } finally {
      clickAndWait(closeDetailsBtn);
    }
  }

  public boolean verifyEmptyMetalake() {
    try {
      boolean isNoRows = waitShowText("No rows", metalakeTableWrapper);

      if (!isNoRows) {
        LOG.error(metalakeTableWrapper.getText(), metalakeTableWrapper);
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
      clickAndWait(nextPageBtn);
      // Check if the previous page button is available
      return prevPageBtn.isEnabled() && performPrevPageAction();
    } catch (Exception e) {
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
      List<WebElement> dataList = dataViewer.findElements(By.xpath(".//div[@data-field='name']"));
      // Check if the text in the first row matches the search input
      boolean isQueried = Objects.equals(dataList.get(0).getText(), name);

      if (isQueried) {
        clearQueryInput();

        return true;
      } else {
        return false;
      }
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
      return false;
    }
  }

  public boolean verifyLinkToCatalogsPage(String name) throws InterruptedException {
    try {
      WebDriverWait wait = new WebDriverWait(driver, MAX_TIMEOUT);
      wait.until(ExpectedConditions.visibilityOf(metalakeNameLink));
      wait.until(ExpectedConditions.urlContains(metalakeNameLink.getAttribute("href")));

      if (!driver.getCurrentUrl().contains(name)
          || !metalakeNameLink.getAttribute("href").contains(name)) {
        LOG.error("metalake name link is not match");
        return false;
      }

      return true;
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
      return false;
    } finally {
      clickAndWait(backHomeBtn);
    }
  }

  public boolean verifyRefreshPage() {
    try {
      WebDriverWait wait = new WebDriverWait(driver, MAX_TIMEOUT);
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
      List<WebElement> dataList = dataViewer.findElements(By.xpath(".//div[@data-field='name']"));
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
