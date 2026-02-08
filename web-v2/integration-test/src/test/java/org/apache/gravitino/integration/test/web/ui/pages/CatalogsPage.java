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

import static org.apache.gravitino.integration.test.web.ui.CatalogsPageTest.DISTRIBUTION;
import static org.apache.gravitino.integration.test.web.ui.CatalogsPageTest.SORT_ORDERS;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import org.apache.gravitino.integration.test.web.ui.utils.BaseWebIT;
import org.openqa.selenium.By;
import org.openqa.selenium.JavascriptExecutor;
import org.openqa.selenium.Keys;
import org.openqa.selenium.StaleElementReferenceException;
import org.openqa.selenium.TimeoutException;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.interactions.Actions;
import org.openqa.selenium.support.FindBy;
import org.openqa.selenium.support.PageFactory;
import org.openqa.selenium.support.ui.ExpectedConditions;
import org.openqa.selenium.support.ui.WebDriverWait;

public class CatalogsPage extends BaseWebIT {
  @FindBy(xpath = "//*[@data-refer='back-home-btn']")
  public WebElement backHomeBtn;

  @FindBy(xpath = "//*[@data-refer='select-metalake']")
  public WebElement metalakeSelect;

  @FindBy(xpath = "//div[@data-refer='table-grid']")
  public WebElement tableGrid;

  @FindBy(xpath = "//*[@data-refer='create-catalog-btn']")
  public WebElement createCatalogBtn;

  @FindBy(xpath = "//*[@data-refer='catalog-name-field']")
  public WebElement catalogNameField;

  @FindBy(xpath = "//*[@data-refer='catalog-comment-field']")
  public WebElement catalogCommentField;

  @FindBy(xpath = "//button[@data-refer='add-props']")
  public WebElement addCatalogPropsBtn;

  @FindBy(xpath = "//*[@data-refer='handle-submit-catalog']")
  public WebElement handleSubmitCatalogBtn;

  @FindBy(xpath = "//*[@data-refer='handle-next-catalog']")
  public WebElement handleNextCatalogBtn;

  @FindBy(xpath = "//*[@data-refer='create-schema-btn']")
  public WebElement createSchemaBtn;

  @FindBy(xpath = "//*[@data-refer='schema-name-field']")
  public WebElement schemaNameField;

  @FindBy(xpath = "//*[@data-refer='schema-comment-field']")
  public WebElement schemaCommentField;

  @FindBy(xpath = "//*[@data-refer='handle-submit-schema']")
  public WebElement handleSubmitSchemaBtn;

  @FindBy(xpath = "//*[@data-refer='create-fileset-btn']")
  public WebElement createFilesetBtn;

  @FindBy(xpath = "//*[@data-refer='fileset-name-field']")
  public WebElement filesetNameField;

  @FindBy(xpath = "//*[@data-refer='fileset-storageLocation-field']")
  public WebElement filesetStorageLocationField;

  @FindBy(xpath = "//*[@data-refer='fileset-comment-field']")
  public WebElement filesetCommentField;

  @FindBy(xpath = "//button[@data-refer='add-props']")
  public WebElement addFilesetPropsBtn;

  @FindBy(xpath = "//*[@data-refer='handle-submit-fileset']")
  public WebElement handleSubmitFilesetBtn;

  @FindBy(xpath = "//*[@data-refer='create-topic-btn']")
  public WebElement createTopicBtn;

  @FindBy(xpath = "//*[@data-refer='topic-name-field']")
  public WebElement topicNameField;

  @FindBy(xpath = "//*[@data-refer='topic-comment-field']")
  public WebElement topicCommentField;

  @FindBy(xpath = "//*[@data-refer='handle-submit-topic']")
  public WebElement handleSubmitTopicBtn;

  @FindBy(xpath = "//*[@data-refer='create-table-btn']")
  public WebElement createTableBtn;

  @FindBy(xpath = "//*[@data-refer='table-name-field']")
  public WebElement tableNameField;

  @FindBy(xpath = "//*[@data-refer='table-comment-field']")
  public WebElement tableCommentField;

  @FindBy(xpath = "//*[@data-refer='handle-submit-table']")
  public WebElement handleSubmitTableBtn;

  @FindBy(xpath = "//div[@data-refer='tree-view']")
  public WebElement treeView;

  @FindBy(xpath = "//div[@data-refer='table-grid']")
  public WebElement tableWrapper;

  @FindBy(xpath = "//div[@role='tab' and contains(text(), 'Table')]")
  public WebElement tabTableBtn;

  @FindBy(xpath = "//div[@data-refer='tab-table-panel']")
  public WebElement tabTableContent;

  @FindBy(xpath = "//div[@role='tab' and contains(text(), 'Details')]")
  public WebElement tabDetailsBtn;

  @FindBy(xpath = "//div[@data-refer='tab-details-panel']")
  public WebElement tabDetailsContent;

  @FindBy(xpath = "//div[@role='tab' and contains(text(), 'Files')]")
  public WebElement tabFilesBtn;

  @FindBy(xpath = "//div[@data-refer='tab-files-panel']")
  public WebElement tabFilesContent;

  @FindBy(xpath = "//div[contains(@class, 'ant-drawer')]")
  public WebElement detailsDrawer;

  @FindBy(xpath = "//div[contains(@class, 'ant-drawer-title')]")
  public WebElement detailsTitle;

  @FindBy(xpath = "//button[contains(@class, 'ant-drawer-close')]")
  public WebElement closeDetailsBtn;

  @FindBy(xpath = "//button[contains(@class, 'ant-btn-dangerous')]")
  public WebElement confirmDeleteBtn;

  @FindBy(xpath = "//*[@data-refer='confirm-delete-input']")
  public WebElement confirmDeleteInput;

  @FindBy(xpath = "//*[@data-refer='metalake-name-link']")
  public WebElement metalakeNameLink;

  @FindBy(xpath = "//thead[contains(@class, 'ant-table-thead')]//tr")
  public WebElement columnHeaders;

  @FindBy(xpath = "//*[@data-refer='metalake-page-title']")
  public WebElement metalakePageTitle;

  @FindBy(xpath = "//*[@data-refer='details-props-table']")
  public WebElement detailsPropsTable;

  @FindBy(xpath = "//*[@data-refer='catalog-provider-selector']")
  public WebElement catalogProviderSelector;

  @FindBy(xpath = "//*[@data-refer='catalog-type-selector']")
  public WebElement catalogTypeSelector;

  @FindBy(xpath = "//div[contains(@class, 'ant-select-dropdown')]")
  public WebElement catalogProviderList;

  @FindBy(xpath = "//div[contains(@class, 'ant-select-dropdown')]")
  public WebElement catalogTypeList;

  public CatalogsPage(WebDriver driver) {
    this.driver = driver;
    PageFactory.initElements(driver, this);
  }

  public void metalakeSelectChange(String metalakeName) {
    try {
      clickAndWait(metalakeSelect);
      Thread.sleep(500); // Wait for dropdown to appear
      // Ant Design Dropdown menu item with data-refer attribute
      String keyPath = "//div[@data-refer='select-option-" + metalakeName + "']";
      WebDriverWait wait = new WebDriverWait(driver, Duration.ofSeconds(MAX_TIMEOUT));
      WebElement selectOption =
          wait.until(ExpectedConditions.elementToBeClickable(By.xpath(keyPath)));
      clickAndWait(selectOption);
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
    }
  }

  public void setCatalogNameField(String nameField) {
    try {
      WebElement catalogNameFieldInput = catalogNameField.findElement(By.tagName("input"));
      catalogNameFieldInput.sendKeys(
          Keys.chord(Keys.HOME, Keys.chord(Keys.SHIFT, Keys.END), Keys.DELETE));
      catalogNameFieldInput.clear();
      catalogNameFieldInput.sendKeys(nameField);
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
    }
  }

  public void setCatalogCommentField(String commentField) {
    try {
      WebElement catalogCommentFieldInput = catalogCommentField.findElement(By.tagName("textarea"));
      catalogCommentFieldInput.sendKeys(
          Keys.chord(Keys.HOME, Keys.chord(Keys.SHIFT, Keys.END), Keys.DELETE));
      catalogCommentFieldInput.clear();
      catalogCommentFieldInput.sendKeys(commentField);
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
    }
  }

  // set the required fixed catalog properties
  public void setCatalogFixedProp(String key, String value) {
    try {
      WebDriverWait wait = new WebDriverWait(driver, Duration.ofSeconds(MAX_TIMEOUT));
      // Try to find by data-refer attribute on input element first
      String xpath = "//input[@data-refer='catalog-props-" + key + "']";
      WebElement propItem =
          wait.until(ExpectedConditions.visibilityOfElementLocated(By.xpath(xpath)));
      propItem.sendKeys(value);
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
    }
  }

  // set the indexed catalog properties
  public void setPropsAt(int index, String key, String value) {
    try {
      WebDriverWait wait = new WebDriverWait(driver, Duration.ofSeconds(MAX_TIMEOUT));
      // Set the indexed props key - Ant Design Form.Item with data-refer/data-testid on wrapper
      String keyPath = "//*[@data-testid='props-key-" + index + "']//input";
      WebElement keyInput =
          wait.until(ExpectedConditions.visibilityOfElementLocated(By.xpath(keyPath)));
      keyInput.sendKeys(key);
      // Set the indexed props value - Ant Design Form.Item with data-refer/data-testid on wrapper
      String valuePath = "//*[@data-testid='props-value-" + index + "']//input";
      WebElement valueInput =
          wait.until(ExpectedConditions.visibilityOfElementLocated(By.xpath(valuePath)));
      valueInput.sendKeys(value);
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
    }
  }

  public void clickEditCatalogBtn(String name) {
    try {
      // UI uses <a> tag with data-refer attribute for edit button
      String xpath = "//a[@data-refer='edit-entity-" + name + "']";
      WebDriverWait wait = new WebDriverWait(driver, Duration.ofSeconds(MAX_TIMEOUT));
      WebElement btn = wait.until(ExpectedConditions.elementToBeClickable(By.xpath(xpath)));
      clickAndWait(btn);
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
    }
  }

  public void clickInUseSwitch(String name) {
    int maxRetries = 3;
    for (int retry = 0; retry < maxRetries; retry++) {
      try {
        // Wait for page to be ready
        Thread.sleep(1000);

        // Find and hover over the Settings icon to open the dropdown menu
        String settingsXpath = "//a[@data-refer='settings-catalog-" + name + "']";
        WebDriverWait wait = new WebDriverWait(driver, Duration.ofSeconds(MAX_TIMEOUT));

        // First ensure the element is present in DOM
        WebElement settingsIcon =
            wait.until(ExpectedConditions.presenceOfElementLocated(By.xpath(settingsXpath)));

        // Scroll element into view
        ((JavascriptExecutor) driver)
            .executeScript("arguments[0].scrollIntoView({block: 'center'});", settingsIcon);
        Thread.sleep(500);

        // Wait for element to be clickable
        wait.until(ExpectedConditions.elementToBeClickable(settingsIcon));

        // Use Actions to hover over the settings icon to trigger the dropdown
        Actions actions = new Actions(driver);
        actions.moveToElement(settingsIcon).perform();
        Thread.sleep(500);

        // Click on the settings icon to ensure dropdown opens
        settingsIcon.click();
        Thread.sleep(500);

        // Wait for dropdown menu to appear and click on the "Not In Use" menu item
        // Ant Design menu item structure: li.ant-dropdown-menu-item >
        // span.ant-dropdown-menu-title-content
        String menuItemXpath =
            "//li[contains(@class, 'ant-dropdown-menu-item')]//span[contains(@class, 'ant-dropdown-menu-title-content') and (contains(text(), 'Not In-Use') or contains(text(), 'In-Use'))]";

        // Use shorter timeout for menu item since we'll retry (10 seconds)
        WebDriverWait menuWait = new WebDriverWait(driver, Duration.ofSeconds(10));
        WebElement menuItem =
            menuWait.until(ExpectedConditions.elementToBeClickable(By.xpath(menuItemXpath)));
        menuItem.click();
        Thread.sleep(500);
        return; // Success, exit the method
      } catch (Exception e) {
        LOG.warn("Attempt {} failed to click in-use switch: {}", retry + 1, e.getMessage());
        // Close any open dropdown by clicking elsewhere
        try {
          driver.findElement(By.tagName("body")).click();
          Thread.sleep(500);
        } catch (Exception closeEx) {
          LOG.debug("Failed to close dropdown: {}", closeEx.getMessage());
        }
        if (retry == maxRetries - 1) {
          LOG.error("Failed to click in-use switch after {} attempts", maxRetries, e);
        }
      }
    }
  }

  public void clickDeleteBtn(String name) {
    try {
      // New UI uses <a> tag instead of <button> for delete action
      String xpath = "//a[@data-refer='delete-entity-" + name + "']";
      WebElement btn = driver.findElement(By.xpath(xpath));
      WebDriverWait wait = new WebDriverWait(driver, Duration.ofSeconds(MAX_TIMEOUT));
      wait.until(ExpectedConditions.elementToBeClickable(By.xpath(xpath)));
      clickAndWait(btn);
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
    }
  }

  // Input the name to confirm deletion in the modal dialog
  public void setConfirmDeleteInput(String name) {
    try {
      WebDriverWait wait = new WebDriverWait(driver, Duration.ofSeconds(MAX_TIMEOUT));
      wait.until(ExpectedConditions.visibilityOf(confirmDeleteInput));
      confirmDeleteInput.sendKeys(name);
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
    }
  }

  public void setSchemaNameField(String nameField) {
    try {
      WebElement schemaNameFieldInput = schemaNameField.findElement(By.tagName("input"));
      schemaNameFieldInput.sendKeys(
          Keys.chord(Keys.HOME, Keys.chord(Keys.SHIFT, Keys.END), Keys.DELETE));
      schemaNameFieldInput.clear();
      schemaNameFieldInput.sendKeys(nameField);
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
    }
  }

  public void setSchemaCommentField(String nameField) {
    try {
      WebElement schemaCommentFieldInput = schemaCommentField.findElement(By.tagName("textarea"));
      schemaCommentFieldInput.sendKeys(
          Keys.chord(Keys.HOME, Keys.chord(Keys.SHIFT, Keys.END), Keys.DELETE));
      schemaCommentFieldInput.clear();
      schemaCommentFieldInput.sendKeys(nameField);
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
    }
  }

  public void setFilesetNameField(String nameField) {
    try {
      filesetNameField.sendKeys(
          Keys.chord(Keys.HOME, Keys.chord(Keys.SHIFT, Keys.END), Keys.DELETE));
      filesetNameField.clear();
      filesetNameField.sendKeys(nameField);
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
    }
  }

  public void setFilesetStorageLocationField(
      int index, String locationName, String storageLocation) {
    try {
      // Set the indexed storageLocations name
      String namePath = "//*[@data-refer='storageLocations-name-" + index + "']";
      WebElement nameInput = driver.findElement(By.xpath(namePath));
      nameInput.sendKeys(locationName);

      // Parse the storage location to extract prefix and path
      String prefix = "file:/";
      String path = storageLocation;
      if (storageLocation.startsWith("hdfs://")) {
        prefix = "hdfs://";
        path = storageLocation.substring("hdfs://".length());
      } else if (storageLocation.startsWith("s3://")) {
        prefix = "s3://";
        path = storageLocation.substring("s3://".length());
      } else if (storageLocation.startsWith("file:/")) {
        prefix = "file:/";
        path = storageLocation.substring("file:/".length());
      }

      // Click the prefix selector - Ant Design addonBefore puts Select in ant-input-group-addon
      // Find the Input first by data-refer, then navigate to its sibling addon
      String locationInputPath = "//*[@data-refer='storageLocations-location-" + index + "']";
      WebElement locationInputEl = driver.findElement(By.xpath(locationInputPath));
      // The addon is a sibling span with class ant-input-group-addon before the input
      WebElement addonWrapper =
          locationInputEl.findElement(
              By.xpath(
                  "./ancestor::span[contains(@class, 'ant-input-wrapper') or contains(@class, 'ant-input-group')]//span[contains(@class, 'ant-select')]"));

      WebDriverWait wait = new WebDriverWait(driver, Duration.ofSeconds(MAX_TIMEOUT));
      wait.until(ExpectedConditions.elementToBeClickable(addonWrapper));
      addonWrapper.click();
      Thread.sleep(500);

      // Select the prefix option from dropdown
      String prefixOptionPath =
          "//div[contains(@class, 'ant-select-item-option') and (@title='"
              + prefix
              + "' or contains(., '"
              + prefix
              + "'))]";
      WebElement prefixOption =
          wait.until(ExpectedConditions.elementToBeClickable(By.xpath(prefixOptionPath)));
      prefixOption.click();
      Thread.sleep(300);

      // Set the indexed storageLocations location (path only, without prefix)
      WebElement locationInput = driver.findElement(By.xpath(locationInputPath));
      locationInput.sendKeys(path);
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
    }
  }

  public void setFilesetCommentField(String commentField) {
    try {
      filesetCommentField.sendKeys(
          Keys.chord(Keys.HOME, Keys.chord(Keys.SHIFT, Keys.END), Keys.DELETE));
      filesetCommentField.clear();
      filesetCommentField.sendKeys(commentField);
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
    }
  }

  public void setTopicNameField(String nameField) {
    try {
      topicNameField.sendKeys(Keys.chord(Keys.HOME, Keys.chord(Keys.SHIFT, Keys.END), Keys.DELETE));
      topicNameField.clear();
      topicNameField.sendKeys(nameField);
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
    }
  }

  public void setTopicCommentField(String commentField) {
    try {
      topicCommentField.sendKeys(
          Keys.chord(Keys.HOME, Keys.chord(Keys.SHIFT, Keys.END), Keys.DELETE));
      topicCommentField.clear();
      topicCommentField.sendKeys(commentField);
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
    }
  }

  public void setTableNameField(String nameField) {
    try {
      WebElement tableNameFieldInput = tableNameField;
      tableNameFieldInput.sendKeys(
          Keys.chord(Keys.HOME, Keys.chord(Keys.SHIFT, Keys.END), Keys.DELETE));
      tableNameFieldInput.clear();
      tableNameFieldInput.sendKeys(nameField);
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
    }
  }

  public void setTableCommentField(String commentField) {
    try {
      WebElement tableCommentFieldInput = tableCommentField;
      tableCommentFieldInput.sendKeys(
          Keys.chord(Keys.HOME, Keys.chord(Keys.SHIFT, Keys.END), Keys.DELETE));
      tableCommentFieldInput.clear();
      tableCommentFieldInput.sendKeys(commentField);
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
    }
  }

  // set the indexed table columns
  public void setTableColumnsAt(int index, String name, String type) {
    try {
      // Set the indexed column name
      String columnName = "//div[@data-refer='column-name-" + index + "']//input";
      WebElement keyInput = driver.findElement(By.xpath(columnName));
      keyInput.sendKeys(name);
      // Set the indexed column type - Ant Design Select structure
      String columnType = "//div[@data-refer='column-type-" + index + "']";
      WebElement typeSelect = driver.findElement(By.xpath(columnType));
      clickAndWait(typeSelect);
      // Wait for dropdown to appear and click the option
      // Ant Design Select dropdown - try multiple selectors for compatibility
      Thread.sleep(500);
      WebDriverWait wait = new WebDriverWait(driver, Duration.ofSeconds(MAX_TIMEOUT));
      String optionXpath =
          "//div[contains(@class, 'ant-select-dropdown')]"
              + "//div[contains(@class, 'ant-select-item-option') and (@title='"
              + type
              + "' or .//div[text()='"
              + capitalizeFirstLetter(type)
              + "'])]";
      WebElement typeItem =
          wait.until(ExpectedConditions.elementToBeClickable(By.xpath(optionXpath)));
      clickAndWait(typeItem);
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
    }
  }

  private String capitalizeFirstLetter(String str) {
    if (str == null || str.isEmpty()) {
      return str;
    }
    return str.substring(0, 1).toUpperCase() + str.substring(1);
  }

  public void clickMetalakeLink(String metalakeName) {
    try {
      String xpath = "//a[@href='?metalake=" + metalakeName + "']";
      WebElement link = tableGrid.findElement(By.xpath(xpath));
      WebDriverWait wait = new WebDriverWait(driver, Duration.ofSeconds(MAX_TIMEOUT));
      wait.until(ExpectedConditions.elementToBeClickable(By.xpath(xpath)));
      clickAndWait(link);
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
    }
  }

  public void clickCatalogLink(String metalakeName, String catalogName, String catalogType) {
    try {
      String linkXpath = "//a[@data-refer='catalog-link-" + catalogName + "']";
      String detailsTabsXpath = "//*[@data-refer='details-tabs']";
      WebDriverWait wait = new WebDriverWait(driver, Duration.ofSeconds(MAX_TIMEOUT));

      // Retry click up to 3 times to handle cases where SPA navigation doesn't trigger
      int maxRetries = 3;
      for (int attempt = 0; attempt < maxRetries; attempt++) {
        WebElement link = wait.until(ExpectedConditions.elementToBeClickable(By.xpath(linkXpath)));
        clickAndWait(link);

        // Wait for navigation to complete: details-tabs only exists on detail pages
        try {
          new WebDriverWait(driver, Duration.ofSeconds(10))
              .until(ExpectedConditions.presenceOfElementLocated(By.xpath(detailsTabsXpath)));
          LOG.info("Navigation to catalog details page completed on attempt {}", attempt + 1);
          return;
        } catch (TimeoutException e) {
          LOG.warn(
              "Catalog details page not loaded after click attempt {}, retrying...", attempt + 1);
        }
      }
      LOG.error("Failed to navigate to catalog details page after {} attempts", maxRetries);
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
    }
  }

  public void clickSchemaLink(
      String metalakeName, String catalogName, String catalogType, String schemaName) {
    try {
      String linkXpath = "//a[@data-refer='schema-link-" + schemaName + "']";
      WebDriverWait wait = new WebDriverWait(driver, Duration.ofSeconds(MAX_TIMEOUT));

      // Retry click up to 3 times to handle cases where SPA navigation doesn't trigger
      int maxRetries = 3;
      for (int attempt = 0; attempt < maxRetries; attempt++) {
        WebElement link = wait.until(ExpectedConditions.elementToBeClickable(By.xpath(linkXpath)));
        clickAndWait(link);

        // Wait for navigation: URL should contain schema= parameter
        try {
          new WebDriverWait(driver, Duration.ofSeconds(10))
              .until(d -> d.getCurrentUrl().contains("schema="));
          LOG.info("Navigation to schema details page completed on attempt {}", attempt + 1);
          return;
        } catch (TimeoutException e) {
          LOG.warn(
              "Schema details page not loaded after click attempt {}, retrying...", attempt + 1);
        }
      }
      LOG.error("Failed to navigate to schema details page after {} attempts", maxRetries);
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
    }
  }

  public void clickTableLink(
      String metalakeName,
      String catalogName,
      String catalogType,
      String schemaName,
      String tableName) {
    try {
      String linkXpath = "//a[@data-refer='table-link-" + tableName + "']";
      WebDriverWait wait = new WebDriverWait(driver, Duration.ofSeconds(MAX_TIMEOUT));

      // Retry click up to 3 times to handle cases where SPA navigation doesn't trigger
      int maxRetries = 3;
      for (int attempt = 0; attempt < maxRetries; attempt++) {
        WebElement link = wait.until(ExpectedConditions.elementToBeClickable(By.xpath(linkXpath)));
        clickAndWait(link);

        // Wait for navigation: URL should contain table= parameter
        try {
          new WebDriverWait(driver, Duration.ofSeconds(10))
              .until(d -> d.getCurrentUrl().contains("table="));
          LOG.info("Navigation to table details page completed on attempt {}", attempt + 1);
          return;
        } catch (TimeoutException e) {
          LOG.warn(
              "Table details page not loaded after click attempt {}, retrying...", attempt + 1);
        }
      }
      LOG.error("Failed to navigate to table details page after {} attempts", maxRetries);
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
    }
  }

  public void clickBreadCrumbsToCatalogs() {
    try {
      WebDriverWait wait = new WebDriverWait(driver, Duration.ofSeconds(MAX_TIMEOUT));
      wait.until(ExpectedConditions.elementToBeClickable(metalakeNameLink));
      clickAndWait(metalakeNameLink);
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
    }
  }

  public void clickSelectProvider(String provider) throws InterruptedException {
    // New UI uses provider cards instead of dropdown
    // Wait for the provider card to be visible before clicking
    WebDriverWait wait = new WebDriverWait(driver, Duration.ofSeconds(MAX_TIMEOUT));
    String providerCardPath = "//div[@data-refer='catalog-provider-" + provider + "']";
    WebElement providerCard =
        wait.until(ExpectedConditions.presenceOfElementLocated(By.xpath(providerCardPath)));
    // Scroll the element into view if needed
    ((JavascriptExecutor) driver)
        .executeScript("arguments[0].scrollIntoView({block: 'center'});", providerCard);
    Thread.sleep(500); // Wait for scroll to complete
    wait.until(ExpectedConditions.elementToBeClickable(providerCard));
    // Use JavaScript click to ensure the click event is properly triggered
    ((JavascriptExecutor) driver).executeScript("arguments[0].click();", providerCard);
    Thread.sleep(500); // Wait for selection to register

    // Verify that provider was selected by checking if Next button becomes enabled
    String nextBtnPath = "//*[@data-refer='handle-next-catalog']";
    wait.until(ExpectedConditions.elementToBeClickable(By.xpath(nextBtnPath)));
  }

  public void clickSelectType(String type) throws InterruptedException {
    // New UI uses Tabs for catalog type selection
    String tabPath = "//div[contains(@class, 'ant-tabs-tab') and @data-node-key='" + type + "']";
    WebElement tabItem = driver.findElement(By.xpath(tabPath));
    clickAndWait(tabItem);
  }

  public void clickTreeNode(String nodeKey) throws InterruptedException {
    // Parse nodeKey format: {{metalake}}{{catalog}}{{type}} or
    // {{metalake}}{{catalog}}{{type}}{{schema}} etc.
    // Extract the last meaningful part (the node name to click)
    String[] parts = nodeKey.replace("{{", "").split("}}");
    String nodeName = "";

    // The parts array will contain: [metalake, catalog, type, schema?, entity?]
    // We need to get the actual node name (catalog, schema, or entity name)
    if (parts.length >= 2) {
      // For catalog node: parts[1] is catalog name
      // For schema node: parts[3] is schema name
      // For table node: parts[4] is table name
      if (parts.length == 3) {
        // Catalog node: {{metalake}}{{catalog}}{{type}}
        nodeName = parts[1];
      } else if (parts.length == 4) {
        // Schema node: {{metalake}}{{catalog}}{{type}}{{schema}}
        nodeName = parts[3];
      } else if (parts.length >= 5) {
        // Table/Entity node: {{metalake}}{{catalog}}{{type}}{{schema}}{{entity}}
        nodeName = parts[4];
      }
    }

    // Use Ant Design Tree structure to find the node by its title.
    // Try multiple locators to handle UI variations and ensure node is visible/clickable.
    String[] candidateXpaths =
        new String[] {
          "//div[contains(@class, 'ant-tree-treenode')]//span[contains(@class, 'ant-tree-title')]//div[@title='"
              + nodeName
              + "']",
          "//div[contains(@class, 'ant-tree-treenode')]//span[contains(@class, 'ant-tree-title')]//div[text()='"
              + nodeName
              + "']",
          "//div[contains(@class, 'ant-tree-treenode')]//span[contains(@class, 'ant-tree-title')][.//text()[normalize-space()='"
              + nodeName
              + "']]",
          "//div[contains(@class,'ant-tree-treenode')]//span[contains(@class,'ant-tree-title') and normalize-space(.)='"
              + nodeName
              + "']",
        };

    WebDriverWait wait = new WebDriverWait(driver, Duration.ofSeconds(MAX_TIMEOUT));
    WebElement treeNode = null;
    for (String xp : candidateXpaths) {
      try {
        treeNode = wait.until(ExpectedConditions.presenceOfElementLocated(By.xpath(xp)));
        if (treeNode != null) {
          // Scroll into view and try to click
          ((JavascriptExecutor) driver)
              .executeScript("arguments[0].scrollIntoView({block: 'center'});", treeNode);
          try {
            wait.until(ExpectedConditions.elementToBeClickable(treeNode));
            clickAndWait(treeNode);
            return;
          } catch (Exception clickEx) {
            // fallback to JS click
            try {
              ((JavascriptExecutor) driver).executeScript("arguments[0].click();", treeNode);
              Thread.sleep(300);
              return;
            } catch (Exception jsEx) {
              LOG.debug(
                  "clickTreeNode: failed to click xpath {}: {}, {}",
                  xp,
                  clickEx.getMessage(),
                  jsEx.getMessage());
            }
          }
        }
      } catch (Exception e) {
        LOG.debug("clickTreeNode: xpath {} not found: {}", xp, e.getMessage());
      }
    }

    // If we reach here, we couldn't find/click the node; throw a descriptive error
    throw new InterruptedException(
        "clickTreeNode: could not locate or click tree node '" + nodeName + "'");
  }

  public void clickTreeNodeRefresh(String nodeKey) throws InterruptedException {
    // Parse nodeKey to get the node name
    String[] parts = nodeKey.replace("{{", "").split("}}");
    String nodeName = "";

    if (parts.length == 3) {
      nodeName = parts[1]; // Catalog name
    } else if (parts.length == 4) {
      nodeName = parts[3]; // Schema name
    } else if (parts.length >= 5) {
      nodeName = parts[4]; // Entity name
    }

    // Find the tree node by title and hover over its icon to trigger refresh
    String nodeXpath =
        "//div[contains(@class, 'ant-tree-treenode')]//span[contains(@class, 'ant-tree-title')]//div[@title='"
            + nodeName
            + "']/ancestor::span[contains(@class, 'ant-tree-title')]/preceding-sibling::span[contains(@class, 'ant-tree-iconEle')]";

    try {
      WebDriverWait wait = new WebDriverWait(driver, Duration.ofSeconds(MAX_TIMEOUT));
      WebElement iconElement =
          wait.until(ExpectedConditions.presenceOfElementLocated(By.xpath(nodeXpath)));

      // Hover over the icon to show refresh button
      Actions actions = new Actions(driver);
      actions.moveToElement(iconElement).perform();
      Thread.sleep(500);

      // Click the icon (which should now be the refresh icon)
      iconElement.click();
      Thread.sleep(1000);
    } catch (Exception e) {
      LOG.error("Failed to click tree node refresh: {}", e.getMessage(), e);
    }
  }

  public boolean verifyGetCatalog(String name) {
    try {
      // Ant Design Tree structure: ant-tree-treenode > ant-tree-title > div[@title='name']
      String xpath =
          "//div[contains(@class, 'ant-tree-treenode')]//span[contains(@class, 'ant-tree-title')]//div[@title='"
              + name
              + "']";
      WebDriverWait wait = new WebDriverWait(driver, Duration.ofSeconds(MAX_TIMEOUT));
      WebElement treeNode =
          wait.until(ExpectedConditions.visibilityOfElementLocated(By.xpath(xpath)));
      boolean match = Objects.equals(treeNode.getText(), name);
      if (!match) {
        LOG.error("tree node: {} does not match with name: {}", treeNode.getText(), name);
        return false;
      }
      return true;
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
      return false;
    }
  }

  public boolean verifyCatalogDetailsPage(String catalogName, String provider, String type) {
    try {
      WebDriverWait wait = new WebDriverWait(driver, Duration.ofSeconds(MAX_TIMEOUT));

      // Verify catalog name is displayed in the page title (h3.ant-typography > span[@title])
      String titleXpath =
          "//h3[contains(@class, 'ant-typography')]//span[@title='" + catalogName + "']";
      WebElement titleElement =
          wait.until(ExpectedConditions.visibilityOfElementLocated(By.xpath(titleXpath)));
      boolean hasTitle = titleElement.isDisplayed();

      // Verify type is displayed in the Space section (after Type icon)
      // The structure is: <Space><Icons.Type /><span>{type}</span></Space>
      String typeXpath = "//div[contains(@class, 'ant-space')]//span[text()='" + type + "']";
      WebElement typeElement =
          wait.until(ExpectedConditions.visibilityOfElementLocated(By.xpath(typeXpath)));
      boolean hasType = typeElement.isDisplayed();

      // Verify provider is displayed in the Space section (after Provider icon)
      String providerXpath =
          "//div[contains(@class, 'ant-space')]//span[text()='" + provider + "']";
      WebElement providerElement =
          wait.until(ExpectedConditions.visibilityOfElementLocated(By.xpath(providerXpath)));
      boolean hasProvider = providerElement.isDisplayed();

      if (!hasTitle || !hasType || !hasProvider) {
        LOG.error(
            "Catalog details page verification failed - hasTitle: {}, hasType: {}, hasProvider: {}",
            hasTitle,
            hasType,
            hasProvider);
        return false;
      }
      return true;
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
      return false;
    }
  }

  public boolean verifyShowFilesContent() {
    try {
      String files = tabFilesContent.getAttribute("hidden");
      return Objects.equals(files, null);
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
      return false;
    }
  }

  public boolean verifyEditedCatalog(String name) {
    try {
      // Ant Design Tree structure: ant-tree-treenode > ant-tree-title > div[@title='name']
      String xpath =
          "//div[contains(@class, 'ant-tree-treenode')]//span[contains(@class, 'ant-tree-title')]//div[@title='"
              + name
              + "']";
      WebDriverWait wait = new WebDriverWait(driver, Duration.ofSeconds(MAX_TIMEOUT));
      WebElement treeNode =
          wait.until(ExpectedConditions.visibilityOfElementLocated(By.xpath(xpath)));

      // Check if the link text is match with name
      boolean match = Objects.equals(treeNode.getText(), name);
      if (!match) {
        LOG.error("tree node {} does not match with name: {}", treeNode.getText(), name);
        return false;
      }
      return true;
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
      return false;
    }
  }

  public boolean verifyEmptyTableData() {
    try {
      // Check is empty table - Ant Design shows ant-empty component for empty tables
      WebDriverWait wait = new WebDriverWait(driver, Duration.ofSeconds(MAX_TIMEOUT));
      String emptyXpath = "//div[@data-refer='table-grid']//div[contains(@class, 'ant-empty')]";
      WebElement emptyElement =
          wait.until(ExpectedConditions.visibilityOfElementLocated(By.xpath(emptyXpath)));
      boolean isNoRows = emptyElement.isDisplayed();
      if (!isNoRows) {
        LOG.error("is not empty catalog list, empty element not found");
        return false;
      }
      return true;
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
      return false;
    }
  }

  public boolean verifyShowTableTitle(String title) {
    try {
      WebDriverWait wait = new WebDriverWait(driver, Duration.ofSeconds(MAX_TIMEOUT));

      // Only use the data-refer scoped xpath to avoid matching tabs from other pages
      // (e.g. the metalake-level "Relational" tab which does NOT have data-refer='details-tabs').
      // All detail pages (CatalogDetailsPage, SchemaDetailsPage, TableDetailsPage) render:
      //   <Tabs data-refer='details-tabs' ...>
      String xpath =
          "//*[@data-refer='details-tabs']//div[contains(@class, 'ant-tabs-tab-active')]";
      WebElement activeTab =
          wait.until(ExpectedConditions.visibilityOfElementLocated(By.xpath(xpath)));
      String tabText = activeTab.getText().trim();
      if (tabText.equals(title) || tabText.contains(title)) {
        LOG.info("Found active tab with title: {}", tabText);
        return true;
      }
      LOG.error("Active tab text '{}' does not match expected title '{}'", tabText, title);
      return false;
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
      return false;
    }
  }

  /**
   * Verifies if a given property item is present in a specified list.
   *
   * @param item The key or value item of the property.
   * @param key The key of the property.
   * @param value The value of key item of the property.
   * @param isHighlight Whether to highlight the property item or not.
   * @return True if the property item is found in the list, false otherwise.
   */
  public boolean verifyShowPropertiesItemInList(
      String item, String key, String value, Boolean isHighlight) {
    WebDriverWait wait = new WebDriverWait(driver, Duration.ofSeconds(ACTION_SLEEP));
    String xpath;
    if (isHighlight) {
      xpath = "//div[@data-refer='props-" + item + "-" + key + "-highlight']";
    } else {
      xpath = "//div[@data-refer='props-" + item + "-" + key + "']";
    }
    WebElement propertyElement =
        wait.until(ExpectedConditions.visibilityOfElementLocated(By.xpath(xpath)));

    boolean match = Objects.equals(propertyElement.getText(), value);

    if (!match) {
      LOG.error("Prop: does not include itemName: {}", value);
      return false;
    }
    return true;
  }

  /**
   * Click the properties link to open the properties popover.
   *
   * @throws InterruptedException if the thread is interrupted.
   */
  public void clickPropertiesLink() throws InterruptedException {
    try {
      WebDriverWait wait = new WebDriverWait(driver, Duration.ofSeconds(MAX_TIMEOUT));
      String xpath = "//a[@data-refer='properties-link']";
      WebElement propertiesLink =
          wait.until(ExpectedConditions.elementToBeClickable(By.xpath(xpath)));
      propertiesLink.click();
      Thread.sleep(500); // Wait for popover to appear
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
    }
  }

  /**
   * Verify that a property key-value pair exists in the properties popover.
   *
   * @param key The property key to verify.
   * @param expectedValue The expected value for the property.
   * @return True if the property exists with the expected value, false otherwise.
   */
  public boolean verifyPropertyInPopover(String key, String expectedValue) {
    try {
      WebDriverWait wait = new WebDriverWait(driver, Duration.ofSeconds(ACTION_SLEEP));
      // Verify the popover content is visible
      String popoverXpath = "//*[@data-refer='properties-popover-content']";
      wait.until(ExpectedConditions.visibilityOfElementLocated(By.xpath(popoverXpath)));

      // Verify the key exists
      String keyXpath = "//span[@data-refer='props-key-" + key + "']";
      WebElement keyElement =
          wait.until(ExpectedConditions.visibilityOfElementLocated(By.xpath(keyXpath)));
      if (!Objects.equals(keyElement.getText(), key)) {
        LOG.error("Property key mismatch: expected {}, got {}", key, keyElement.getText());
        return false;
      }

      // Verify the value
      String valueXpath = "//span[@data-refer='props-value-" + key + "']";
      WebElement valueElement =
          wait.until(ExpectedConditions.visibilityOfElementLocated(By.xpath(valueXpath)));
      if (!Objects.equals(valueElement.getText(), expectedValue)) {
        LOG.error(
            "Property value mismatch for key {}: expected {}, got {}",
            key,
            expectedValue,
            valueElement.getText());
        return false;
      }

      return true;
    } catch (Exception e) {
      LOG.error("Failed to verify property in popover: {}", e.getMessage(), e);
      return false;
    }
  }

  /**
   * Verify the table overview link value for properties, partitions, sort orders, distribution, or
   * indexes.
   *
   * @param linkType The type of link to verify (properties, partitions, sortorders, distribution,
   *     indexes).
   * @param expectedValue The expected value/count displayed in the link.
   * @return True if the link displays the expected value, false otherwise.
   */
  public boolean verifyTableOverviewLink(String linkType, String expectedValue) {
    try {
      WebDriverWait wait = new WebDriverWait(driver, Duration.ofSeconds(MAX_TIMEOUT));
      String xpath = "//a[@data-refer='overview-" + linkType + "-link']";
      WebElement linkElement =
          wait.until(ExpectedConditions.visibilityOfElementLocated(By.xpath(xpath)));
      String actualValue = linkElement.getText().trim();
      if (!Objects.equals(actualValue, expectedValue)) {
        LOG.error(
            "Table overview {} link mismatch: expected '{}', got '{}'",
            linkType,
            expectedValue,
            actualValue);
        return false;
      }
      return true;
    } catch (Exception e) {
      LOG.error("Failed to verify table overview {} link: {}", linkType, e.getMessage(), e);
      return false;
    }
  }

  /**
   * Click the table overview link to open the popover.
   *
   * @param linkType The type of link to click (properties, partitions, sortorders, distribution,
   *     indexes).
   * @throws InterruptedException if the thread is interrupted.
   */
  public void clickTableOverviewLink(String linkType) throws InterruptedException {
    try {
      WebDriverWait wait = new WebDriverWait(driver, Duration.ofSeconds(MAX_TIMEOUT));
      String xpath = "//a[@data-refer='overview-" + linkType + "-link']";
      WebElement linkElement = wait.until(ExpectedConditions.elementToBeClickable(By.xpath(xpath)));
      linkElement.click();
      Thread.sleep(500); // Wait for popover to appear
    } catch (Exception e) {
      LOG.error("Failed to click table overview {} link: {}", linkType, e.getMessage(), e);
    }
  }

  /**
   * Hover the overview tip area for a given overview `type` (e.g. "sortOrders"), trying multiple
   * possible locators to support different UI variants.
   */
  public void hoverOverviewTip(String type) throws InterruptedException {
    String[] candidateXpaths =
        new String[] {
          "//*[@data-refer='overview-tip-" + type + "']",
          "//*[@data-refer='overview-tip-" + type + "-items']",
          "//*[@data-refer='overview-" + type.toLowerCase() + "-link']",
          "//*[@data-refer='overview-" + type + "-items']",
        };

    WebDriverWait wait = new WebDriverWait(driver, Duration.ofSeconds(5));
    WebElement found = null;
    for (String xp : candidateXpaths) {
      try {
        found = wait.until(ExpectedConditions.presenceOfElementLocated(By.xpath(xp)));
        if (found != null) break;
      } catch (Exception e) {
        LOG.debug("hoverOverviewTip: xpath {} not present: {}", xp, e.getMessage());
        // try next candidate
      }
    }

    if (found == null) {
      LOG.warn("hoverOverviewTip: no overview element found for type {}", type);
      return;
    }

    ((JavascriptExecutor) driver)
        .executeScript("arguments[0].scrollIntoView({block: 'center'});", found);
    Thread.sleep(300);
    try {
      Actions actions = new Actions(driver);
      actions.moveToElement(found).perform();
      Thread.sleep(300);
    } catch (Exception e) {
      // fallback to JS click to open popover if hover fails
      try {
        ((JavascriptExecutor) driver).executeScript("arguments[0].click();", found);
        Thread.sleep(300);
      } catch (Exception ex) {
        LOG.error("hoverOverviewTip: cannot hover or click overview element", ex);
      }
    }
  }

  public boolean verifyShowDataItemInList(String itemName, Boolean isColumnLevel) {
    try {
      Thread.sleep(ACTION_SLEEP * 1000);
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
    }
    WebDriverWait wait = new WebDriverWait(driver, Duration.ofSeconds(ACTION_SLEEP));

    // Ant Design Table structure: table > tbody > tr > td
    // For catalog/schema pages, the first column contains a Link (<a> tag)
    // For column level (table details), the first column contains a <p> tag
    String baseXpath =
        "//div[@data-refer='table-grid']"
            + "//table/tbody/tr[contains(@class, 'ant-table-row')]"
            + "/td[contains(@class, 'ant-table-cell')][1]";

    // Try multiple xpaths to handle different table structures
    String[] xpaths;
    if (isColumnLevel) {
      xpaths = new String[] {baseXpath + "//p", baseXpath};
    } else {
      // For schema/catalog list, content is rendered in <a> (Link) or direct text
      xpaths = new String[] {baseXpath + "//a", baseXpath};
    }

    for (String xpath : xpaths) {
      try {
        List<WebElement> list =
            wait.until(ExpectedConditions.visibilityOfAllElementsLocatedBy(By.xpath(xpath)));
        List<String> texts = new ArrayList<>();

        for (int i = 0; i < list.size(); i++) {
          try {
            texts.add(list.get(i).getText());
          } catch (StaleElementReferenceException e) {
            // Element failed, re-find and obtain text
            List<WebElement> refreshedList = driver.findElements(By.xpath(xpath));
            if (i < refreshedList.size()) {
              texts.add(refreshedList.get(i).getText());
            }
          }
        }

        if (texts.contains(itemName)) {
          LOG.info("Found item '{}' in table list: {}", itemName, texts);
          return true;
        }
        LOG.info("Item '{}' not found with xpath '{}', texts: {}", itemName, xpath, texts);
      } catch (Exception e) {
        LOG.info("Xpath '{}' failed, trying next...", xpath);
      }
    }

    LOG.error("table list does not include itemName: {}", itemName);
    return false;
  }

  public boolean verifyNoDataItemInList(String itemName, Boolean isColumnLevel) {
    try {
      Thread.sleep(ACTION_SLEEP * 1000);
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
    }
    // Ant Design Table structure: table > tbody > tr > td
    String xpath =
        "//div[@data-refer='table-grid']"
            + "//table/tbody/tr[contains(@class, 'ant-table-row')]"
            + "/td[contains(@class, 'ant-table-cell')][1]";
    if (isColumnLevel) {
      xpath = xpath + "//p";
    }
    WebDriverWait wait = new WebDriverWait(driver, Duration.ofSeconds(ACTION_SLEEP));
    List<WebElement> list =
        wait.until(ExpectedConditions.visibilityOfAllElementsLocatedBy(By.xpath(xpath)));
    List<String> texts = new ArrayList<>();
    for (WebElement element : list) {
      texts.add(element.getText());
    }

    if (texts.contains(itemName)) {
      LOG.error("table list: {} does not include itemName: {}", texts, itemName);
      return false;
    }

    return true;
  }

  public boolean verifyTableColumns() {
    try {
      List<String> columns = Arrays.asList("Column Name", "Data Type", "Tags", "Policies");

      // Ant Design Table structure: thead > tr > th
      List<WebElement> columnHeadersRows =
          columnHeaders.findElements(By.xpath("./th[contains(@class, 'ant-table-cell')]"));
      // Filter out header cells that have no visible text (selection/expand columns)
      List<String> headerTexts = new ArrayList<>();
      for (WebElement th : columnHeadersRows) {
        String txt = th.getText() == null ? "" : th.getText().trim();
        boolean hasInteractiveChild = false;
        try {
          // if header contains inputs/icons/buttons and no text, treat as non-data header
          List<WebElement> interactive =
              th.findElements(By.xpath(".//input|.//button|.//svg|.//i"));
          if (interactive != null && !interactive.isEmpty()) {
            hasInteractiveChild = true;
          }
        } catch (Exception e) {
          LOG.debug("Error checking interactive children of header: {}", e.getMessage());
        }

        if (!txt.isEmpty()) {
          headerTexts.add(txt);
        } else if (!hasInteractiveChild) {
          // If there is no interactive child but text is empty, still skip it
          // (some UIs render invisible helper headers)
          continue;
        }
      }

      if (headerTexts.size() != columns.size()) {
        LOG.error(
            "Column headers count does not match after filtering, expected: {}, actual: {} -> {}",
            columns.size(),
            headerTexts.size(),
            headerTexts);
        return false;
      }

      for (int i = 0; i < columns.size(); i++) {
        String headerText = headerTexts.get(i);
        if (!headerText.equals(columns.get(i))) {
          LOG.error("Column header '{}' does not match, expected '{}'", headerText, columns.get(i));
          return false;
        }
      }

      return true;
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
      return false;
    }
  }

  public boolean verifyTableProperties(String type, String colName) {
    try {
      String xpath = "";
      String formattedColName = "";
      if (type.equals(DISTRIBUTION)) {
        xpath = "//*[@data-refer='tip-" + DISTRIBUTION + "-item-" + colName + "']";
        formattedColName = "hash[10](" + colName + ")";
      } else if (type.equals(SORT_ORDERS)) {
        xpath = "//*[@data-refer='tip-" + SORT_ORDERS + "-item-" + colName + "']";
        formattedColName = colName + " desc nulls_last";
      }

      // Wait a bit for the popover animation to complete
      Thread.sleep(500);

      // Wait for the popover content to be visible
      WebDriverWait wait = new WebDriverWait(driver, Duration.ofSeconds(10));
      wait.until(ExpectedConditions.presenceOfElementLocated(By.xpath(xpath)));

      // Then find all matching elements
      List<WebElement> tooltipItems = driver.findElements(By.xpath(xpath));
      List<String> texts = new ArrayList<>();
      for (WebElement text : tooltipItems) {
        String textContent = text.getText();
        LOG.info("Found tooltip text: '{}'", textContent);
        texts.add(textContent);
      }

      if (!texts.contains(formattedColName)) {
        LOG.error(
            "Tooltip item {} does not match, actual texts: '{}', expected: '{}'",
            colName,
            texts,
            formattedColName);
        return false;
      }
      return true;
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
      return false;
    }
  }

  public boolean verifyTablePropertiesOverview(List<String> cols) {
    try {
      // Wait a bit for the popover animation to complete
      Thread.sleep(500);

      WebElement columnsText = null;
      boolean isMatchText = false;
      // try multiple possible overview element locators for compatibility across UI versions
      String[] candidateColumnsXpaths =
          new String[] {
            "//*[@data-refer='overview-sortOrders-items']",
            "//*[@data-refer='overview-sortOrders-items']",
            "//*[@data-refer='overview-sortorders-link']",
            "//*[@data-refer='overview-sortOrders-items']"
          };
      for (String xp : candidateColumnsXpaths) {
        try {
          columnsText = driver.findElement(By.xpath(xp));
          if (columnsText != null) {
            isMatchText = columnsText.getText().contains(",");
            break;
          }
        } catch (Exception e) {
          LOG.debug("candidateColumnsXpaths: xpath {} not found or error: {}", xp, e.getMessage());
        }
      }

      List<WebElement> tooltipCols = new ArrayList<>();
      String[] candidateTooltipXpaths =
          new String[] {
            "//*[@data-refer='overview-tip-sortOrders-items']",
            "//*[@data-refer='overview-tip-sortOrders']",
            "//*[@data-refer='overview-tip-sortOrders-items']"
          };
      for (String xp : candidateTooltipXpaths) {
        try {
          List<WebElement> found = driver.findElements(By.xpath(xp));
          if (found != null && !found.isEmpty()) {
            tooltipCols.addAll(found);
          }
        } catch (Exception e) {
          LOG.debug("candidateTooltipXpaths: xpath {} search error: {}", xp, e.getMessage());
        }
      }
      List<String> texts = new ArrayList<>();
      for (WebElement text : tooltipCols) {
        String t = text.getText() == null ? "" : text.getText().trim();
        if (t.isEmpty()) {
          // try to extract non-empty text from descendants (some UI versions render text in child
          // spans)
          try {
            List<WebElement> descendants = text.findElements(By.xpath(".//*"));
            for (WebElement d : descendants) {
              String dt = d.getText() == null ? "" : d.getText().trim();
              if (!dt.isEmpty()) {
                texts.add(dt);
              }
            }
          } catch (Exception e) {
            LOG.debug("extract descendants text failed: {}", e.getMessage());
          }
        } else {
          texts.add(t);
        }
      }
      List<String> colsTexts = new ArrayList<>();
      for (String col : cols) {
        colsTexts.add(col + " desc nulls_last");
      }
      if (!isMatchText || !texts.containsAll(colsTexts)) {
        LOG.error(
            "Overview tooltip does not match, actual: '{}', expected: '{}'", texts, colsTexts);
        return false;
      }
      return true;
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
      return false;
    }
  }

  public boolean verifyBackHomePage() {
    WebDriverWait wait = new WebDriverWait(driver, Duration.ofSeconds(MAX_TIMEOUT));
    wait.until(ExpectedConditions.visibilityOf(metalakePageTitle));
    boolean matchTitle = Objects.equals(metalakePageTitle.getText(), "Metalakes");
    if (!matchTitle) {
      LOG.error("metalakePageTitle: {} does not match with Metalakes", metalakePageTitle.getText());
      return false;
    }
    return true;
  }

  public boolean verifyRefreshPage() {
    WebDriverWait wait = new WebDriverWait(driver, Duration.ofSeconds(MAX_TIMEOUT));
    wait.until(
        webDriver ->
            ((JavascriptExecutor) webDriver)
                .executeScript("return document.readyState")
                .equals("complete"));
    wait.until(ExpectedConditions.visibilityOf(metalakeNameLink));
    boolean isDisplayed = metalakeNameLink.isDisplayed();
    if (!isDisplayed) {
      LOG.error("No match with link, get {}", metalakeNameLink.getText());
      return false;
    }
    return true;
  }

  public boolean verifyCreatedCatalogs(List<String> catalogNames) {
    // Ant Design Table structure: table > tbody > tr > td
    List<WebElement> list =
        tableGrid.findElements(
            By.xpath(
                ".//table/tbody/tr[contains(@class, 'ant-table-row')]"
                    + "/td[contains(@class, 'ant-table-cell')][1]"));
    List<String> texts = new ArrayList<>();
    for (WebElement webElement : list) {
      String rowItemColName = webElement.getText();
      texts.add(rowItemColName);
    }
    if (!texts.containsAll(catalogNames)) {
      LOG.error("table list: {} does not containsAll catalogNames: {}", texts, catalogNames);
      return false;
    }
    return true;
  }

  public boolean verifyTreeNodes(List<String> treeNodes) {
    WebDriverWait wait = new WebDriverWait(driver, Duration.ofSeconds(ACTION_SLEEP));
    List<WebElement> list =
        wait.until(
            ExpectedConditions.visibilityOfAllElementsLocatedBy(
                By.xpath(
                    "//div[@data-refer='tree-view']"
                        + "//div[@class='ant-tree-list-holder']"
                        + "/div/div[@class='ant-tree-list-holder-inner']"
                        + "/div[contains(@class, 'ant-tree-treenode')]")));
    List<String> texts = new ArrayList<>();
    for (WebElement webElement : list) {
      String nodeName =
          webElement.findElement(By.xpath(".//span[@class='ant-tree-title']")).getText();
      texts.add(nodeName);
    }
    if (!treeNodes.containsAll(texts)) {
      LOG.error("tree nodes list: {} does not containsAll treeNodes: {}", texts, treeNodes);
      return false;
    }
    return true;
  }

  public boolean verifySelectedNode(String nodeName) {

    WebDriverWait wait = new WebDriverWait(driver, Duration.ofSeconds(ACTION_SLEEP));

    WebElement selectedNode =
        wait.until(
            ExpectedConditions.visibilityOfElementLocated(
                By.xpath(
                    "//div[@data-refer='tree-view']"
                        + "//div[contains(@class, 'ant-tree-treenode-selected')]"
                        + "//span[@class='ant-tree-title']")));
    if (!selectedNode.getText().equals(nodeName)) {
      LOG.error(
          "selectedNode: {} does not match with nodeName: {}", selectedNode.getText(), nodeName);
      return false;
    }
    return true;
  }
}
