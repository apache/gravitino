/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.integration.test.web.ui.pages;

import com.datastrato.gravitino.integration.test.web.ui.utils.AbstractWebIT;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import org.openqa.selenium.By;
import org.openqa.selenium.JavascriptExecutor;
import org.openqa.selenium.Keys;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.support.FindBy;
import org.openqa.selenium.support.PageFactory;
import org.openqa.selenium.support.ui.ExpectedConditions;
import org.openqa.selenium.support.ui.WebDriverWait;

public class CatalogsPage extends AbstractWebIT {
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

  @FindBy(xpath = "//button[@data-refer='add-catalog-props']")
  public WebElement addCatalogPropsBtn;

  @FindBy(xpath = "//*[@data-refer='handle-submit-catalog']")
  public WebElement handleSubmitCatalogBtn;

  @FindBy(xpath = "//div[@data-refer='tree-view']")
  public WebElement treeView;

  @FindBy(xpath = "//div[@data-refer='table-grid']//div[contains(@class, 'MuiDataGrid-overlay')]")
  public WebElement tableWrapper;

  @FindBy(xpath = "//button[@data-refer='tab-table']")
  public WebElement tabTableBtn;

  @FindBy(xpath = "//div[@data-refer='tab-table-panel']")
  public WebElement tabTableContent;

  @FindBy(xpath = "//button[@data-refer='tab-details']")
  public WebElement tabDetailsBtn;

  @FindBy(xpath = "//div[@data-refer='tab-details-panel']")
  public WebElement tabDetailsContent;

  @FindBy(xpath = "//div[@data-refer='details-drawer']")
  public WebElement detailsDrawer;

  @FindBy(xpath = "//h6[@data-refer='details-title']")
  public WebElement detailsTitle;

  @FindBy(xpath = "//button[@data-refer='close-details-btn']")
  public WebElement closeDetailsBtn;

  @FindBy(xpath = "//button[@data-refer='confirm-delete']")
  public WebElement confirmDeleteBtn;

  @FindBy(xpath = "//a[@data-refer='metalake-name-link']")
  public WebElement metalakeNameLink;

  @FindBy(xpath = "//div[contains(@class, 'MuiDataGrid-columnHeaders')]//div[@role='row']")
  public WebElement columnHeaders;

  @FindBy(xpath = "//*[@data-refer='metalake-page-title']")
  public WebElement metalakePageTitle;

  @FindBy(xpath = "//*[@data-refer='details-props-table']")
  public WebElement detailsPropsTable;

  @FindBy(xpath = "//*[@data-refer='catalog-provider-selector']")
  public WebElement catalogProviderSelector;

  @FindBy(xpath = "//*[@data-refer='catalog-type-selector']")
  public WebElement catalogTypeSelector;

  @FindBy(xpath = "//ul[@aria-labelledby='select-catalog-provider']")
  public WebElement catalogProviderList;

  @FindBy(xpath = "//ul[@aria-labelledby='select-catalog-type']")
  public WebElement catalogTypeList;

  public CatalogsPage() {
    PageFactory.initElements(driver, this);
  }

  public void metalakeSelectChange(String metalakeName) {
    try {
      clickAndWait(metalakeSelect);
      String keyPath = "//li[@data-refer='select-option-" + metalakeName + "']";
      WebElement selectOption = driver.findElement(By.xpath(keyPath));
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

  public void setCatalogCommentField(String nameField) {
    try {
      WebElement metalakeCommentFieldInput =
          catalogCommentField.findElement(By.tagName("textarea"));
      metalakeCommentFieldInput.sendKeys(
          Keys.chord(Keys.HOME, Keys.chord(Keys.SHIFT, Keys.END), Keys.DELETE));
      metalakeCommentFieldInput.clear();
      metalakeCommentFieldInput.sendKeys(nameField);
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
    }
  }

  // set the required fixed catalog properties
  public void setCatalogFixedProp(String key, String value) {
    try {
      String xpath = "//div[@data-prev-refer='props-" + key + "']//input[@name='value']";
      WebElement propItem = driver.findElement(By.xpath(xpath));
      propItem.sendKeys(value);
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
    }
  }

  // set the indexed catalog properties
  public void setCatalogPropsAt(int index, String key, String value) {
    try {
      // Set the indexed props key
      String keyPath = "//div[@data-refer='props-key-" + index + "']//input[@name='key']";
      WebElement keyInput = driver.findElement(By.xpath(keyPath));
      keyInput.sendKeys(key);
      // Set the indexed props value
      String valuePath = "//div[@data-refer='props-value-" + index + "']//input[@name='value']";
      WebElement valueInput = driver.findElement(By.xpath(valuePath));
      valueInput.sendKeys(value);
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
    }
  }

  public void clickViewCatalogBtn(String name) {
    try {
      String xpath = "//button[@data-refer='view-catalog-" + name + "']";
      WebElement btn = driver.findElement(By.xpath(xpath));
      WebDriverWait wait = new WebDriverWait(driver, MAX_TIMEOUT);
      wait.until(ExpectedConditions.elementToBeClickable(By.xpath(xpath)));
      clickAndWait(btn);
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
    }
  }

  public void clickEditCatalogBtn(String name) {
    try {
      String xpath = "//button[@data-refer='edit-catalog-" + name + "']";
      WebElement btn = driver.findElement(By.xpath(xpath));
      WebDriverWait wait = new WebDriverWait(driver, MAX_TIMEOUT);
      wait.until(ExpectedConditions.elementToBeClickable(By.xpath(xpath)));
      clickAndWait(btn);
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
    }
  }

  public void clickDeleteCatalogBtn(String name) {
    try {
      String xpath = "//button[@data-refer='delete-catalog-" + name + "']";
      WebElement btn = driver.findElement(By.xpath(xpath));
      WebDriverWait wait = new WebDriverWait(driver, MAX_TIMEOUT);
      wait.until(ExpectedConditions.elementToBeClickable(By.xpath(xpath)));
      clickAndWait(btn);
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
    }
  }

  public void clickCatalogLink(String metalakeName, String catalogName, String catalogType) {
    try {
      String xpath =
          "//a[@href='?metalake="
              + metalakeName
              + "&catalog="
              + catalogName
              + "&type="
              + catalogType
              + "']";
      WebElement link = tableGrid.findElement(By.xpath(xpath));
      WebDriverWait wait = new WebDriverWait(driver, MAX_TIMEOUT);
      wait.until(ExpectedConditions.elementToBeClickable(By.xpath(xpath)));
      clickAndWait(link);
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
    }
  }

  public void clickSchemaLink(
      String metalakeName, String catalogName, String catalogType, String schemaName) {
    try {
      String xpath =
          "//a[@href='?metalake="
              + metalakeName
              + "&catalog="
              + catalogName
              + "&type="
              + catalogType
              + "&schema="
              + schemaName
              + "']";
      WebElement link = tableGrid.findElement(By.xpath(xpath));
      WebDriverWait wait = new WebDriverWait(driver, MAX_TIMEOUT);
      wait.until(ExpectedConditions.elementToBeClickable(By.xpath(xpath)));
      clickAndWait(link);
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
      String xpath =
          "//a[@href='?metalake="
              + metalakeName
              + "&catalog="
              + catalogName
              + "&type="
              + catalogType
              + "&schema="
              + schemaName
              + "&table="
              + tableName
              + "']";
      WebElement link = tableGrid.findElement(By.xpath(xpath));
      WebDriverWait wait = new WebDriverWait(driver, MAX_TIMEOUT);
      wait.until(ExpectedConditions.elementToBeClickable(By.xpath(xpath)));
      clickAndWait(link);
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
    }
  }

  public void clickBreadCrumbsToCatalogs() {
    try {
      WebDriverWait wait = new WebDriverWait(driver, MAX_TIMEOUT);
      wait.until(ExpectedConditions.elementToBeClickable(metalakeNameLink));
      clickAndWait(metalakeNameLink);
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
    }
  }

  public void clickSelectProvider(String provider) throws InterruptedException {
    WebElement providerItem =
        catalogProviderList.findElement(By.xpath(".//li[@data-value='" + provider + "']"));
    clickAndWait(providerItem);
  }

  public void clickSelectType(String type) throws InterruptedException {
    WebElement typeItem =
        catalogTypeList.findElement(By.xpath(".//li[@data-value='" + type + "']"));
    clickAndWait(typeItem);
  }

  public void clickTreeNode(String nodeKey) throws InterruptedException {
    WebElement treeNode = driver.findElement(By.xpath("//p[@data-refer-node='" + nodeKey + "']"));
    clickAndWait(treeNode);
  }

  public void clickTreeNodeRefresh(String nodeKey) throws InterruptedException {
    WebElement treeNodeRefreshBtn =
        driver.findElement(By.xpath("//button[@data-refer='tree-node-refresh-" + nodeKey + "']"));
    try {
      int reTry = 3;
      for (int i = 0; i < reTry; i++) {
        clickAndWait(treeNodeRefreshBtn);
      }
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
    }
  }

  public boolean verifyGetCatalog(String name) {
    try {
      String xpath =
          "//div[contains(@class, 'ant-tree-treenode')]//span[@title='"
              + name
              + "']//p[@data-refer='tree-node']";
      WebElement treeNode = treeView.findElement(By.xpath(xpath));
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

  public boolean verifyShowTableContent() {
    try {
      String table = tabTableContent.getAttribute("hidden");
      return Objects.equals(table, null);
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
      return false;
    }
  }

  public boolean verifyShowDetailsContent() {
    try {
      String details = tabDetailsContent.getAttribute("hidden");
      return Objects.equals(details, null);
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
      return false;
    }
  }

  public boolean verifyShowCatalogDetails(String name, String hiveMetastoreUris)
      throws InterruptedException {
    try {
      // Check the drawer css property value
      WebDriverWait wait = new WebDriverWait(driver, MAX_TIMEOUT);
      wait.until(ExpectedConditions.visibilityOf(detailsDrawer));
      String drawerVisible = detailsDrawer.getCssValue("visibility");
      boolean isVisible = Objects.equals(drawerVisible, "visible");
      // Check the created catalog name
      String drawerTitle = detailsTitle.getText();
      boolean isText = Objects.equals(drawerTitle, name);

      // Since the provided catalog attributes are known and fixed, validation is also performed on
      // these values.
      boolean isHiveURIS =
          waitShowText(
              hiveMetastoreUris,
              By.xpath(".//*[@data-prev-refer='details-props-key-metastore.uris']"));
      boolean isShowCheck =
          waitShowText(
              "false",
              By.xpath(
                  ".//*[@data-prev-refer='details-props-key-gravitino.bypass.hive.metastore.client.capability.check']"));

      boolean verifyAll = isVisible && isText && isHiveURIS && isShowCheck;
      if (!verifyAll) {
        LOG.error(
            "not verified all - isVisible: {}, isText: {}, isHiveURIS: {}, isShowCheck: {}",
            isVisible,
            isText,
            isHiveURIS,
            isShowCheck);
        return false;
      }
      return true;
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
      return false;
    } finally {
      clickAndWait(closeDetailsBtn);
    }
  }

  public boolean verifyEditedCatalog(String name) {
    try {
      String xpath =
          "//div[contains(@class, 'ant-tree-treenode')]//span[@title='"
              + name
              + "']//p[@data-refer='tree-node']";
      WebElement treeNode = treeView.findElement(By.xpath(xpath));
      WebDriverWait wait = new WebDriverWait(driver, MAX_TIMEOUT);
      wait.until(ExpectedConditions.visibilityOf(treeNode));

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

  public boolean verifyEmptyCatalog() {
    try {
      // Check is empty table
      boolean isNoRows = waitShowText("No rows", tableWrapper);
      if (!isNoRows) {
        LOG.error(
            "is not empty catalog list, tableWrapper text: {}, tableWrapper: {}",
            tableWrapper.getText(),
            tableWrapper);
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
      WebElement text = tabTableBtn.findElement(By.tagName("p"));
      WebDriverWait wait = new WebDriverWait(driver, MAX_TIMEOUT);
      wait.until(ExpectedConditions.visibilityOf(text));
      boolean matchTitle = Objects.equals(text.getText(), title);
      if (!matchTitle) {
        LOG.error("table title: {} does not match with title: {}", text.getText(), title);
        return false;
      }
      return true;
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
      return false;
    }
  }

  public boolean verifyShowDataItemInList(String itemName) {
    try {
      Thread.sleep(ACTION_SLEEP_MILLIS);
      List<WebElement> list =
          driver.findElements(
              By.xpath(
                  "//div[@data-refer='table-grid']//div[contains(@class, 'MuiDataGrid-main')]/div[contains(@class, 'MuiDataGrid-virtualScroller')]/div/div[@role='rowgroup']//div[@data-field='name']"));
      List<String> texts = new ArrayList<>();
      for (WebElement element : list) {
        texts.add(element.getText());
      }

      if (!texts.contains(itemName)) {
        LOG.error("table list: {} does not include itemName: {}", texts, itemName);
        return false;
      }

      return true;
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
      return false;
    }
  }

  public boolean verifyTableColumns() {
    try {
      List<String> columns = Arrays.asList("Name", "Type", "Nullable", "AutoIncrement", "Comment");

      List<WebElement> columnHeadersRows =
          columnHeaders.findElements(By.xpath("./div[@role='columnheader']"));
      if (columnHeadersRows.size() != columns.size()) {
        LOG.error("Column headers count does not match, expected: {}", columns.size());
        return false;
      }

      for (int i = 0; i < columnHeadersRows.size(); i++) {
        String headerText = columnHeadersRows.get(i).getText();
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

  public boolean verifyBackHomePage() {
    try {
      WebDriverWait wait = new WebDriverWait(driver, MAX_TIMEOUT);
      wait.until(ExpectedConditions.visibilityOf(metalakePageTitle));
      boolean matchTitle = Objects.equals(metalakePageTitle.getText(), "Metalakes");
      if (!matchTitle) {
        LOG.error(
            "metalakePageTitle: {} does not match with Metalakes", metalakePageTitle.getText());
        return false;
      }
      return true;
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
      return false;
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
      wait.until(ExpectedConditions.visibilityOf(metalakeNameLink));
      boolean isDisplayed = metalakeNameLink.isDisplayed();
      if (!isDisplayed) {
        LOG.error("No match with link, get {}", metalakeNameLink.getText());
        return false;
      }
      return true;
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
      return false;
    }
  }

  public boolean verifyCreatedCatalogs(List<String> catalogNames) {
    try {
      List<WebElement> list =
          tableGrid.findElements(
              By.xpath(
                  "./div[contains(@class, 'MuiDataGrid-main')]/div[contains(@class, 'MuiDataGrid-virtualScroller')]/div/div[@role='rowgroup']//div[@data-field='name']"));
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
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
      return false;
    }
  }

  public boolean verifyTreeNodes(List<String> treeNodes) {
    try {
      Thread.sleep(ACTION_SLEEP_MILLIS);
      List<WebElement> list =
          driver.findElements(
              By.xpath(
                  "//div[@data-refer='tree-view']//div[@class='ant-tree-list-holder']/div/div[@class='ant-tree-list-holder-inner']/div[contains(@class, 'ant-tree-treenode')]"));
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
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
      return false;
    }
  }

  public boolean verifySelectedNode(String nodeName) {
    try {
      Thread.sleep(ACTION_SLEEP_MILLIS);
      WebElement selectedNode =
          driver.findElement(
              By.xpath(
                  "//div[@data-refer='tree-view']//div[contains(@class, 'ant-tree-treenode-selected')]//span[@class='ant-tree-title']"));
      waitShowText(nodeName, selectedNode);
      if (!selectedNode.getText().equals(nodeName)) {
        LOG.error(
            "selectedNode: {} does not match with nodeName: {}", selectedNode.getText(), nodeName);
        return false;
      }
      return true;
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
      return false;
    }
  }
}
