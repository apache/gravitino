/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.integration.test.web.ui.pages;

import com.datastrato.gravitino.integration.test.web.ui.utils.AbstractWebIT;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
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

  @FindBy(xpath = "//ul[@aria-labelledby='select-catalog-provider']")
  public WebElement catalogProviderList;

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

  public void setCatalogPropInput(String key, String value) {
    try {
      String xpath = "//div[@data-prev-refer='props-" + key + "']//input[@name='value']";
      WebElement propItem = driver.findElement(By.xpath(xpath));
      propItem.sendKeys(value);
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
    }
  }

  public void setCatalogProps(int index, String key, String value) {
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

  public void sendPostJsonRequest(String targetURL, String jsonInputString) {
    HttpURLConnection connection = null;
    try {
      URL url = new URL(targetURL);
      connection = (HttpURLConnection) url.openConnection();
      connection.setRequestMethod("POST");
      connection.setRequestProperty("Content-Type", "application/json;charset=UTF-8");
      connection.setRequestProperty("Accept", "application/vnd.gravitino.v1+json");

      connection.setUseCaches(false);
      connection.setDoOutput(true);

      try (OutputStream os = connection.getOutputStream()) {
        byte[] input = jsonInputString.getBytes(StandardCharsets.UTF_8);
        os.write(input, 0, input.length);
      }
      int responseCode = connection.getResponseCode();
      LOG.info("POST Response Code :: " + responseCode);
      if (responseCode == HttpURLConnection.HTTP_OK) {
        LOG.info("POST request success.");
      } else {
        LOG.info("POST request failed.");
      }
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
    } finally {
      if (connection != null) {
        connection.disconnect();
      }
    }
  }

  public void createTable(
      String baseURL, String metalakeName, String catalogName, String schemaName) {
    String url =
        baseURL
            + "/api/metalakes/"
            + metalakeName
            + "/catalogs/"
            + catalogName
            + "/schemas/"
            + schemaName
            + "/tables";
    String jsonInputString =
        "{\"name\":\"table\",\"comment\":\"This is my table\",\"columns\":[{\"name\":\"id\",\"type\":\"integer\",\"comment\":\"id column comment\",\"nullable\":true},{\"name\":\"name\",\"type\":\"string\",\"comment\":\"name column comment\",\"nullable\":true},{\"name\":\"age\",\"type\":\"integer\",\"comment\":\"age column comment\",\"nullable\":true},{\"name\":\"dt\",\"type\":\"date\",\"comment\":\"dt column comment\",\"nullable\":true}]}";
    LOG.info(url);
    sendPostJsonRequest(url, jsonInputString);
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

  public boolean verifyCreateCatalog(String name) {
    try {
      String xpath =
          "//div[contains(@class, 'ant-tree-treenode')]//span[@title='"
              + name
              + "']//p[@data-refer='tree-node']";
      WebElement treeNode = treeView.findElement(By.xpath(xpath));
      return Objects.equals(treeNode.getText(), name);
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

      return isVisible && isText && isHiveURIS && isShowCheck;
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
      return Objects.equals(treeNode.getText(), name);
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
        LOG.error(tableWrapper.getText(), tableWrapper);
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
      return Objects.equals(text.getText(), title);
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
        LOG.error("Column headers count does not match expected: {}", columns.size());
        return false;
      }

      for (int i = 0; i < columnHeadersRows.size(); i++) {
        String headerText = columnHeadersRows.get(i).getText();
        if (!headerText.equals(columns.get(i))) {
          LOG.error("Column header '{}' does not match expected '{}'", headerText, columns.get(i));
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
      return Objects.equals(metalakePageTitle.getText(), "Metalakes");
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
}
