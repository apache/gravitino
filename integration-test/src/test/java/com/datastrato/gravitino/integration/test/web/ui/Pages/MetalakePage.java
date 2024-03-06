/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.integration.test.web.ui.Pages;

import com.datastrato.gravitino.integration.test.web.ui.utils.AbstractWebIT;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.support.FindBy;
import org.openqa.selenium.support.PageFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetalakePage {
  protected static final Logger LOG = LoggerFactory.getLogger(AbstractWebIT.class);
  protected static WebDriver driver;

  @FindBy(
      xpath =
          "//div[contains(@class, 'MuiDataGrid-main')]//div[contains(@class, 'MuiDataGrid-virtualScroller')]//div[@role='rowgroup']")
  public WebElement dataViewer;

  @FindBy(xpath = "//*[@id='createMetalakeBtn']")
  public WebElement createMetalakeBtn;

  @FindBy(xpath = "//*[@id='metalakeNameField']")
  public WebElement nameField;

  @FindBy(xpath = "//*[@id='metalakeCommentField']")
  public WebElement commentField;

  @FindBy(xpath = "//*[@id='query-metalake']")
  public WebElement queryMetalakeInput;

  @FindBy(xpath = "//*[@id='submitHandleMetalake']")
  public WebElement submitHandleMetalakeBtn;

  @FindBy(xpath = "//div[@data-id='test']")
  public WebElement createdMetalakeRow;

  @FindBy(xpath = "//div[@data-field='name']//a[@href='/ui/metalakes?metalake=test']")
  public WebElement createdMetalakeLink;

  @FindBy(xpath = "//div[@data-field='name']//a[@href='/ui/metalakes?metalake=test_edited']")
  public WebElement editedMetalakeLink;

  @FindBy(xpath = "//button[@data-refer='view-metalake-test']")
  public WebElement viewMetalakeBtn;

  @FindBy(xpath = "//button[@data-refer='edit-metalake-test']")
  public WebElement editMetalakeBtn;

  @FindBy(xpath = "//button[@data-refer='delete-metalake-test_edited']")
  public WebElement deleteMetalakeBtn;

  @FindBy(xpath = "//button[@data-refer='confirm-delete']")
  public WebElement confirmDeleteBtn;

  @FindBy(xpath = "//div[@data-refer='metalake-details-drawer']")
  public WebElement metalakeDetailsDrawer;

  @FindBy(xpath = "//button[@data-refer='close-metalake-details-btn']")
  public WebElement closeMetalakeDetailsBtn;

  @FindBy(xpath = "//button[@data-refer='add-metalake-props']")
  public WebElement addMetalakePropertyBtn;

  @FindBy(xpath = "//button[@aria-label='Go to next page']")
  public WebElement nextPageBtn;

  @FindBy(xpath = "//button[@aria-label='Go to previous page']")
  public WebElement prevPageBtn;

  public MetalakePage(WebDriver driver) {
    MetalakePage.driver = driver;
    PageFactory.initElements(driver, this);
    driver.manage().timeouts().implicitlyWait(5, TimeUnit.SECONDS);
  }

  public void clickCreateBtn() {
    LOG.info("click create button");
    this.createMetalakeBtn.click();
  }

  public void enterNameField(String nameField) {
    LOG.info("enter name field");
    this.nameField.sendKeys(nameField);
  }

  public void enterCommentField(String commentField) {
    LOG.info("enter comment field");
    this.commentField.sendKeys(commentField);
  }

  public void enterQueryInput(String queryInput) {
    LOG.info("enter query input");
    this.queryMetalakeInput.sendKeys(queryInput);
  }

  public void clickSubmitBtn() {
    LOG.info("click submit button");
    this.submitHandleMetalakeBtn.click();
  }

  public void clickCloseDetailsBtn() {
    LOG.info("click close details button");
    this.closeMetalakeDetailsBtn.click();
  }

  public void clickDeleteMetalakeBtn() {
    LOG.info("click delete metalake button");
    this.deleteMetalakeBtn.click();
  }

  public void clickConfirmDeleteBtn() {
    LOG.info("click confirm delete button");
    this.confirmDeleteBtn.click();
  }

  public void clickViewMetalakeBtn() {
    LOG.info("click view metalake details button");
    this.viewMetalakeBtn.click();
  }

  public void clickEditMetalakeBtn() {
    LOG.info("click edit metalake button");
    this.editMetalakeBtn.click();
  }

  public void clickAddPropertyBtn() {
    LOG.info("click add metalake property button");
    this.addMetalakePropertyBtn.click();
  }

  public void enterPropsValues(int index, String key, String value) {
    LOG.info("enter add property values");
    String keyPath = "//div[@data-refer='add-props-key-" + index + "']//input[@name='key']";
    WebElement keyInput = driver.findElement(By.xpath(keyPath));
    keyInput.sendKeys(key);

    String valuePath = "//div[@data-refer='add-props-value-" + index + "']//input[@name='value']";
    WebElement valueInput = driver.findElement(By.xpath(valuePath));
    valueInput.sendKeys(value);
  }

  public void clickNextPageBtn() {
    LOG.info("click next page button");
    this.nextPageBtn.click();
  }

  public void clickPrevPageBtn() {
    LOG.info("click prev page button");
    this.prevPageBtn.click();
  }

  public boolean verifyIsCreatedMetalake() {
    try {
      boolean isRow = createdMetalakeRow.isDisplayed();
      boolean isLink = createdMetalakeLink.isDisplayed();
      boolean isText = Objects.equals(createdMetalakeLink.getText(), "test");

      return isRow && isLink && isText;
    } catch (Exception e) {
      return false;
    }
  }

  public boolean verifyQueryMetalake() {
    try {
      List<WebElement> dataList = dataViewer.findElements(By.xpath(".//div[@data-field='name']"));

      return dataList.size() == 1 && Objects.equals(dataList.get(0).getText(), "test");
    } catch (Exception e) {
      return false;
    }
  }

  public boolean verifyIsEditedMetalake() {
    try {
      return Objects.equals(editedMetalakeLink.getText(), "test_edited");
    } catch (Exception e) {
      return false;
    }
  }

  public boolean verifyIsShowDetails() {
    try {
      metalakeDetailsDrawer.isDisplayed();
      String drawerVisible = metalakeDetailsDrawer.getCssValue("visibility");

      return Objects.equals(drawerVisible, "visible");
    } catch (Exception e) {
      return false;
    } finally {
      clickCloseDetailsBtn();
    }
  }

  public boolean verifyIsDeletedMetalake() {
    String noRowPath = "//div[contains(@class, 'MuiDataGrid-overlay')]";
    WebElement noMetalakeRows = driver.findElement(By.xpath(noRowPath));

    return Objects.equals(noMetalakeRows.getText(), "No rows");
  }

  public boolean verifyIsCreatedManyMetalakes() {
    try {
      List<WebElement> dataList = dataViewer.findElements(By.xpath(".//div[@data-field='name']"));

      if (dataList.size() == 9) {
        clickNextPageBtn();
        List<WebElement> dataListNext =
            dataViewer.findElements(By.xpath(".//div[@data-field='name']"));

        return dataListNext.size() == 1;
      } else {
        return false;
      }
    } catch (Exception e) {
      return false;
    } finally {
      clickPrevPageBtn();
    }
  }

  public boolean verifyIsCreatedMetalakeWithProperty() {
    try {
      boolean isRow = createdMetalakeRow.isDisplayed();
      boolean isLink = createdMetalakeLink.isDisplayed();
      boolean isText = Objects.equals(createdMetalakeLink.getText(), "test");

      return isRow && isLink && isText;
    } catch (Exception e) {
      return false;
    }
  }

  public boolean verifyIsLinkedToCatalogsPage() {
    try {
      String nameLinkPath = "//a[@data-refer='metalake-name-link']";
      WebElement nameLink = driver.findElement(By.xpath(nameLinkPath));

      String url = driver.getCurrentUrl();
      boolean isUrl = url.contains("/ui/metalakes?metalake=metalake_test");

      return nameLink.isDisplayed() && isUrl;
    } catch (Exception e) {
      return false;
    }
  }

  public void createMetalakeAction(String name, String comment, boolean submit) {
    LOG.info("test create metalake action started");
    clickCreateBtn();
    enterNameField(name);
    enterCommentField(comment);
    if (submit) {
      clickSubmitBtn();
    }
  }

  public void queryMetalakeAction() {
    LOG.info("test query metalake action started");
    enterQueryInput("tes");
  }

  public void viewMetalakeAction() {
    LOG.info("test view metalake action started");
    clickViewMetalakeBtn();
  }

  public void editMetalakeAction() {
    LOG.info("test edit metalake action started");
    clickEditMetalakeBtn();
    enterNameField("_edited");
    clickSubmitBtn();
  }

  public void deleteMetalakeAction() {
    LOG.info("test delete metalake action started");
    clickDeleteMetalakeBtn();
    clickConfirmDeleteBtn();
  }

  public void createManyMetalakesAction() {
    LOG.info("test create many metalakes action started");
    int[] arraySize = new int[11];
    for (int i = 0; i < arraySize.length; i++) {
      LOG.info("create metalake: {}", i + 1);
      createMetalakeAction("test_" + (i + 1), "test", true);
    }
  }

  public void createMetalakeWithPropertyAction() {
    LOG.info("test create metalake with property action started");
    createMetalakeAction("test", "test", false);
    clickAddPropertyBtn();
    enterPropsValues(0, "test", "test");
    clickAddPropertyBtn();
    enterPropsValues(1, "test1", "test1");
    clickSubmitBtn();
  }

  public void linkToCatalogsPageAction() {
    LOG.info("test link to catalogs page action started");
    createMetalakeAction("metalake_test", "test", true);

    String linkPath = "//div[@data-field='name']//a[@href='/ui/metalakes?metalake=metalake_test']";
    WebElement metalakeLink = driver.findElement(By.xpath(linkPath));
    metalakeLink.click();
  }
}
