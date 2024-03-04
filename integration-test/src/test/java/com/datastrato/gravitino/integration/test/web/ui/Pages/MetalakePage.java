/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.integration.test.web.ui.Pages;

import com.datastrato.gravitino.integration.test.web.ui.utils.AbstractWebIT;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.support.FindBy;
import org.openqa.selenium.support.PageFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetalakePage {
  protected static final Logger LOG = LoggerFactory.getLogger(AbstractWebIT.class);
  protected static WebDriver driver;

  @FindBy(xpath = "//*[@id='createMetalakeBtn']")
  public WebElement createMetalakeBtn;

  @FindBy(xpath = "//*[@id='metalakeNameField']")
  public WebElement nameField;

  @FindBy(xpath = "//*[@id='metalakeCommentField']")
  public WebElement commentField;

  @FindBy(xpath = "//*[@id='submitHandleMetalake']")
  public WebElement submitHandleMetalakeBtn;

  @FindBy(xpath = "//div[contains(@class, 'MuiDataGrid-overlay')]")
  public WebElement noMetalakeRows;

  @FindBy(xpath = "//div[@data-id='test']")
  public WebElement createdMetalakeRow;

  @FindBy(xpath = "//div[@data-id='test_edited']")
  public WebElement editedMetalakeRow;

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

  public MetalakePage(WebDriver driver) {
    MetalakePage.driver = driver;
    PageFactory.initElements(driver, this);
  }

  public void clickCreateBtn() {
    LOG.info("click create button");
    this.createMetalakeBtn.click();
  }

  public void enterNameField(String nameField) {
    LOG.info("set name field");
    this.nameField.sendKeys(nameField);
  }

  public void enterCommentField(String commentField) {
    LOG.info("set comment field");
    this.commentField.sendKeys(commentField);
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

  public boolean verifyIsCreatedMetalake() {
    try {
      createdMetalakeRow.isDisplayed();
      createdMetalakeLink.isDisplayed();

      return Objects.equals(createdMetalakeLink.getText(), "test");
    } catch (Exception e) {
      LOG.error(String.valueOf(e));

      return false;
    }
  }

  public boolean verifyIsEditedMetalake() {
    try {
      return Objects.equals(editedMetalakeLink.getText(), "test_edited");
    } catch (Exception e) {
      LOG.error(String.valueOf(e));

      return false;
    }
  }

  public boolean verifyIsShowDetails() {
    try {
      metalakeDetailsDrawer.isDisplayed();
      String drawerVisible = metalakeDetailsDrawer.getCssValue("visibility");
      LOG.info(drawerVisible);

      return true;
    } catch (Exception e) {
      LOG.error(String.valueOf(e));

      return false;
    } finally {
      driver.manage().timeouts().implicitlyWait(1, TimeUnit.SECONDS);
      clickCloseDetailsBtn();
    }
  }

  public boolean verifyIsDeletedMetalake() {
    return Objects.equals(noMetalakeRows.getText(), "No rows");
  }

  public void clickViewMetalakeBtn() {
    LOG.info("click view metalake details button");
    this.viewMetalakeBtn.click();
  }

  public void clickEditMetalakeBtn() {
    LOG.info("click edit metalake button");
    this.editMetalakeBtn.click();
  }

  public void createMetalakeAction() {
    LOG.info("test create metalake action started");
    clickCreateBtn();
    enterNameField("test");
    enterCommentField("test");
    clickSubmitBtn();
  }

  public void viewMetalakeAction() {
    LOG.info("test view metalake action started");
    clickViewMetalakeBtn();
    driver.manage().timeouts().implicitlyWait(1, TimeUnit.SECONDS);
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
}
