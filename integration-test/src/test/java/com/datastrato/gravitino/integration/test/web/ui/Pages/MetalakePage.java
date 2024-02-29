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

  @FindBy(xpath = "//div[@data-id='test']")
  public WebElement createdMetalakeRow;

  @FindBy(xpath = "//div[@data-field='name']//a[@href='/ui/metalakes?metalake=test']")
  public WebElement createdMetalakeLink;

  @FindBy(xpath = "//button[@data-refer='view-metalake-test']")
  public WebElement viewMetalakeBtn;

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

  public void clickViewMetalakeBtn() {
    LOG.info("click view metalake details button");
    this.viewMetalakeBtn.click();
  }

  public void createMetalakeAction() {
    LOG.info("test create metalake action stared");
    clickCreateBtn();
    enterNameField("test");
    enterCommentField("test");
    clickSubmitBtn();
  }

  public void viewMetalakeAction() {
    LOG.info("test view metalake action stared");
    clickViewMetalakeBtn();
    driver.manage().timeouts().implicitlyWait(1, TimeUnit.SECONDS);
  }
}
