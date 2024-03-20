/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.integration.test.web.ui.pages;

import com.datastrato.gravitino.integration.test.web.ui.utils.AbstractWebIT;
import java.util.List;
import java.util.Objects;
import org.openqa.selenium.By;
import org.openqa.selenium.Keys;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.support.FindBy;
import org.openqa.selenium.support.PageFactory;
import org.openqa.selenium.support.ui.ExpectedConditions;
import org.openqa.selenium.support.ui.WebDriverWait;

public class MetalakePage extends AbstractWebIT {
  @FindBy(
      xpath =
          "//div[contains(@class, 'MuiDataGrid-main')]//div[contains(@class, 'MuiDataGrid-virtualScroller')]//div[@role='rowgroup']")
  public WebElement dataViewer;

  @FindBy(xpath = "//div[@data-refer='metalake-table-grid']")
  public WebElement metalakeTableGrid;

  @FindBy(xpath = "//*[@id='createMetalakeBtn']")
  public WebElement createMetalakeBtn;

  @FindBy(xpath = "//*[@id='metalakeNameField']")
  public WebElement metalakeNameField;

  @FindBy(xpath = "//*[@id='metalakeCommentField']")
  public WebElement metalakeCommentField;

  @FindBy(xpath = "//*[@id='query-metalake']")
  public WebElement queryMetalakeInput;

  @FindBy(xpath = "//*[@id='submitHandleMetalake']")
  public WebElement submitHandleMetalakeBtn;

  @FindBy(xpath = "//*[@id='cancelHandleMetalake']")
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

  public MetalakePage() {
    PageFactory.initElements(driver, this);
  }

  public WebElement findElementByXPath(String xpath) {
    return driver.findElement(By.xpath(xpath));
  }

  public void setMetalakeNameField(String nameField) {
    metalakeNameField.sendKeys(
        Keys.chord(Keys.HOME, Keys.chord(Keys.SHIFT, Keys.END), Keys.DELETE));
    metalakeNameField.clear();
    metalakeNameField.sendKeys(nameField);
  }

  public void setMetalakeCommentField(String commentField) {
    metalakeCommentField.sendKeys(
        Keys.chord(Keys.HOME, Keys.chord(Keys.SHIFT, Keys.END), Keys.DELETE));
    metalakeCommentField.clear();
    metalakeCommentField.sendKeys(commentField);
  }

  public void setQueryInput(String queryInput) {
    try {
      Thread.sleep(ACTION_SLEEP_MILLIS);
      clearQueryInput();
      queryMetalakeInput.sendKeys(queryInput);
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
      Thread.sleep(ACTION_SLEEP_MILLIS);
      String xpath = "//a[@data-refer='metalake-link-" + name + "']";
      LOG.info("Debug metalake name: {}", name);
      LOG.info(metalakeTableGrid.getAttribute("innerHTML"));
      WebElement metalakeLink = metalakeTableGrid.findElement(By.xpath(xpath));
      LOG.info(metalakeLink.getAttribute("innerHTML"));
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
      String xpath = "//div[@data-refer='metalake-name-field']";
      WebElement nameField = findElementByXPath(xpath);
      List<WebElement> errorText =
          nameField.findElements(By.xpath("//div[contains(@class, 'Mui-error')]"));

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
      String xpath =
          "//div[@data-refer='metalake-table-grid']//div[contains(@class, 'MuiDataGrid-overlay')]";

      boolean isNoRows = waitShowText("No rows", By.xpath(xpath));

      if (!isNoRows) {
        WebElement noMetalakeRows = driver.findElement(By.xpath(xpath));
        LOG.error(noMetalakeRows.getText(), noMetalakeRows);
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
      setQueryInput(name);
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

  public boolean verifyLinkToCatalogsPage(String name) {
    try {
      String xpath = "//a[@data-refer='metalake-name-link']";
      WebDriverWait wait = new WebDriverWait(driver, MAX_TIMEOUT);
      WebElement nameLink = driver.findElement(By.xpath(xpath));
      wait.until(ExpectedConditions.visibilityOf(nameLink));
      wait.until(ExpectedConditions.urlContains(nameLink.getAttribute("href")));

      if (!driver.getCurrentUrl().contains(name) || !nameLink.getAttribute("href").contains(name)) {
        LOG.error("name link is not match");
        return false;
      }

      return true;
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
      return false;
    }
  }
}
