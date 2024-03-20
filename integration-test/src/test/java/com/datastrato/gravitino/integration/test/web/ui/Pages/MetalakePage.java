/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.integration.test.web.ui.Pages;

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

  @FindBy(xpath = "//button[@aria-label='Go to next page']")
  public WebElement nextPageBtn;

  @FindBy(xpath = "//button[@aria-label='Go to previous page']")
  public WebElement prevPageBtn;

  public MetalakePage() {
    PageFactory.initElements(driver, this);
  }

  public WebElement findElementByXPath(String xpath) {
    return driver.findElement(By.xpath(xpath));
  }

  // ** comment for MetalakePageTest.java testLinkToCatalogsPage()
  //  public WebElement findElementByLink(String name) {
  //    String xpath = "//div[@data-field='name']//a[@href='/ui/metalakes?metalake=" + name + "']";
  //
  //    return driver.findElement(By.xpath(xpath));
  //  }

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
    clearQueryInput();
    queryMetalakeInput.sendKeys(queryInput);
  }

  public void clearQueryInput() {
    queryMetalakeInput.sendKeys(
        Keys.chord(Keys.HOME, Keys.chord(Keys.SHIFT, Keys.END), Keys.DELETE));
    queryMetalakeInput.clear();
  }

  public void clickDeleteMetalakeBtn(String name) {
    String xpath = "//button[@data-refer='delete-metalake-" + name + "']";
    WebElement deleteMetalakeBtn = driver.findElement(By.xpath(xpath));
    deleteMetalakeBtn.click();
  }

  public void clickViewMetalakeBtn(String name) {
    String xpath = "//button[@data-refer='view-metalake-" + name + "']";
    WebElement viewMetalakeBtn = driver.findElement(By.xpath(xpath));
    viewMetalakeBtn.click();
  }

  public void clickEditMetalakeBtn(String name) {
    String xpath = "//button[@data-refer='edit-metalake-" + name + "']";
    WebElement editMetalakeBtn = driver.findElement(By.xpath(xpath));
    editMetalakeBtn.click();
  }

  public void clickMetalakeLink(String name) {
    String xpath = "//a[@href='/ui/metalakes?metalake=" + name + "']";
    WebElement metalakeLink = driver.findElement(By.xpath(xpath));
    WebDriverWait wait = new WebDriverWait(driver, 10);
    wait.until(ExpectedConditions.elementToBeClickable(By.xpath(xpath)));
    metalakeLink.click();
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

  public boolean checkIsErrorName() {
    String xpath = "//div[@data-refer='metalake-name-field']";
    WebElement nameField = findElementByXPath(xpath);
    List<WebElement> errorText =
        nameField.findElements(By.xpath("//div[contains(@class, 'Mui-error')]"));

    return !errorText.isEmpty();
  }

  // ** comment for MetalakePageTest.java testLinkToCatalogsPage()
  //  public void waitLinkElementDisplayed(String name) {
  //    int retry = 0;
  //    int sleepTimeMillis = 1_000;
  //
  //    while (retry++ < 5) {
  //      try {
  //        WebElement ele = findElementByLink(name);
  //        Wait<WebDriver> wait =
  //            new FluentWait<>(driver)
  //                .withTimeout(Duration.ofSeconds(10))
  //                .pollingEvery(Duration.ofMillis(300))
  //                .ignoring(ElementNotInteractableException.class);
  //        wait.until(d -> ele.isDisplayed());
  //        Thread.sleep(sleepTimeMillis);
  //      } catch (Exception e) {
  //        LOG.error(e.getMessage(), e);
  //      }
  //    }
  //  }

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

  public boolean verifyShowMetalakeDetails(String name) {
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
      return false;
    } finally {
      closeDetailsBtn.click();
    }
  }

  public boolean verifyEmptyMetalake() {
    String xpath = "//div[contains(@class, 'MuiDataGrid-overlay')]";
    WebElement noMetalakeRows = driver.findElement(By.xpath(xpath));

    return Objects.equals(noMetalakeRows.getText(), "No rows");
  }

  public boolean verifyChangePagination() {
    try {
      if (!nextPageBtn.isEnabled()) {
        return false;
      }
      nextPageBtn.click();

      // Check if the previous page button is available
      return prevPageBtn.isEnabled() && performPrevPageAction();
    } catch (Exception e) {
      return false;
    }
  }

  private boolean performPrevPageAction() {
    try {
      prevPageBtn.click();
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
      return false;
    }
  }

  // ** comment for MetalakePageTest.java testLinkToCatalogsPage()
  //  public boolean verifyLinkToCatalogsPage(String name) {
  //    int sleepTimeMillis = 1_000;
  //    try {
  //      Thread.sleep(sleepTimeMillis);
  //      String xpath = "//a[@data-refer='metalake-name-link']";
  //      WebElement nameLink = driver.findElement(By.xpath(xpath));
  //
  //      Wait<WebDriver> wait =
  //          new FluentWait<>(driver)
  //              .withTimeout(Duration.ofSeconds(10))
  //              .pollingEvery(Duration.ofMillis(300))
  //              .ignoring(ElementNotInteractableException.class);
  //      wait.until(d -> nameLink.isDisplayed());
  //
  //      String url = driver.getCurrentUrl();
  //      boolean isUrl = url.contains("/ui/metalakes?metalake=" + name);
  //
  //      return nameLink.isDisplayed() && isUrl;
  //    } catch (Exception e) {
  //      return false;
  //    } finally {
  //      // Back to homepage
  //      String xpath = "//*[@data-refer='back-home-btn']";
  //      WebElement backHomeBtn = driver.findElement(By.xpath(xpath));
  //      backHomeBtn.click();
  //    }
  //  }
}
