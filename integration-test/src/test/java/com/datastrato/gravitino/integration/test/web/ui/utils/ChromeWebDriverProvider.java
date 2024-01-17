/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.integration.test.web.ui.utils;

import com.datastrato.gravitino.integration.test.util.ITUtils;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.SystemUtils;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.chrome.ChromeDriver;
import org.openqa.selenium.chrome.ChromeOptions;
import org.rauschig.jarchivelib.ArchiveFormat;
import org.rauschig.jarchivelib.Archiver;
import org.rauschig.jarchivelib.ArchiverFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ChromeWebDriverProvider implements WebDriverProvider {
  protected static final Logger LOG = LoggerFactory.getLogger(ChromeWebDriverProvider.class);
  private final String chromeDriverBinName;
  private final String chromeBinName;
  private static final String downLoadDir =
      ITUtils.joinPath(System.getenv("IT_PROJECT_DIR"), "chrome");

  public ChromeWebDriverProvider() {
    if (SystemUtils.IS_OS_MAC_OSX) {
      this.chromeDriverBinName = ITUtils.joinPath("chromedriver_mac64", "chromedriver");
      this.chromeBinName = ITUtils.joinPath("chrome-mac", "Chromium.app");
    } else if (SystemUtils.IS_OS_LINUX) {
      this.chromeDriverBinName = ITUtils.joinPath("chromedriver_linux64", "chromedriver");
      this.chromeBinName = ITUtils.joinPath("chrome-linux", "chrome");
    } else if (SystemUtils.IS_OS_WINDOWS) {
      this.chromeDriverBinName = ITUtils.joinPath("chromedriver_win32", "chromedriver.exe");
      this.chromeBinName = ITUtils.joinPath("chrome-win", "chrome.exe");
    } else {
      throw new RuntimeException("Unsupported OS");
    }
  }

  @Override
  public void downloadWebDriver() {
    // https://commondatastorage.googleapis.com/chromium-browser-snapshots/index.html
    StringBuilder downloadURL =
        new StringBuilder(
            "https://www.googleapis.com/download/storage/v1/b/chromium-browser-snapshots/o/");
    StringBuilder chromeDownloadURL = new StringBuilder(downloadURL),
        chromeDriverDownloadURL = new StringBuilder(downloadURL);

    String chromeZipFile = "", chromeDriverZipFile = "";
    if (SystemUtils.IS_OS_LINUX) {
      // https://www.googleapis.com/download/storage/v1/b/chromium-browser-snapshots/o/Linux_x64%2F1000022%2Fchrome-linux.zip?generation=1651778257041732&alt=media
      chromeZipFile = "chrome-linux.zip";
      chromeDownloadURL
          .append("Linux_x64%2F1000022%2F")
          .append(chromeZipFile)
          .append("?generation=1651778257041732&alt=media");

      // https://www.googleapis.com/download/storage/v1/b/chromium-browser-snapshots/o/Linux_x64%2F1000022%2Fchromedriver_linux64.zip?generation=1651778262235204&alt=media
      chromeDriverZipFile = "chromedriver_linux64.zip";
      chromeDriverDownloadURL
          .append("Linux_x64%2F1000022%2F")
          .append(chromeDriverZipFile)
          .append("?generation=1651778262235204&alt=media");
    } else if (SystemUtils.IS_OS_MAC_OSX) {
      // https://www.googleapis.com/download/storage/v1/b/chromium-browser-snapshots/o/Mac%2F1000022%2Fchrome-mac.zip?generation=1651779420087881&alt=media
      chromeZipFile = "chrome-mac.zip";
      chromeDownloadURL
          .append("Mac%2F1000022%2F")
          .append(chromeZipFile)
          .append("?generation=1651779420087881&alt=media");

      // https://www.googleapis.com/download/storage/v1/b/chromium-browser-snapshots/o/Mac%2F1000022%2Fchromedriver_mac64.zip?generation=1651779426705083&alt=media
      chromeDriverZipFile = "chromedriver_mac64.zip";
      chromeDriverDownloadURL
          .append("Mac%2F1000022%2F")
          .append(chromeDriverZipFile)
          .append("?generation=1651779426705083&alt=media");
    } else if (SystemUtils.IS_OS_WINDOWS) {
      // https://www.googleapis.com/download/storage/v1/b/chromium-browser-snapshots/o/Win_x64%2F1000027%2Fchrome-win.zip?generation=1651780728332948&alt=media
      chromeZipFile = "chrome-win.zip";
      chromeDownloadURL
          .append("Win_x64%2F1000027%2F")
          .append(chromeZipFile)
          .append("?generation=1651780728332948&alt=media");

      // https://www.googleapis.com/download/storage/v1/b/chromium-browser-snapshots/o/Win_x64%2F1000027%2Fchromedriver_win32.zip?generation=1651780916599219&alt=media
      chromeDriverZipFile = "chromedriver_win32.zip";
      chromeDriverDownloadURL
          .append("Win_x64%2F1000027%2F")
          .append(chromeDriverZipFile)
          .append("?generation=1651780916599219&alt=media");
    }

    downloadZipFile(chromeDriverDownloadURL.toString(), chromeDriverZipFile, chromeDriverBinName);
    downloadZipFile(chromeDownloadURL.toString(), chromeZipFile, chromeBinName);

    LOG.info("Download the chromeDriver to " + downLoadDir + " successfully.");
  }

  @Override
  public WebDriver createWebDriver() {
    System.setProperty(
        "webdriver.chrome.driver", ITUtils.joinPath(downLoadDir, chromeDriverBinName));
    ChromeOptions chromeOptions = new ChromeOptions();
    if (SystemUtils.IS_OS_MAC_OSX) {
      chromeOptions.setBinary(
          ITUtils.joinPath(downLoadDir, chromeBinName, "Contents", "MacOS", "Chromium"));
    } else {
      chromeOptions.setBinary(ITUtils.joinPath(downLoadDir, chromeBinName));
      chromeOptions.addArguments("--headless");
    }

    return new ChromeDriver(chromeOptions);
  }

  private void downloadZipFile(String url, String zipFileName, String fileName) {
    File targetFile = new File(downLoadDir, fileName);
    if (targetFile.exists()) {
      LOG.info("The file " + targetFile.getAbsolutePath() + " already exists, skip download.");
      return;
    }

    try {
      LOG.info("Download the zip file from " + url + " to " + downLoadDir);
      File chromeDriverZip = new File(ChromeWebDriverProvider.downLoadDir, zipFileName);
      FileUtils.copyURLToFile(new URL(url), chromeDriverZip);

      LOG.info("Extract the zip file from " + chromeDriverZip.getAbsolutePath());
      Archiver archiver = ArchiverFactory.createArchiver(ArchiveFormat.ZIP);
      archiver.extract(new File(downLoadDir, zipFileName), new File(downLoadDir));
    } catch (IOException e) {
      LOG.error(
          "Download of: " + url + ", failed in path " + ChromeWebDriverProvider.downLoadDir, e);
      throw new RuntimeException(e);
    }
    LOG.info("Download the zip file from " + url + " to " + downLoadDir + " successfully.");
  }
}
