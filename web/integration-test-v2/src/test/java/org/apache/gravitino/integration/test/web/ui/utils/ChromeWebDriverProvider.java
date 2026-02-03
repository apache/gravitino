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
package org.apache.gravitino.integration.test.web.ui.utils;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.time.Instant;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.SystemUtils;
import org.apache.gravitino.integration.test.util.ITUtils;
import org.apache.logging.log4j.util.Strings;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.chrome.ChromeDriver;
import org.openqa.selenium.chrome.ChromeOptions;
import org.rauschig.jarchivelib.ArchiveFormat;
import org.rauschig.jarchivelib.Archiver;
import org.rauschig.jarchivelib.ArchiverFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// ChromeWebDriverProvider provides a ChromeDriver instance for WEB UI tests.
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
      throw new RuntimeException("Unsupported OS : " + SystemUtils.OS_NAME);
    }
  }

  @Override
  public void downloadWebDriver() {
    // Chrome release list in here:
    // https://commondatastorage.googleapis.com/chromium-browser-snapshots/index.html
    String chromeDownloadURL = "", chromeDriverDownloadURL = "";
    String chromeZipFile = "", chromeDriverZipFile = "";
    if (SystemUtils.IS_OS_LINUX) {
      chromeZipFile = "chrome-linux.zip";
      chromeDownloadURL =
          "https://www.googleapis.com/download/storage/v1/b/chromium-browser-snapshots/o/Linux_x64%2F1000022%2Fchrome-linux.zip?generation=1651778257041732&alt=media";

      chromeDriverZipFile = "chromedriver_linux64.zip";
      chromeDriverDownloadURL =
          "https://www.googleapis.com/download/storage/v1/b/chromium-browser-snapshots/o/Linux_x64%2F1000022%2Fchromedriver_linux64.zip?generation=1651778262235204&alt=media";
    } else if (SystemUtils.IS_OS_MAC_OSX) {
      chromeZipFile = "chrome-mac.zip";
      chromeDownloadURL =
          "https://www.googleapis.com/download/storage/v1/b/chromium-browser-snapshots/o/Mac%2F1000022%2Fchrome-mac.zip?generation=1651779420087881&alt=media";

      chromeDriverZipFile = "chromedriver_mac64.zip";
      chromeDriverDownloadURL =
          "https://www.googleapis.com/download/storage/v1/b/chromium-browser-snapshots/o/Mac%2F1000022%2Fchromedriver_mac64.zip?generation=1651779426705083&alt=media";
    } else if (SystemUtils.IS_OS_WINDOWS) {
      chromeZipFile = "chrome-win.zip";
      chromeDownloadURL =
          "https://www.googleapis.com/download/storage/v1/b/chromium-browser-snapshots/o/Win_x64%2F1000027%2Fchrome-win.zip?generation=1651780728332948&alt=media";

      chromeDriverZipFile = "chromedriver_win32.zip";
      chromeDriverDownloadURL =
          "https://www.googleapis.com/download/storage/v1/b/chromium-browser-snapshots/o/Win_x64%2F1000027%2Fchromedriver_win32.zip?generation=1651780916599219&alt=media";
    }

    downloadZipFile(chromeDriverDownloadURL, chromeDriverZipFile, chromeDriverBinName);
    downloadZipFile(chromeDownloadURL, chromeZipFile, chromeBinName);

    LOG.info("Download the chromeDriver to {} successfully.", downLoadDir);
  }

  @Override
  public WebDriver createWebDriver() {
    System.setProperty(
        "webdriver.chrome.driver", ITUtils.joinPath(downLoadDir, chromeDriverBinName));
    ChromeOptions chromeOptions = new ChromeOptions();

    // Display the web interface during testing
    if (Strings.isEmpty(System.getenv("DISPLAY_WEBPAGE_IN_TESTING"))) {
      chromeOptions.addArguments("--headless");
      chromeOptions.addArguments("--no-sandbox");
    }

    if (SystemUtils.IS_OS_MAC_OSX) {
      chromeOptions.setBinary(
          ITUtils.joinPath(downLoadDir, chromeBinName, "Contents", "MacOS", "Chromium"));
    } else {
      chromeOptions.setBinary(ITUtils.joinPath(downLoadDir, chromeBinName));
    }

    return new ChromeDriver(chromeOptions);
  }

  // Be careful, fileName contains directory path.
  private void downloadZipFile(String url, String zipFileName, String fileName) {
    File targetFile = new File(downLoadDir, fileName);
    if (targetFile.exists() && LOG.isInfoEnabled()) {
      LOG.info("The file {} already exists, skip download.", targetFile.getAbsolutePath());
      return;
    }

    Instant limit = Instant.now().plusSeconds(120);
    int retryNum = 0;
    IOException last = null;

    while (retryNum < 3 && Instant.now().isBefore(limit)) {
      String downLoadTmpDir =
          ITUtils.joinPath(
              System.getenv("IT_PROJECT_DIR"),
              String.format("chrome-%d", Instant.now().toEpochMilli()));
      try {
        LOG.info("Download the zip file from {} to {}", url, downLoadTmpDir);
        File chromeDriverZip = new File(downLoadTmpDir, zipFileName);
        FileUtils.copyURLToFile(new URL(url), chromeDriverZip, 30000, 30000);

        LOG.info(
            "Extract the zip file from "
                + chromeDriverZip.getAbsolutePath()
                + " to "
                + downLoadTmpDir);
        Archiver archiver = ArchiverFactory.createArchiver(ArchiveFormat.ZIP);
        archiver.extract(chromeDriverZip, new File(downLoadTmpDir));

        // fileName contains directory path like "chrome-linux/chrome", there's an assumption
        // that the zip file is extracted to the firstPath "chrome-linux"
        String firstPath = ITUtils.splitPath(fileName)[0];
        LOG.info("filename:{}, firstPath:{}, {}", fileName, firstPath, ITUtils.splitPath(fileName));
        File unzipFile = new File(downLoadTmpDir, firstPath);
        File dstFile = new File(downLoadDir);
        LOG.info(
            "Move file from " + unzipFile.getAbsolutePath() + " to " + dstFile.getAbsolutePath());
        FileUtils.moveToDirectory(unzipFile, dstFile, true);
        LOG.info("Download the zip file from {} to {} successfully.", url, downLoadDir);
        return;
      } catch (IOException e) {
        LOG.error("Download of: {}, failed in path {}", url, downLoadDir, e);
        retryNum += 1;
        last = e;
      } finally {
        LOG.info("Remove temp directory: {}", downLoadTmpDir);
        FileUtils.deleteQuietly(new File(downLoadTmpDir));
      }
    }
    throw new RuntimeException(last);
  }
}
