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

package org.apache.gravitino.catalog.hive;

import java.io.File;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestFetchFileUtils {

  private static final Logger LOG = LoggerFactory.getLogger(TestFetchFileUtils.class);
  private static final int MAX_RETRIES = 8;
  private static final long INITIAL_RETRY_DELAY_MS = 1000;

  @Test
  public void testLinkLocalFile() throws Exception {
    File srcFile = new File("test");
    File destFile = new File("dest");

    try {
      if (srcFile.createNewFile()) {
        FetchFileUtils.fetchFileFromUri(
            srcFile.toURI().toString(), destFile, 10, new Configuration());
        Assertions.assertTrue(destFile.exists(), "Destination file should exist after linking");
      } else {
        Assertions.fail("Failed to create the source file");
      }
    } finally {
      if (!srcFile.delete()) {
        LOG.warn("Failed to delete source file after test");
      }
      if (!destFile.delete()) {
        LOG.warn("Failed to delete destination file after test");
      }
    }
  }

  @Test
  public void testDownloadFromHTTP() throws Exception {
    File destFile = new File("dest");
    String fileUrl = "https://downloads.apache.org/hadoop/common/KEYS";
    Configuration conf = new Configuration();

    boolean success = false;
    int attempts = 0;

    while (!success && attempts < MAX_RETRIES) {
      try {
        LOG.info("Attempting to download file from URL: {} (Attempt {})", fileUrl, attempts + 1);
        FetchFileUtils.fetchFileFromUri(fileUrl, destFile, 10, conf);
        success = true;
        LOG.info("File downloaded successfully on attempt {}", attempts + 1);
      } catch (IOException e) {
        attempts++;
        LOG.error("Download attempt {} failed due to: {}", attempts, e.getMessage(), e);
        if (attempts < MAX_RETRIES) {
          long retryDelay = INITIAL_RETRY_DELAY_MS * (1L << (attempts - 1));
          LOG.warn("Retrying in {}ms", retryDelay);
          Thread.sleep(retryDelay);
        } else {
          throw new AssertionError("Failed to download file after " + MAX_RETRIES + " attempts", e);
        }
      }
    }

    Assertions.assertTrue(destFile.exists(), "File should exist after successful download");

    if (!destFile.delete()) {
      LOG.warn("Failed to delete destination file after test");
    }
  }
}
