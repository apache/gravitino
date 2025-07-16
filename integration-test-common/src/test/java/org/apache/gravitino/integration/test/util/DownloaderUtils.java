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
package org.apache.gravitino.integration.test.util;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Assertions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DownloaderUtils {

  public static final Logger LOG = LoggerFactory.getLogger(DownloaderUtils.class);

  // Supported driver types and their corresponding version extraction patterns
  private static final Pattern MYSQL_DRIVER_PATTERN =
      Pattern.compile("mysql-connector-java-([\\d.]+)\\.jar");
  private static final Pattern POSTGRESQL_DRIVER_PATTERN =
      Pattern.compile("postgresql-([\\d.]+)\\.jar");

  public static void downloadFile(String fileUrl, String... destinationDirectories)
      throws IOException {

    // Check and clean driver version conflicts before downloading
    try {
      checkAndCleanDriverConflicts(fileUrl, destinationDirectories);
    } catch (IOException e) {
      LOG.warn("Failed to check and clean driver conflicts for URL: {}", fileUrl, e);
    }

    URL url = new URL(fileUrl);
    URLConnection connection = url.openConnection();
    String fileName = getFileName(url.getPath());
    String destinationDirectory = destinationDirectories[0];
    Path destinationPath = Paths.get(destinationDirectory, fileName);
    File file = new File(destinationPath.toString());
    if (!file.exists()) {
      LOG.info("Start download file from:{}", fileUrl);
      try (InputStream in = connection.getInputStream()) {

        if (!Files.exists(Paths.get(destinationDirectory))) {
          Files.createDirectories(Paths.get(destinationDirectory));
        }

        Files.copy(in, destinationPath, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
        Assertions.assertTrue(new File(destinationPath.toString()).exists());
        LOG.info("Download file:{} success. path:{}", fileName, destinationPath);
      }
    }
    for (int i = 1; i < destinationDirectories.length; i++) {
      Path targetPath = Paths.get(destinationDirectories[i], fileName);
      Files.copy(destinationPath, targetPath, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
    }
  }

  /**
   * Check and clean driver version conflicts in directories
   *
   * @param targetUrl Target driver URL
   * @param directories Directories to check
   * @throws IOException If file operations fail
   */
  private static void checkAndCleanDriverConflicts(String targetUrl, String... directories)
      throws IOException {
    String expectedFileName = getFileName(targetUrl);
    String expectedVersion = extractVersion(expectedFileName);
    String driverType = getDriverType(expectedFileName);

    // expectedVersion and driverType can be null when the driver type is not currently supported
    if (expectedVersion == null || driverType == null) {
      LOG.warn(
          "Unable to extract driver version or type from URL: {}. Only mysql and postgresql drivers are currently supported.",
          targetUrl);
      return;
    }

    LOG.info(
        "Starting driver version conflict check, expected version: {} - {}",
        driverType,
        expectedVersion);

    for (String directory : directories) {
      cleanConflictingDrivers(directory, driverType, expectedVersion);
    }
  }

  /** Clean conflicting drivers in the specified directory */
  private static void cleanConflictingDrivers(
      String directory, String driverType, String expectedVersion) throws IOException {
    Path dirPath = Paths.get(directory);
    if (!Files.exists(dirPath)) {
      return;
    }

    File[] files = dirPath.toFile().listFiles();
    if (files == null) {
      return;
    }

    Set<String> conflictingFiles = new HashSet<>();

    for (File file : files) {
      if (file.isFile()) {
        String fileName = file.getName();
        String version = extractVersion(fileName);
        String type = getDriverType(fileName);

        // If it's the same type of driver but different version, mark as conflict
        if (type != null
            && type.equals(driverType)
            && version != null
            && !version.equals(expectedVersion)) {
          conflictingFiles.add(fileName);
        }
      }
    }

    // Delete conflicting driver files
    for (String conflictingFile : conflictingFiles) {
      Path conflictingPath = dirPath.resolve(conflictingFile);
      try {
        Files.deleteIfExists(conflictingPath);
        LOG.info("Deleted conflicting driver file: {}", conflictingPath);
      } catch (IOException e) {
        LOG.warn("Failed to delete conflicting driver file: {}", conflictingPath, e);
      }
    }
  }

  /** Extract filename from URL string */
  private static String getFileName(String url) {
    String[] pathSegments = url.split("/");
    return pathSegments[pathSegments.length - 1];
  }

  /** Extract version number from filename */
  private static String extractVersion(String fileName) {
    Matcher mysqlMatcher = MYSQL_DRIVER_PATTERN.matcher(fileName);
    if (mysqlMatcher.matches()) {
      return mysqlMatcher.group(1);
    }

    Matcher postgresqlMatcher = POSTGRESQL_DRIVER_PATTERN.matcher(fileName);
    if (postgresqlMatcher.matches()) {
      return postgresqlMatcher.group(1);
    }

    return null;
  }

  /** Extract driver type from filename */
  private static String getDriverType(String fileName) {
    if (MYSQL_DRIVER_PATTERN.matcher(fileName).matches()) {
      return "mysql";
    }
    if (POSTGRESQL_DRIVER_PATTERN.matcher(fileName).matches()) {
      return "postgresql";
    }
    return null;
  }
}
