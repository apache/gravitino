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
import org.junit.jupiter.api.Assertions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DownloaderUtils {

  public static final Logger LOG = LoggerFactory.getLogger(DownloaderUtils.class);

  public static void downloadFile(String fileUrl, String... destinationDirectories)
      throws IOException {

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

  /** Extract filename from URL string */
  public static String getFileName(String url) {
    String[] pathSegments = url.split("/");
    return pathSegments[pathSegments.length - 1];
  }
}
