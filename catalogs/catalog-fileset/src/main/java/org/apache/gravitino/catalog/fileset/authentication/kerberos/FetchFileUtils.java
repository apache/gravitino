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
package org.apache.gravitino.catalog.fileset.authentication.kerberos;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.util.Optional;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class FetchFileUtils {

  private FetchFileUtils() {}

  public static void fetchFileFromUri(
      String fileUri, File destFile, int timeout, Configuration conf) throws IOException {
    try {
      URI uri = new URI(fileUri);
      String scheme = Optional.ofNullable(uri.getScheme()).orElse("file");

      switch (scheme) {
        case "http":
        case "https":
        case "ftp":
          FileUtils.copyURLToFile(uri.toURL(), destFile, timeout * 1000, timeout * 1000);
          break;

        case "file":
          Files.createSymbolicLink(destFile.toPath(), new File(uri.getPath()).toPath());
          break;

        case "hdfs":
          FileSystem.get(conf).copyToLocalFile(new Path(uri), new Path(destFile.toURI()));
          break;

        default:
          throw new IllegalArgumentException(
              String.format("Doesn't support the scheme %s", scheme));
      }
    } catch (URISyntaxException ue) {
      throw new IllegalArgumentException("The uri of file has the wrong format", ue);
    }
  }
}
