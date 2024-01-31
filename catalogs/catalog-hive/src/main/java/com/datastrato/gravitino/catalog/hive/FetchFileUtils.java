/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.hive;

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
