/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.filesystem.hadoop.utils;

import com.datastrato.gravitino.filesystem.hadoop.GravitinoVirtualFileSystemConfiguration;
import com.datastrato.gravitino.filesystem.hadoop.HdfsMiniClusterTestBase;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class FileSystemTestUtils {
  private FileSystemTestUtils() {}

  public static Path createGvfsPrefix(
      String metalakeName, String filesetCatalog, String schema, String fileset) {
    return new Path(
        GravitinoVirtualFileSystemConfiguration.GVFS_FILESET_PREFIX
            + "/"
            + metalakeName
            + "/"
            + filesetCatalog
            + "/"
            + schema
            + "/"
            + fileset);
  }

  public static Path createHdfsPrefix(
      String metalakeName, String filesetCatalog, String schema, String fileset) {
    return new Path(
        GravitinoVirtualFileSystemConfiguration.HDFS_SCHEME_PREFIX
            + "localhost/"
            + metalakeName
            + "/"
            + filesetCatalog
            + "/"
            + schema
            + "/"
            + fileset);
  }

  public static Path createHdfsFilePath(String filePath) {
    return new Path(
        GravitinoVirtualFileSystemConfiguration.HDFS_SCHEME_PREFIX + "localhost" + filePath);
  }

  public static void create(Path path, FileSystem fileSystem) throws IOException {
    boolean overwrite = true;
    try (FSDataOutputStream outputStream =
        fileSystem.create(
            path,
            HdfsMiniClusterTestBase.fsPermission(),
            overwrite,
            HdfsMiniClusterTestBase.bufferSize(),
            HdfsMiniClusterTestBase.replication(),
            HdfsMiniClusterTestBase.blockSize(),
            HdfsMiniClusterTestBase.progressable())) {}
  }

  public static void append(Path path, FileSystem fileSystem) throws IOException {
    try (FSDataOutputStream mockOutputStream =
        fileSystem.append(path, HdfsMiniClusterTestBase.bufferSize())) {
      // Hello, World!
      byte[] mockBytes = new byte[] {72, 101, 108, 108, 111, 44, 32, 87, 111, 114, 108, 100, 33};
      mockOutputStream.write(mockBytes);
    }
  }

  public static byte[] read(Path path, FileSystem fileSystem) throws IOException {
    try (FSDataInputStream inputStream =
        fileSystem.open(path, HdfsMiniClusterTestBase.bufferSize())) {
      int bytesRead;
      byte[] buffer = new byte[1024];
      try (ByteArrayOutputStream byteOutputStream = new ByteArrayOutputStream()) {
        while ((bytesRead = inputStream.read(buffer)) != -1) {
          byteOutputStream.write(buffer, 0, bytesRead);
        }
        return byteOutputStream.toByteArray();
      }
    }
  }
}
