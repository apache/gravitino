/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.filesystem.hadoop;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.UUID;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

public class FileSystemTestUtils {
  private static final String LOCAL_FS_PREFIX =
      "file:/tmp/gravitino_test_fs_" + UUID.randomUUID().toString().replace("-", "") + "/";

  private static final int BUFFER_SIZE = 3;
  private static final short REPLICATION = 1;
  private static final long BLOCK_SIZE = 1048576L;
  private static final FsPermission MOCK_PERMISSION = FsPermission.createImmutable((short) 0777);
  private static final Progressable DEFAULT_PROGRESS = () -> {};

  private FileSystemTestUtils() {}

  public static String localRootPrefix() {
    return LOCAL_FS_PREFIX;
  }

  public static Path createFilesetPath(String filesetCatalog, String schema, String fileset) {
    return new Path(
        GravitinoVirtualFileSystemConfiguration.GVFS_FILESET_PREFIX
            + "/"
            + filesetCatalog
            + "/"
            + schema
            + "/"
            + fileset);
  }

  public static Path createLocalRootDir(String filesetCatalog) {
    return new Path(LOCAL_FS_PREFIX + filesetCatalog);
  }

  public static Path createLocalDirPrefix(String filesetCatalog, String schema, String fileset) {
    return new Path(LOCAL_FS_PREFIX + filesetCatalog + "/" + schema + "/" + fileset);
  }

  public static void create(Path path, FileSystem fileSystem) throws IOException {
    boolean overwrite = true;
    try (FSDataOutputStream outputStream =
        fileSystem.create(
            path,
            MOCK_PERMISSION,
            overwrite,
            BUFFER_SIZE,
            REPLICATION,
            BLOCK_SIZE,
            DEFAULT_PROGRESS)) {}
  }

  public static void append(Path path, FileSystem fileSystem) throws IOException {
    try (FSDataOutputStream mockOutputStream = fileSystem.append(path, BUFFER_SIZE)) {
      // Hello, World!
      byte[] mockBytes = new byte[] {72, 101, 108, 108, 111, 44, 32, 87, 111, 114, 108, 100, 33};
      mockOutputStream.write(mockBytes);
    }
  }

  public static byte[] read(Path path, FileSystem fileSystem) throws IOException {
    try (FSDataInputStream inputStream = fileSystem.open(path, BUFFER_SIZE)) {
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

  public static void mkdirs(Path path, FileSystem fileSystem) throws IOException {
    fileSystem.mkdirs(path, MOCK_PERMISSION);
  }
}
