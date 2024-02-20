/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.filesystem.hadoop;

import java.io.Closeable;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.util.Progressable;

public class HdfsMiniClusterTestBase implements Closeable {
  private static final MiniDFSCluster HDFS_CLUSTER;
  private static final int BUFFER_SIZE = 3;
  private static final short replication = 1;
  private static final long blockSize = 1048576L;
  private static final FsPermission MOCK_PERMISSION = FsPermission.createImmutable((short) 0777);
  private static final Progressable DEFAULT_PROGRESS = () -> {};

  static {
    Configuration hdfsConf = new Configuration();
    try {
      HDFS_CLUSTER =
          new MiniDFSCluster.Builder(hdfsConf).nameNodePort(8020).numDataNodes(1).build();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static FileSystem hadoopFileSystem() throws IOException {
    return HDFS_CLUSTER.getFileSystem();
  }

  public static int bufferSize() {
    return BUFFER_SIZE;
  }

  public static short replication() {
    return replication;
  }

  public static long blockSize() {
    return blockSize;
  }

  public static FsPermission fsPermission() {
    return MOCK_PERMISSION;
  }

  public static Progressable progressable() {
    return DEFAULT_PROGRESS;
  }

  @Override
  public void close() {
    if (HDFS_CLUSTER != null) {
      HDFS_CLUSTER.shutdown();
    }
  }
}
