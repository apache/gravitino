/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.filesystem.hadoop;

public class GravitinoVirtualFileSystemConfiguration {
  public static final String GVFS_FILESET_PREFIX = "gvfs://fileset";
  public static final String GVFS_SCHEME = "gvfs";
  public static final String HDFS_SCHEME = "hdfs";

  private GravitinoVirtualFileSystemConfiguration() {}
}
