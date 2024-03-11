/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.filesystem.hadoop;

public class GravitinoVirtualFileSystemConfiguration {
  public static final String GVFS_FILESET_PREFIX = "gvfs://fileset/";
  public static final String GVFS_SCHEME = "gvfs";
  public static final String FS_GRAVITINO_SERVER_URI_KEY = "fs.gravitino.server.uri";
  public static final String FS_GRAVITINO_CLIENT_METALAKE_KEY = "fs.gravitino.client.metalake";

  private GravitinoVirtualFileSystemConfiguration() {}
}
