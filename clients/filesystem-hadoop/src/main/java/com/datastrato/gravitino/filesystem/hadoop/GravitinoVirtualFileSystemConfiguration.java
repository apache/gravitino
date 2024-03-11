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
  public static final String FS_GRAVITINO_FILESET_CACHE_MAX_CAPACITY_KEY =
      "fs.gravitino.fileset.cache.maxCapacity";
  public static final int FS_GRAVITINO_FILESET_CACHE_MAX_CAPACITY_DEFAULT = 20;
  public static final String FS_GRAVITINO_FILESET_CACHE_EVICTION_MILLS_AFTER_ACCESS_KEY =
      "fs.gravitino.fileset.cache.evictionMillsAfterAccess";
  public static final long FS_GRAVITINO_FILESET_CACHE_EVICTION_MILLS_AFTER_ACCESS_DEFAULT =
      1000L * 60 * 5;

  private GravitinoVirtualFileSystemConfiguration() {}
}
