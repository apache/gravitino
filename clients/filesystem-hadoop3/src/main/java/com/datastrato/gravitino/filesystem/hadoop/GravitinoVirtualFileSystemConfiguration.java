/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.filesystem.hadoop;

/** Configuration class for Gravitino Virtual File System. */
class GravitinoVirtualFileSystemConfiguration {
  public static final String GVFS_FILESET_PREFIX = "gvfs://fileset/";
  public static final String GVFS_SCHEME = "gvfs";

  /** The configuration key for the Gravitino server URI. */
  public static final String FS_GRAVITINO_SERVER_URI_KEY = "fs.gravitino.server.uri";

  /** The configuration key for the Gravitino client Metalake. */
  public static final String FS_GRAVITINO_CLIENT_METALAKE_KEY = "fs.gravitino.client.metalake";

  /** The configuration key for the Gravitino client auth type. */
  public static final String FS_GRAVITINO_CLIENT_AUTH_TYPE_KEY = "fs.gravitino.client.authType";

  public static final String SIMPLE_AUTH_TYPE = "simple";
  public static final String OAUTH2_AUTH_TYPE = "oauth2";

  // oauth2
  /** The configuration key for the URI of the default OAuth server. */
  public static final String FS_GRAVITINO_CLIENT_OAUTH2_SERVER_URI_KEY =
      "fs.gravitino.client.oauth2.serverUri";

  /** The configuration key for the client credential. */
  public static final String FS_GRAVITINO_CLIENT_OAUTH2_CREDENTIAL_KEY =
      "fs.gravitino.client.oauth2.credential";

  /** The configuration key for the path which to get the token. */
  public static final String FS_GRAVITINO_CLIENT_OAUTH2_PATH_KEY =
      "fs.gravitino.client.oauth2.path";

  /** The configuration key for the scope of the token. */
  public static final String FS_GRAVITINO_CLIENT_OAUTH2_SCOPE_KEY =
      "fs.gravitino.client.oauth2.scope";

  /** The configuration key for the maximum capacity of the Gravitino fileset cache. */
  public static final String FS_GRAVITINO_FILESET_CACHE_MAX_CAPACITY_KEY =
      "fs.gravitino.fileset.cache.maxCapacity";

  public static final int FS_GRAVITINO_FILESET_CACHE_MAX_CAPACITY_DEFAULT = 20;

  /**
   * The configuration key for the eviction time of the Gravitino fileset cache, measured in mills
   * after access.
   */
  public static final String FS_GRAVITINO_FILESET_CACHE_EVICTION_MILLS_AFTER_ACCESS_KEY =
      "fs.gravitino.fileset.cache.evictionMillsAfterAccess";

  public static final long FS_GRAVITINO_FILESET_CACHE_EVICTION_MILLS_AFTER_ACCESS_DEFAULT =
      1000L * 60 * 5;

  private GravitinoVirtualFileSystemConfiguration() {}
}
