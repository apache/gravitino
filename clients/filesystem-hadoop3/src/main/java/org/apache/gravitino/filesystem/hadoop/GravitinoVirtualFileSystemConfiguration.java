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
package org.apache.gravitino.filesystem.hadoop;

import com.google.common.collect.ImmutableList;
import java.util.List;

/** Configuration class for Gravitino Virtual File System. */
public class GravitinoVirtualFileSystemConfiguration {

  /**
   * The prefix of the Gravitino fileset URI. The URI of the Gravitino fileset should start with
   * this prefix.
   */
  public static final String GVFS_FILESET_PREFIX = "gvfs://fileset";

  /** The scheme of the Gravitino Virtual File System. */
  public static final String GVFS_SCHEME = "gvfs";

  /** The prefix of the Gravitino Virtual File System. */
  public static final String GVFS_CONFIG_PREFIX = "fs.gvfs.";

  /** The configuration key for the Gravitino server URI. */
  public static final String FS_GRAVITINO_SERVER_URI_KEY = "fs.gravitino.server.uri";

  /** The configuration key for the Gravitino client Metalake. */
  public static final String FS_GRAVITINO_CLIENT_METALAKE_KEY = "fs.gravitino.client.metalake";

  /** The configuration key for the Gravitino client auth type. */
  public static final String FS_GRAVITINO_CLIENT_AUTH_TYPE_KEY = "fs.gravitino.client.authType";

  /** The authentication type for simple authentication. */
  public static final String SIMPLE_AUTH_TYPE = "simple";

  /** The authentication type for oauth2 authentication. */
  public static final String OAUTH2_AUTH_TYPE = "oauth2";

  /** The authentication type for kerberos authentication. */
  public static final String KERBEROS_AUTH_TYPE = "kerberos";

  // oauth2
  /** The configuration key prefix for oauth2 */
  public static final String FS_GRAVITINO_CLIENT_OAUTH2_PREFIX = "fs.gravitino.client.oauth2.";

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

  /** The configuration key prefix for kerberos */
  public static final String FS_GRAVITINO_CLIENT_KERBEROS_PREFIX = "fs.gravitino.client.kerberos.";

  /** The configuration key for the principal. */
  public static final String FS_GRAVITINO_CLIENT_KERBEROS_PRINCIPAL_KEY =
      "fs.gravitino.client.kerberos.principal";

  /** The configuration key for the keytab file path corresponding to the principal. */
  public static final String FS_GRAVITINO_CLIENT_KERBEROS_KEYTAB_FILE_PATH_KEY =
      "fs.gravitino.client.kerberos.keytabFilePath";

  /** The configuration key for the maximum capacity of the Gravitino fileset cache. */
  public static final String FS_GRAVITINO_FILESET_CACHE_MAX_CAPACITY_KEY =
      "fs.gravitino.fileset.cache.maxCapacity";

  /**
   * The default value for the maximum capacity of the Gravitino fileset cache. The default value is
   * 20.
   */
  public static final int FS_GRAVITINO_FILESET_CACHE_MAX_CAPACITY_DEFAULT = 20;

  /**
   * The configuration key for the eviction time of the Gravitino fileset cache, measured in mills
   * after access.
   */
  public static final String FS_GRAVITINO_FILESET_CACHE_EVICTION_MILLS_AFTER_ACCESS_KEY =
      "fs.gravitino.fileset.cache.evictionMillsAfterAccess";

  /**
   * The default value for the eviction time of the Gravitino fileset cache, measured in mills after
   * access.
   */
  public static final long FS_GRAVITINO_FILESET_CACHE_EVICTION_MILLS_AFTER_ACCESS_DEFAULT =
      1000L * 60 * 60;

  /**
   * The configuration key for the fileset with multiple locations, on which the file system will
   * operate. If not set, the file system will operate on the default location.
   */
  public static final String FS_GRAVITINO_CURRENT_LOCATION_NAME =
      "fs.gravitino.current.location.name";

  /**
   * The configuration key for the env variable name that indicates the current location name. If
   * not set, the file system will read the location name from CURRENT_LOCATION_NAME env variable.
   */
  public static final String FS_GRAVITINO_CURRENT_LOCATION_NAME_ENV_VAR =
      "fs.gravitino.current.location.name.env.var";

  /** The default env variable to read from to get current location name. */
  public static final String FS_GRAVITINO_CURRENT_LOCATION_NAME_ENV_VAR_DEFAULT =
      "CURRENT_LOCATION_NAME";

  /** The configuration key for the GVFS operations class. */
  public static final String FS_GRAVITINO_OPERATIONS_CLASS = "fs.gravitino.operations.class";

  /** The default value for the GVFS operations class. */
  public static final String FS_GRAVITINO_OPERATIONS_CLASS_DEFAULT =
      DefaultGVFSOperations.class.getCanonicalName();

  /** The configuration key for the block size of the GVFS file. */
  public static final String FS_GRAVITINO_BLOCK_SIZE = "fs.gravitino.block.size";

  /** The default block size of the GVFS file. */
  public static final long FS_GRAVITINO_BLOCK_SIZE_DEFAULT = 32 * 1024 * 1024;

  /** The configuration key for the Gravitino hook class. */
  public static final String FS_GRAVITINO_HOOK_CLASS = "fs.gravitino.hook.class";

  /** The default value for the Gravitino hook class. */
  public static final String FS_GRAVITINO_HOOK_CLASS_DEFAULT = NoOpHook.class.getCanonicalName();

  /** The configuration key prefix for the Gravitino client request header. */
  public static final String FS_GRAVITINO_CLIENT_REQUEST_HEADER_PREFIX =
      "fs.gravitino.client.request.header.";

  /** The configuration key for whether to enable credential vending. The default is false. */
  public static final String FS_GRAVITINO_ENABLE_CREDENTIAL_VENDING =
      "fs.gravitino.enableCredentialVending";

  /** The configuration key prefix for the Gravitino client config. */
  public static final String FS_GRAVITINO_CLIENT_CONFIG_PREFIX = "fs.gravitino.client.";

  /** The default value for whether to enable credential vending. */
  public static final boolean FS_GRAVITINO_ENABLE_CREDENTIAL_VENDING_DEFAULT = false;

  /** The configuration key list which not a Gravitino client config */
  public static final List<String> NOT_GRAVITINO_CLIENT_CONFIG_LIST =
      ImmutableList.of(FS_GRAVITINO_CLIENT_METALAKE_KEY, FS_GRAVITINO_CLIENT_AUTH_TYPE_KEY);

  /**
   * The configuration key for whether to enable fileset and catalog cache. The default is false.
   * Note that this cache causes a side effect: if you modify the fileset or fileset catalog
   * metadata, the client can not see the latest changes.
   */
  public static final String FS_GRAVITINO_FILESET_METADATA_CACHE_ENABLE =
      "fs.gravitino.filesetMetadataCache.cache.enable";

  /** The default value for whether to enable fileset and catalog cache. */
  public static final boolean FS_GRAVITINO_FILESET_METADATA_CACHE_ENABLE_DEFAULT = false;

  private GravitinoVirtualFileSystemConfiguration() {}
}
