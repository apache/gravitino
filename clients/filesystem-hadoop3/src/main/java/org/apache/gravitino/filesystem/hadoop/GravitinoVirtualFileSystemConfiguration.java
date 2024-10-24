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

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.gravitino.catalog.hadoop.fs.FileSystemProvider;
import org.apache.gravitino.storage.GCSProperties;
import org.apache.gravitino.storage.OSSProperties;
import org.apache.gravitino.storage.S3Properties;

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

  /**
   * File system provider names configuration key. The value is a comma separated list of file
   * system provider name which is defined in the service loader. Users can custom their own file
   * system by implementing the {@link FileSystemProvider} interface.
   */
  public static final String FS_FILESYSTEM_PROVIDERS = "fs.gvfs.filesystem.providers";

  /** The authentication type for simple authentication. */
  public static final String SIMPLE_AUTH_TYPE = "simple";
  /** The authentication type for oauth2 authentication. */
  public static final String OAUTH2_AUTH_TYPE = "oauth2";

  /** The authentication type for kerberos authentication. */
  public static final String KERBEROS_AUTH_TYPE = "kerberos";
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

  private static final Map<String, String> GVFS_S3_KEY_TO_HADOOP_KEY =
      ImmutableMap.of(
          formatPropertyKey(S3Properties.GRAVITINO_S3_ENDPOINT),
          "fs.s3a.endpoint",
          formatPropertyKey(S3Properties.GRAVITINO_S3_ACCESS_KEY_ID),
          "fs.s3a.access.key",
          formatPropertyKey(S3Properties.GRAVITINO_S3_SECRET_ACCESS_KEY),
          "fs.s3a.secret.key",
          formatPropertyKey(S3Properties.GRAVITINO_S3_CREDS_PROVIDER),
          "fs.s3a.aws.credentials.provider");

  private static final Map<String, String> GVFS_OSS_KEY_TO_HADOOP_KEY =
      ImmutableMap.of(
          formatPropertyKey(OSSProperties.GRAVITINO_OSS_ENDPOINT), "fs.oss.endpoint",
          formatPropertyKey(OSSProperties.GRAVITINO_OSS_ACCESS_KEY_ID), "fs.oss.accessKeyId",
          formatPropertyKey(OSSProperties.GRAVITINO_OSS_ACCESS_KEY_SECRET),
              "fs.oss.accessKeySecret");

  private static final Map<String, String> GVFS_GCS_KEY_TO_HADOOP_KEY =
      ImmutableMap.of(
          formatPropertyKey(GCSProperties.GCS_SERVICE_ACCOUNT_JSON_PATH),
          "fs.gs.auth.service.account.json.keyfile");

  /**
   * The mapping from Gravitino Virtual File System key to Hadoop key.
   *
   * <p>For example, if users want to set the OSS endpoint, they can use the key "oss.endpoint" in
   * the Gravitino Virtual File System configuration. The corresponding Hadoop key is
   * "fs.oss.endpoint".
   */
  public static final Map<String, String> GVFS_KEY_TO_HADOOP_KEY =
      ImmutableMap.<String, String>builder()
          .putAll(GVFS_S3_KEY_TO_HADOOP_KEY)
          .putAll(GVFS_OSS_KEY_TO_HADOOP_KEY)
          .putAll(GVFS_GCS_KEY_TO_HADOOP_KEY)
          .build();

  static String formatPropertyKey(String key) {
    return key.replace("-", ".");
  }

  private GravitinoVirtualFileSystemConfiguration() {}
}
