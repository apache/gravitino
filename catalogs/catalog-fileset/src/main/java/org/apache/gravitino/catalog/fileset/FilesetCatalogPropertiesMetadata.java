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
package org.apache.gravitino.catalog.fileset;

import static org.apache.gravitino.catalog.fileset.authentication.kerberos.KerberosConfig.KERBEROS_PROPERTY_ENTRIES;
import static org.apache.gravitino.catalog.hadoop.fs.Constants.BUILTIN_LOCAL_FS_PROVIDER;
import static org.apache.gravitino.file.Fileset.LOCATION_NAME_UNKNOWN;
import static org.apache.gravitino.file.Fileset.PROPERTY_MULTIPLE_LOCATIONS_PREFIX;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.gravitino.catalog.fileset.authentication.AuthenticationConfig;
import org.apache.gravitino.catalog.hadoop.fs.FileSystemProvider;
import org.apache.gravitino.catalog.hadoop.fs.LocalFileSystemProvider;
import org.apache.gravitino.connector.BaseCatalogPropertiesMetadata;
import org.apache.gravitino.connector.PropertyEntry;
import org.apache.gravitino.credential.config.CredentialConfig;

public class FilesetCatalogPropertiesMetadata extends BaseCatalogPropertiesMetadata {

  // Property "location" is used to specify the storage location managed by fileset catalog.
  // If specified, the location will be used as the default storage location for all
  // managed filesets created under this catalog.
  //
  // If not, users have to specify the storage location in the Schema or Fileset level.
  public static final String LOCATION = "location";

  /**
   * The name of {@link FileSystemProvider} to be added to the catalog. Except built-in
   * FileSystemProvider like LocalFileSystemProvider and HDFSFileSystemProvider, users can add their
   * own FileSystemProvider by specifying the provider name here. The value can be find {@link
   * FileSystemProvider#name()}.
   */
  public static final String FILESYSTEM_PROVIDERS = "filesystem-providers";

  /**
   * The default file system provider class name, used to create the default file system. If not
   * specified, the default file system provider will be {@link LocalFileSystemProvider#name()}:
   * 'builtin-local'.
   */
  public static final String DEFAULT_FS_PROVIDER = "default-filesystem-provider";

  /** The interval in milliseconds to evict the fileset cache. */
  public static final String FILESET_CACHE_EVICTION_INTERVAL_MS =
      "fileset-cache-eviction-interval-ms";

  /** The maximum number of the filesets the cache may contain. */
  public static final String FILESET_CACHE_MAX_SIZE = "fileset-cache-max-size";

  /** The value to indicate the cache value is not set. */
  public static final long CACHE_VALUE_NOT_SET = -1;

  static final String FILESYSTEM_CONNECTION_TIMEOUT_SECONDS = "filesystem-conn-timeout-secs";
  static final int DEFAULT_GET_FILESYSTEM_TIMEOUT_SECONDS = 6;

  /**
   * The property to disable file system operations like list, exists, mkdir operations in the
   * server side, so that the server side catalog can be used as a metadata only catalog, no need to
   * configure the file system access related configurations. By default, it is false.
   */
  static final String DISABLE_FILESYSTEM_OPS = "disable-filesystem-ops";

  static final boolean DEFAULT_DISABLE_FILESYSTEM_OPS = false;

  private static final Map<String, PropertyEntry<?>> FILESET_CATALOG_PROPERTY_ENTRIES =
      ImmutableMap.<String, PropertyEntry<?>>builder()
          .put(
              LOCATION,
              PropertyEntry.stringOptionalPropertyEntry(
                  LOCATION,
                  "The storage location managed by fileset catalog",
                  false /* immutable */,
                  null,
                  false /* hidden */))
          .put(
              PROPERTY_MULTIPLE_LOCATIONS_PREFIX + LOCATION_NAME_UNKNOWN,
              PropertyEntry.stringReservedPropertyEntry(
                  PROPERTY_MULTIPLE_LOCATIONS_PREFIX + LOCATION_NAME_UNKNOWN,
                  "The storage location equivalent to property 'location'",
                  true /* hidden */))
          .put(
              PROPERTY_MULTIPLE_LOCATIONS_PREFIX,
              PropertyEntry.stringImmutablePropertyPrefixEntry(
                  PROPERTY_MULTIPLE_LOCATIONS_PREFIX,
                  "The prefix of the location name",
                  false /* required */,
                  null /* default value */,
                  false /* hidden */,
                  false /* reserved */))
          .put(
              FILESYSTEM_PROVIDERS,
              PropertyEntry.stringOptionalPropertyEntry(
                  FILESYSTEM_PROVIDERS,
                  "The file system provider names, separated by comma",
                  false /* immutable */,
                  null,
                  false /* hidden */))
          .put(
              DEFAULT_FS_PROVIDER,
              PropertyEntry.stringOptionalPropertyEntry(
                  DEFAULT_FS_PROVIDER,
                  "Default file system provider name",
                  false /* immutable */,
                  BUILTIN_LOCAL_FS_PROVIDER, // please see LocalFileSystemProvider#name()
                  false /* hidden */))
          .put(
              FILESYSTEM_CONNECTION_TIMEOUT_SECONDS,
              PropertyEntry.integerOptionalPropertyEntry(
                  FILESYSTEM_CONNECTION_TIMEOUT_SECONDS,
                  "Timeout to wait for to create the HCFS file system client instance.",
                  false /* immutable */,
                  DEFAULT_GET_FILESYSTEM_TIMEOUT_SECONDS,
                  false /* hidden */))
          .put(
              FILESET_CACHE_EVICTION_INTERVAL_MS,
              PropertyEntry.longOptionalPropertyEntry(
                  FILESET_CACHE_EVICTION_INTERVAL_MS,
                  "The interval in milliseconds to evict the fileset cache, -1 means never evict.",
                  false /* immutable */,
                  60 * 60 * 1000L /* 1 hour */,
                  false /* hidden */))
          .put(
              FILESET_CACHE_MAX_SIZE,
              PropertyEntry.longOptionalPropertyEntry(
                  FILESET_CACHE_MAX_SIZE,
                  "The maximum number of the filesets the cache may contain, -1 means no limit.",
                  false /* immutable */,
                  200_000L,
                  false /* hidden */))
          .put(
              DISABLE_FILESYSTEM_OPS,
              PropertyEntry.booleanPropertyEntry(
                  DISABLE_FILESYSTEM_OPS,
                  "Disable file system operations in the server side",
                  false /* required */,
                  true /* immutable */,
                  DEFAULT_DISABLE_FILESYSTEM_OPS,
                  false /* hidden */,
                  false /* reserved */))
          // The following two are about authentication.
          .putAll(KERBEROS_PROPERTY_ENTRIES)
          .putAll(AuthenticationConfig.AUTHENTICATION_PROPERTY_ENTRIES)
          .putAll(CredentialConfig.CREDENTIAL_PROPERTY_ENTRIES)
          .build();

  @Override
  protected Map<String, PropertyEntry<?>> specificPropertyEntries() {
    return FILESET_CATALOG_PROPERTY_ENTRIES;
  }
}
