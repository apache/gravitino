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
package org.apache.gravitino.catalog.hadoop;

import static org.apache.gravitino.catalog.hadoop.authentication.kerberos.KerberosConfig.KERBEROS_PROPERTY_ENTRIES;
import static org.apache.gravitino.catalog.hadoop.fs.Constants.BUILTIN_LOCAL_FS_PROVIDER;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.gravitino.catalog.hadoop.authentication.AuthenticationConfig;
import org.apache.gravitino.catalog.hadoop.fs.FileSystemProvider;
import org.apache.gravitino.catalog.hadoop.fs.LocalFileSystemProvider;
import org.apache.gravitino.connector.BaseCatalogPropertiesMetadata;
import org.apache.gravitino.connector.PropertyEntry;
import org.apache.gravitino.credential.config.CredentialConfig;

public class HadoopCatalogPropertiesMetadata extends BaseCatalogPropertiesMetadata {

  // Property "location" is used to specify the storage location managed by Hadoop fileset catalog.
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

  static final String FILESYSTEM_CONNECTION_TIMEOUT_SECONDS = "filesystem-conn-timeout-secs";
  static final int DEFAULT_GET_FILESYSTEM_TIMEOUT_SECONDS = 6;

  private static final Map<String, PropertyEntry<?>> HADOOP_CATALOG_PROPERTY_ENTRIES =
      ImmutableMap.<String, PropertyEntry<?>>builder()
          .put(
              LOCATION,
              PropertyEntry.stringOptionalPropertyEntry(
                  LOCATION,
                  "The storage location managed by Hadoop fileset catalog",
                  false /* immutable */,
                  null,
                  false /* hidden */))
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
                  "Timeout to wait for to create the Hadoop file system client instance.",
                  false /* immutable */,
                  DEFAULT_GET_FILESYSTEM_TIMEOUT_SECONDS,
                  false /* hidden */))
          // The following two are about authentication.
          .putAll(KERBEROS_PROPERTY_ENTRIES)
          .putAll(AuthenticationConfig.AUTHENTICATION_PROPERTY_ENTRIES)
          .putAll(CredentialConfig.CREDENTIAL_PROPERTY_ENTRIES)
          .build();

  @Override
  protected Map<String, PropertyEntry<?>> specificPropertyEntries() {
    return HADOOP_CATALOG_PROPERTY_ENTRIES;
  }
}
