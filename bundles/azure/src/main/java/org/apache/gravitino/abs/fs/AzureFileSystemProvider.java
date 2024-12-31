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

package org.apache.gravitino.abs.fs;

import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_ACCOUNT_AUTH_TYPE_PROPERTY_NAME;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_ACCOUNT_IS_HNS_ENABLED;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_SAS_TOKEN_PROVIDER_TYPE;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.Map;
import javax.annotation.Nonnull;
import org.apache.gravitino.catalog.hadoop.fs.FileSystemProvider;
import org.apache.gravitino.catalog.hadoop.fs.FileSystemUtils;
import org.apache.gravitino.filesystem.hadoop.GravitinoVirtualFileSystemConfiguration;
import org.apache.gravitino.storage.AzureProperties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.services.AuthType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AzureFileSystemProvider implements FileSystemProvider {

  private static final Logger LOGGER = LoggerFactory.getLogger(AzureFileSystemProvider.class);

  @VisibleForTesting public static final String ABS_PROVIDER_SCHEME = "abfss";

  @VisibleForTesting public static final String ABS_PROVIDER_NAME = "abs";

  private static final String ABFS_IMPL = "org.apache.hadoop.fs.azurebfs.SecureAzureBlobFileSystem";

  private static final String ABFS_IMPL_KEY = "fs.abfss.impl";

  @Override
  public FileSystem getFileSystem(@Nonnull Path path, @Nonnull Map<String, String> config)
      throws IOException {
    Configuration configuration = new Configuration();

    Map<String, String> hadoopConfMap =
        FileSystemUtils.toHadoopConfigMap(config, ImmutableMap.of());

    if (config.containsKey(AzureProperties.GRAVITINO_AZURE_STORAGE_ACCOUNT_NAME)
        && config.containsKey(AzureProperties.GRAVITINO_AZURE_STORAGE_ACCOUNT_KEY)) {
      hadoopConfMap.put(
          String.format(
              "fs.azure.account.key.%s.dfs.core.windows.net",
              config.get(AzureProperties.GRAVITINO_AZURE_STORAGE_ACCOUNT_NAME)),
          config.get(AzureProperties.GRAVITINO_AZURE_STORAGE_ACCOUNT_KEY));
    }

    if (!config.containsKey(ABFS_IMPL_KEY)) {
      configuration.set(ABFS_IMPL_KEY, ABFS_IMPL);
    }

    hadoopConfMap.forEach(configuration::set);

    // Check whether this is from GVFS client.
    if (config.containsKey(GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_SERVER_URI_KEY)) {
      // Test whether SAS works
      try {
        AzureSasCredentialProvider azureSasCredentialProvider = new AzureSasCredentialProvider();
        azureSasCredentialProvider.initialize(configuration, null);
        String sas = azureSasCredentialProvider.getSASToken(null, null, null, null);
        if (sas != null) {
          String accountName =
              String.format(
                  "%s.dfs.core.windows.net",
                  config.get(AzureProperties.GRAVITINO_AZURE_STORAGE_ACCOUNT_NAME));

          configuration.set(
              FS_AZURE_ACCOUNT_AUTH_TYPE_PROPERTY_NAME + "." + accountName, AuthType.SAS.name());
          configuration.set(
              FS_AZURE_SAS_TOKEN_PROVIDER_TYPE + "." + accountName,
              AzureSasCredentialProvider.class.getName());
          configuration.set(FS_AZURE_ACCOUNT_IS_HNS_ENABLED, "true");
        } else if (azureSasCredentialProvider.getAzureStorageAccountKey() != null
            && azureSasCredentialProvider.getAzureStorageAccountName() != null) {
          configuration.set(
              String.format(
                  "fs.azure.account.key.%s.dfs.core.windows.net",
                  azureSasCredentialProvider.getAzureStorageAccountName()),
              azureSasCredentialProvider.getAzureStorageAccountKey());
        }
      } catch (Exception e) {
        // Can't use SAS, use account key and account key instead
        LOGGER.warn(
            "Failed to use SAS token and user account from credential provider, use default conf. ",
            e);
      }
    }

    return FileSystem.get(path.toUri(), configuration);
  }

  @Override
  public String scheme() {
    return ABS_PROVIDER_SCHEME;
  }

  @Override
  public String name() {
    return ABS_PROVIDER_NAME;
  }
}
