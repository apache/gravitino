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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.Map;
import javax.annotation.Nonnull;
import org.apache.gravitino.catalog.hadoop.fs.FileSystemProvider;
import org.apache.gravitino.catalog.hadoop.fs.FileSystemUtils;
import org.apache.gravitino.storage.AzureProperties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class AzureFileSystemProvider implements FileSystemProvider {

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
