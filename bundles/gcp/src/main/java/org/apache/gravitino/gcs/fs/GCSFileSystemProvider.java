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
package org.apache.gravitino.gcs.fs;

import com.google.cloud.hadoop.util.AccessTokenProvider;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.Map;
import org.apache.gravitino.catalog.hadoop.fs.FileSystemProvider;
import org.apache.gravitino.catalog.hadoop.fs.FileSystemUtils;
import org.apache.gravitino.filesystem.hadoop.GravitinoVirtualFileSystemConfiguration;
import org.apache.gravitino.storage.GCSProperties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class GCSFileSystemProvider implements FileSystemProvider {
  private static final String GCS_SERVICE_ACCOUNT_JSON_FILE =
      "fs.gs.auth.service.account.json.keyfile";

  @VisibleForTesting
  public static final Map<String, String> GRAVITINO_KEY_TO_GCS_HADOOP_KEY =
      ImmutableMap.of(
          GCSProperties.GRAVITINO_GCS_SERVICE_ACCOUNT_FILE, GCS_SERVICE_ACCOUNT_JSON_FILE);

  @Override
  public FileSystem getFileSystem(Path path, Map<String, String> config) throws IOException {
    Configuration configuration = new Configuration();
    FileSystemUtils.toHadoopConfigMap(config, GRAVITINO_KEY_TO_GCS_HADOOP_KEY)
        .forEach(configuration::set);

    if (config.containsKey(GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_SERVER_URI_KEY)) {
      AccessTokenProvider accessTokenProvider = new GravitinoGCSCredentialProvider();
      accessTokenProvider.setConf(configuration);
      // Why is this check necessary?, if Gravitino fails to get any credentials, we fall back to
      // the default behavior of the GoogleHadoopFileSystem to use service account credentials.
      if (accessTokenProvider.getAccessToken() != null) {
        configuration.set(
            "fs.gs.auth.access.token.provider.impl",
            GravitinoGCSCredentialProvider.class.getName());
      }
    }

    return FileSystem.newInstance(path.toUri(), configuration);
  }

  @Override
  public String scheme() {
    return "gs";
  }

  @Override
  public String name() {
    return "gcs";
  }
}
