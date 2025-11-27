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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.util.Map;
import org.apache.gravitino.catalog.hadoop.fs.FileSystemProvider;
import org.apache.gravitino.catalog.hadoop.fs.FileSystemUtils;
import org.apache.gravitino.catalog.hadoop.fs.SupportsCredentialVending;
import org.apache.gravitino.credential.Credential;
import org.apache.gravitino.credential.GCSTokenCredential;
import org.apache.gravitino.storage.GCSProperties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class GCSFileSystemProvider implements FileSystemProvider, SupportsCredentialVending {
  private static final String GCS_SERVICE_ACCOUNT_JSON_FILE =
      "fs.gs.auth.service.account.json.keyfile";
  private static final String GCS_TOKEN_PROVIDER_IMPL = "fs.gs.auth.access.token.provider.impl";

  @VisibleForTesting
  public static final Map<String, String> GRAVITINO_KEY_TO_GCS_HADOOP_KEY =
      ImmutableMap.of(
          GCSProperties.GRAVITINO_GCS_SERVICE_ACCOUNT_FILE, GCS_SERVICE_ACCOUNT_JSON_FILE);

  @Override
  public FileSystem getFileSystem(Path path, Map<String, String> config) throws IOException {
    Map<String, String> hadoopConfMap =
        FileSystemUtils.toHadoopConfigMap(config, GRAVITINO_KEY_TO_GCS_HADOOP_KEY);
    hadoopConfMap = additionalGCSConfig(hadoopConfMap);

    Configuration configuration = FileSystemUtils.createConfiguration(hadoopConfMap);
    return FileSystem.newInstance(path.toUri(), configuration);
  }

  @Override
  public Map<String, String> getFileSystemCredentialConf(Credential[] credentials) {
    Credential credential = GCSUtils.getGCSTokenCredential(credentials);
    Map<String, String> result = Maps.newHashMap();
    if (credential instanceof GCSTokenCredential) {
      result.put(GCS_TOKEN_PROVIDER_IMPL, GCSCredentialsProvider.class.getName());
    }

    return result;
  }

  private Map<String, String> additionalGCSConfig(Map<String, String> configs) {
    Map<String, String> additionalConfigs = Maps.newHashMap(configs);

    // Avoid multiple retries to speed up failure in test cases.
    // Use hard code instead of GoogleHadoopFileSystemBase.GCS_HTTP_CONNECT_TIMEOUT_KEY to avoid
    // dependency on a specific Hadoop version
    if (!configs.containsKey("fs.gs.http.connect-timeout")) {
      additionalConfigs.put("fs.gs.http.connect-timeout", "5000");
    }

    if (!configs.containsKey("fs.gs.http.max.retry")) {
      additionalConfigs.put("fs.gs.http.max.retry", "2");
    }

    // More tuning can be added here.

    return ImmutableMap.copyOf(additionalConfigs);
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
