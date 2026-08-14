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
package org.apache.gravitino.cos.fs;

import static org.apache.gravitino.catalog.hadoop.fs.Constants.COS_CONNECTION_TIMEOUT_KEY;
import static org.apache.gravitino.catalog.hadoop.fs.Constants.COS_MAX_RETRIES_KEY;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.util.Map;
import org.apache.gravitino.catalog.hadoop.fs.FileSystemProvider;
import org.apache.gravitino.catalog.hadoop.fs.FileSystemUtils;
import org.apache.gravitino.catalog.hadoop.fs.SupportsCredentialVending;
import org.apache.gravitino.credential.COSSecretKeyCredential;
import org.apache.gravitino.credential.Credential;
import org.apache.gravitino.storage.COSProperties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CosFileSystem;
import org.apache.hadoop.fs.CosNConfigKeys;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class COSFileSystemProvider implements FileSystemProvider, SupportsCredentialVending {

  private static final String COS_FILESYSTEM_IMPL = "fs.cosn.impl";

  // Default connection timeout (ms) used when caller does not specify one. Aligns with the
  // OSS bundle's "fail fast in tests" defaults rather than hadoop-cos's own 10s default.
  @VisibleForTesting static final String DEFAULT_COS_CONNECTION_TIMEOUT_MS = "5000";

  // Default max retries used when caller does not specify one. hadoop-cos's own default is 200,
  // which is too aggressive for non-production usage.
  @VisibleForTesting static final String DEFAULT_COS_MAX_RETRIES = "2";

  // Mapping from Gravitino property keys to the corresponding hadoop-cos
  // (CosNConfigKeys) keys. Mirrors OSSFileSystemProvider.GRAVITINO_KEY_TO_OSS_HADOOP_KEY.
  @VisibleForTesting
  public static final Map<String, String> GRAVITINO_KEY_TO_COS_HADOOP_KEY =
      ImmutableMap.of(
          COSProperties.GRAVITINO_COS_REGION, CosNConfigKeys.COSN_REGION_KEY,
          COSProperties.GRAVITINO_COS_ENDPOINT, CosNConfigKeys.COSN_ENDPOINT_SUFFIX_KEY,
          COSProperties.GRAVITINO_COS_ACCESS_KEY_ID, CosNConfigKeys.COSN_USERINFO_SECRET_ID_KEY,
          COSProperties.GRAVITINO_COS_ACCESS_KEY_SECRET,
              CosNConfigKeys.COSN_USERINFO_SECRET_KEY_KEY);

  @Override
  public FileSystem getFileSystem(Path path, Map<String, String> config) throws IOException {
    Map<String, String> hadoopConfMap =
        FileSystemUtils.toHadoopConfigMap(config, GRAVITINO_KEY_TO_COS_HADOOP_KEY);
    // hadoop-cos relies on fs.cosn.impl rather than ServiceLoader for FileSystem registration.
    if (!hadoopConfMap.containsKey(COS_FILESYSTEM_IMPL)) {
      hadoopConfMap.put(COS_FILESYSTEM_IMPL, CosFileSystem.class.getCanonicalName());
    }

    hadoopConfMap = additionalCOSConfig(hadoopConfMap);

    Configuration configuration = FileSystemUtils.createCompatibleConfiguration(hadoopConfMap);

    // CosFileSystem does not expose a `newInstance(URI, Configuration)` static helper the way
    // AliyunOSSFileSystem does, so we go through Hadoop's generic FileSystem.newInstance which
    // is the recommended path and honours `fs.cosn.impl`.
    return FileSystem.newInstance(path.toUri(), configuration);
  }

  @Override
  public Map<String, String> getFileSystemCredentialConf(Credential[] credentials) {
    Credential credential = COSUtils.getSuitableCredential(credentials);
    Map<String, String> result = Maps.newHashMap();
    if (credential instanceof COSSecretKeyCredential) {
      result.put(
          CosNConfigKeys.COSN_CREDENTIALS_PROVIDER,
          COSCredentialsProvider.class.getCanonicalName());
    }

    return result;
  }

  @Override
  public String scheme() {
    // hadoop-cos uses the `cosn://` scheme. We expose the same scheme to Gravitino users so
    // existing Hadoop tooling keeps working unchanged.
    return "cosn";
  }

  @Override
  public String name() {
    return "cos";
  }

  /**
   * Add additional COS configurations for better performance and reliability in test/dev
   * environments. Production deployments are free to override these via catalog properties.
   *
   * @param configs Original configurations
   * @return Configurations with additional COS settings
   */
  private Map<String, String> additionalCOSConfig(Map<String, String> configs) {
    Map<String, String> additionalConfigs = Maps.newHashMap(configs);

    if (!configs.containsKey(COS_CONNECTION_TIMEOUT_KEY)) {
      additionalConfigs.put(COS_CONNECTION_TIMEOUT_KEY, DEFAULT_COS_CONNECTION_TIMEOUT_MS);
    }

    if (!configs.containsKey(COS_MAX_RETRIES_KEY)) {
      additionalConfigs.put(COS_MAX_RETRIES_KEY, DEFAULT_COS_MAX_RETRIES);
    }

    return ImmutableMap.copyOf(additionalConfigs);
  }
}
