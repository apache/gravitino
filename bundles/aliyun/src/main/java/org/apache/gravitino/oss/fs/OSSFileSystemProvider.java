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
package org.apache.gravitino.oss.fs;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.Map;
import org.apache.gravitino.catalog.hadoop.fs.FileSystemProvider;
import org.apache.gravitino.catalog.hadoop.fs.FileSystemUtils;
import org.apache.gravitino.filesystem.common.GravitinoVirtualFileSystemConfiguration;
import org.apache.gravitino.storage.OSSProperties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.aliyun.oss.AliyunOSSFileSystem;
import org.apache.hadoop.fs.aliyun.oss.Constants;

public class OSSFileSystemProvider implements FileSystemProvider {

  private static final String OSS_FILESYSTEM_IMPL = "fs.oss.impl";

  // This map maintains the mapping relationship between the OSS properties in Gravitino and
  // the Hadoop properties. Through this map, users can customize the OSS properties in Gravitino
  // and map them to the corresponding Hadoop properties.
  // For example, User can use oss-endpoint to set the endpoint of OSS 'fs.oss.endpoint' in
  // Gravitino.
  // GCS and S3 also have similar mapping relationship.

  @VisibleForTesting
  public static final Map<String, String> GRAVITINO_KEY_TO_OSS_HADOOP_KEY =
      ImmutableMap.of(
          OSSProperties.GRAVITINO_OSS_ENDPOINT, Constants.ENDPOINT_KEY,
          OSSProperties.GRAVITINO_OSS_ACCESS_KEY_ID, Constants.ACCESS_KEY_ID,
          OSSProperties.GRAVITINO_OSS_ACCESS_KEY_SECRET, Constants.ACCESS_KEY_SECRET);

  @Override
  public FileSystem getFileSystem(Path path, Map<String, String> config) throws IOException {
    Configuration configuration = new Configuration();

    Map<String, String> hadoopConfMap =
        FileSystemUtils.toHadoopConfigMap(config, GRAVITINO_KEY_TO_OSS_HADOOP_KEY);
    // OSS do not use service loader to load the file system, so we need to set the impl class
    if (!hadoopConfMap.containsKey(OSS_FILESYSTEM_IMPL)) {
      hadoopConfMap.put(OSS_FILESYSTEM_IMPL, AliyunOSSFileSystem.class.getCanonicalName());
    }

    //    if (shouldSetCredentialsProviderExplicitly(config)) {
    //      hadoopConfMap.put(
    //          Constants.CREDENTIALS_PROVIDER_KEY,
    // OSSCredentialsProvider.class.getCanonicalName());
    //    }

    if (enableCredentialProvidedByGravitino(config)) {
      hadoopConfMap.put(
          Constants.CREDENTIALS_PROVIDER_KEY, TestOSSCredentialProvider.class.getCanonicalName());
    }

    hadoopConfMap.forEach(configuration::set);

    return AliyunOSSFileSystem.newInstance(path.toUri(), configuration);
  }

  private boolean enableCredentialProvidedByGravitino(Map<String, String> config) {
    return null != config.get("fs.gvfs.provider.impl");
  }

  /**
   * Check if the credential provider should be set explicitly.
   *
   * <p>When the credential provider is not set and the server URI is set (this means the call is
   * from GVFS client), we need to manually set the credential provider
   *
   * @param config the configuration map
   * @return true if the credential provider should be set explicitly
   */
  private boolean shouldSetCredentialsProviderExplicitly(Map<String, String> config) {
    return !config.containsKey(Constants.CREDENTIALS_PROVIDER_KEY)
        && config.containsKey(GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_SERVER_URI_KEY);
  }

  @Override
  public String scheme() {
    return "oss";
  }

  @Override
  public String name() {
    return "oss";
  }
}
