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

package org.apache.gravitino.s3.fs;

import static org.apache.gravitino.catalog.hadoop.fs.Constants.DEFAULT_CONNECTION_TIMEOUT;
import static org.apache.gravitino.catalog.hadoop.fs.Constants.DEFAULT_RETRY_LIMIT;
import static org.apache.gravitino.catalog.hadoop.fs.Constants.S3_DIRECTORY_MARKER_RETENTION;
import static org.apache.gravitino.catalog.hadoop.fs.Constants.S3_DIRECTORY_MARKER_RETENTION_DELETE;
import static org.apache.gravitino.catalog.hadoop.fs.Constants.S3_ESTABLISH_TIMEOUT;
import static org.apache.gravitino.catalog.hadoop.fs.Constants.S3_MAX_ERROR_RETRIES;
import static org.apache.gravitino.catalog.hadoop.fs.Constants.S3_RETRY_LIMIT;
import static org.apache.gravitino.catalog.hadoop.fs.Constants.S3_RETRY_THROTTLE_LIMIT;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.catalog.hadoop.fs.FileSystemProvider;
import org.apache.gravitino.catalog.hadoop.fs.FileSystemUtils;
import org.apache.gravitino.catalog.hadoop.fs.SupportsCredentialVending;
import org.apache.gravitino.credential.AwsIrsaCredential;
import org.apache.gravitino.credential.Credential;
import org.apache.gravitino.credential.S3SecretKeyCredential;
import org.apache.gravitino.credential.S3TokenCredential;
import org.apache.gravitino.storage.S3Properties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.Constants;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class S3FileSystemProvider implements FileSystemProvider, SupportsCredentialVending {

  private static final Logger LOG = LoggerFactory.getLogger(S3FileSystemProvider.class);

  @VisibleForTesting
  public static final Map<String, String> GRAVITINO_KEY_TO_S3_HADOOP_KEY =
      ImmutableMap.of(
          S3Properties.GRAVITINO_S3_ENDPOINT, Constants.ENDPOINT,
          S3Properties.GRAVITINO_S3_ACCESS_KEY_ID, Constants.ACCESS_KEY,
          S3Properties.GRAVITINO_S3_SECRET_ACCESS_KEY, Constants.SECRET_KEY,
          S3Properties.GRAVITINO_S3_PATH_STYLE_ACCESS, Constants.PATH_STYLE_ACCESS);

  // We can't use Constants.AWS_CREDENTIALS_PROVIDER directly, as in 2.7, this key does not exist.
  private static final String S3_CREDENTIAL_KEY = "fs.s3a.aws.credentials.provider";
  // Class names instead of class literals, so that loading this class does not need the AWS SDK
  // that is not in use. TestS3CredentialsProviderSelection checks that they match the classes.
  @VisibleForTesting
  static final String S3_CREDENTIALS_PROVIDER_V1 =
      "org.apache.gravitino.s3.fs.S3CredentialsProvider";

  @VisibleForTesting
  static final String S3_CREDENTIALS_PROVIDER_V2 =
      "org.apache.gravitino.s3.fs.S3CredentialsProviderV2";

  private static final String S3_SIMPLE_CREDENTIAL =
      "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider";

  @Override
  public FileSystem getFileSystem(Path path, Map<String, String> config) throws IOException {
    Map<String, String> hadoopConfMap =
        FileSystemUtils.toHadoopConfigMap(config, GRAVITINO_KEY_TO_S3_HADOOP_KEY);

    if (!hadoopConfMap.containsKey(S3_CREDENTIAL_KEY)) {
      hadoopConfMap.put(S3_CREDENTIAL_KEY, S3_SIMPLE_CREDENTIAL);
    }

    // Hadoop-aws 2 does not support IAMInstanceCredentialsProvider
    checkAndSetCredentialProvider(hadoopConfMap);

    hadoopConfMap = additionalS3Config(hadoopConfMap);

    Configuration configuration = FileSystemUtils.createCompatibleConfiguration(hadoopConfMap);
    return S3AFileSystem.newInstance(path.toUri(), configuration);
  }

  @Override
  public Map<String, String> getFileSystemCredentialConf(Credential[] credentials) {
    Credential credential = S3Utils.getSuitableCredential(credentials);
    Map<String, String> result = Maps.newHashMap();
    if (credential instanceof S3SecretKeyCredential
        || credential instanceof S3TokenCredential
        || credential instanceof AwsIrsaCredential) {
      // Hadoop 3.4+ is built on the AWS SDK v2 and Hadoop 3.3 on the AWS SDK v1.
      result.put(
          Constants.AWS_CREDENTIALS_PROVIDER,
          S3Utils.isHadoopAwsSdkV2() ? S3_CREDENTIALS_PROVIDER_V2 : S3_CREDENTIALS_PROVIDER_V1);
    }

    return result;
  }

  @VisibleForTesting
  void checkAndSetCredentialProvider(Map<String, String> configs) {
    String provides = configs.get(S3_CREDENTIAL_KEY);
    if (provides == null) {
      return;
    }

    Splitter splitter = Splitter.on(',').trimResults().omitEmptyStrings();
    Joiner joiner = Joiner.on(",").skipNulls();
    // Split the list of providers
    List<String> providers = splitter.splitToList(provides);
    List<String> validProviders = Lists.newArrayList();

    for (String provider : providers) {
      if (S3Utils.isRemappedByHadoop(provider)) {
        // Hadoop maps this AWS SDK v1 provider name to its AWS SDK v2 counterpart by itself.
        validProviders.add(provider);
        continue;
      }
      try {
        Class<?> c = Class.forName(provider);
        if (S3Utils.isCredentialsProvider(c)) {
          validProviders.add(provider);
        } else {
          LOG.warn(
              "Credential provider {} is not an AWS SDK v1 or v2 credentials provider, skipping",
              provider);
        }
      } catch (ClassNotFoundException e) {
        LOG.warn(
            "Credential provider {} not found in the Hadoop runtime, falling back to default",
            provider,
            e);
        configs.put(S3_CREDENTIAL_KEY, S3_SIMPLE_CREDENTIAL);
        return;
      } catch (Exception | LinkageError e) {
        // The class exists, but it or a class it depends on cannot be loaded or initialized, for
        // example because of an incompatible AWS SDK or Hadoop version on the classpath.
        LOG.warn(
            "Credential provider {} cannot be loaded in the Hadoop runtime, falling back to default",
            provider,
            e);
        configs.put(S3_CREDENTIAL_KEY, S3_SIMPLE_CREDENTIAL);
        return;
      }
    }

    if (validProviders.isEmpty()) {
      configs.put(S3_CREDENTIAL_KEY, S3_SIMPLE_CREDENTIAL);
    } else {
      configs.put(S3_CREDENTIAL_KEY, joiner.join(validProviders));
    }
  }

  /**
   * Get the scheme of the FileSystem. Attention, for S3 the schema is "s3a", not "s3". Users should
   * use "s3a://..." to access S3.
   */
  @Override
  public String scheme() {
    return "s3a";
  }

  @Override
  public String name() {
    return "s3";
  }

  /**
   * Add additional S3 configurations to improve performance and reliability.
   *
   * @param configs Original configurations
   * @return Configurations with additional S3 settings
   */
  @VisibleForTesting
  Map<String, String> additionalS3Config(Map<String, String> configs) {
    Map<String, String> additionalConfigs = Maps.newHashMap(configs);

    // Avoid multiple retries to speed up failure in test cases.
    // Use hard code instead of Constants.MAX_ERROR_RETRIES to avoid dependency on a specific Hadoop
    // version
    if (!configs.containsKey(S3_MAX_ERROR_RETRIES)) {
      additionalConfigs.put(S3_MAX_ERROR_RETRIES, DEFAULT_RETRY_LIMIT);
    }

    if (!configs.containsKey(S3_ESTABLISH_TIMEOUT)) {
      additionalConfigs.put(S3_ESTABLISH_TIMEOUT, DEFAULT_CONNECTION_TIMEOUT);
    }

    if (!configs.containsKey(S3_RETRY_LIMIT)) {
      additionalConfigs.put(S3_RETRY_LIMIT, DEFAULT_RETRY_LIMIT);
    }

    if (!configs.containsKey(S3_RETRY_THROTTLE_LIMIT)) {
      additionalConfigs.put(S3_RETRY_THROTTLE_LIMIT, DEFAULT_RETRY_LIMIT);
    }

    // Keep deleting the directory markers like Hadoop 3.3 and earlier do, so that the objects
    // written by Gravitino stay readable by S3A clients that do not understand kept markers.
    if (!configs.containsKey(S3_DIRECTORY_MARKER_RETENTION)) {
      additionalConfigs.put(S3_DIRECTORY_MARKER_RETENTION, S3_DIRECTORY_MARKER_RETENTION_DELETE);
    }

    // More tuning can be added here.

    return ImmutableMap.copyOf(additionalConfigs);
  }
}
