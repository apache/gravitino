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

import com.amazonaws.auth.AWSCredentialsProvider;
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
          S3Properties.GRAVITINO_S3_SECRET_ACCESS_KEY, Constants.SECRET_KEY);

  // We can't use Constants.AWS_CREDENTIALS_PROVIDER directly, as in 2.7, this key does not exist.
  private static final String S3_CREDENTIAL_KEY = "fs.s3a.aws.credentials.provider";
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

    Configuration configuration = FileSystemUtils.createConfiguration(hadoopConfMap);
    return S3AFileSystem.newInstance(path.toUri(), configuration);
  }

  @Override
  public Map<String, String> getFileSystemCredentialConf(Credential[] credentials) {
    Credential credential = S3Utils.getSuitableCredential(credentials);
    Map<String, String> result = Maps.newHashMap();
    if (credential instanceof S3SecretKeyCredential
        || credential instanceof S3TokenCredential
        || credential instanceof AwsIrsaCredential) {
      result.put(
          Constants.AWS_CREDENTIALS_PROVIDER, S3CredentialsProvider.class.getCanonicalName());
    }

    return result;
  }

  private void checkAndSetCredentialProvider(Map<String, String> configs) {
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
      try {
        Class<?> c = Class.forName(provider);
        if (AWSCredentialsProvider.class.isAssignableFrom(c)) {
          validProviders.add(provider);
        } else {
          LOG.warn(
              "Credential provider {} is not a subclass of AWSCredentialsProvider, skipping",
              provider);
        }
      } catch (Exception e) {
        LOG.warn(
            "Credential provider {} not found in the Hadoop runtime, falling back to default",
            provider);
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
}
