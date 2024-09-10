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
package org.apache.gravitino.catalog.hadoop.storage.s3;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;

public class HadoopS3Initializer {
  private static final String FS_S3A_PROPERTY = "fs.s3a.impl";
  private static final String FS_S3A_PROPERTY_VALUE = "org.apache.hadoop.fs.s3a.S3AFileSystem";
  private static final String FS_S3A_ABSTRACT_PROPERTY = "fs.AbstractFileSystem.s3a.impl";
  private static final String FS_S3A_ABSTRACT_PROPERTY_VALUE = "org.apache.hadoop.fs.s3a.S3A";
  private static final String SSL_PROPERTY = "fs.s3a.connection.ssl.enabled";
  private static final String PATH_STYLE_PROPERTY = "fs.s3a.path.style.access";
  private static final String CREDENTIALS_PROVIDER_PROPERTY = "fs.s3a.aws.credentials.provider";
  private static final String ACCESS_KEY_PROPERTY = "fs.s3a.access.key";
  private static final String SECRET_KEY_PROPERTY = "fs.s3a.secret.key";
  private static final String SESSION_TOKEN_PROPERTY = "fs.s3a.session.token";
  private static final String ENDPOINT_PROPERTY = "fs.s3a.endpoint";
  private static final String REGION_PROPERTY = "fs.s3a.endpoint.region";
  private static final String ANONYMOUS_CREDENTIALS_PROVIDER =
      "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider";
  private static final String TEMPORARY_CREDENTIALS_PROVIDER =
      "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider";
  private static final String SIMPLE_CREDENTIALS_PROVIDER =
      "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider";

  public static Configuration initialize(Map<String, String> properties) {
    Configuration configuration = new Configuration();
    configuration.set(FS_S3A_PROPERTY, FS_S3A_PROPERTY_VALUE);
    configuration.set(FS_S3A_ABSTRACT_PROPERTY, FS_S3A_ABSTRACT_PROPERTY_VALUE);

    HadoopS3Config s3Config = new HadoopS3Config(properties);
    configuration.set(SSL_PROPERTY, s3Config.isSSLEnabled().toString());
    configuration.set(PATH_STYLE_PROPERTY, s3Config.isPathStyleEnabled().toString());
    String region = s3Config.getRegion();
    if (StringUtils.isBlank(region)) {
      String endpoint = s3Config.getEndpoint();
      checkArgument(StringUtils.isNotBlank(endpoint), "aws region or endpoint must be specified");
      configuration.set(ENDPOINT_PROPERTY, endpoint);
    } else {
      configuration.set(REGION_PROPERTY, region);
    }
    String credentialsProvider = s3Config.getCredentialsProvider();
    if ("anonymous".equalsIgnoreCase(credentialsProvider)) {
      configuration.set(CREDENTIALS_PROVIDER_PROPERTY, ANONYMOUS_CREDENTIALS_PROVIDER);
    } else if ("token".equalsIgnoreCase(credentialsProvider)) {
      String accessKey = s3Config.getAccessKey();
      checkArgument(StringUtils.isNotBlank(accessKey), "aws access key cannot be blank");
      String secretKey = s3Config.getSecretKey();
      checkArgument(StringUtils.isNotBlank(secretKey), "aws secret key cannot be blank");
      String sessionToken = s3Config.getSessionToken();
      checkArgument(StringUtils.isNotBlank(sessionToken), "aws session token cannot be blank");
      configuration.set(CREDENTIALS_PROVIDER_PROPERTY, TEMPORARY_CREDENTIALS_PROVIDER);
      configuration.set(ACCESS_KEY_PROPERTY, accessKey);
      configuration.set(SECRET_KEY_PROPERTY, secretKey);
      configuration.set(SESSION_TOKEN_PROPERTY, sessionToken);
    } else if ("simple".equals(credentialsProvider)) {
      String accessKey = s3Config.getAccessKey();
      checkArgument(StringUtils.isNotBlank(accessKey), "aws access key cannot be blank");
      String secretKey = s3Config.getSecretKey();
      checkArgument(StringUtils.isNotBlank(secretKey), "aws secret key cannot be blank");
      configuration.set(CREDENTIALS_PROVIDER_PROPERTY, SIMPLE_CREDENTIALS_PROVIDER);
      configuration.set(ACCESS_KEY_PROPERTY, accessKey);
      configuration.set(SECRET_KEY_PROPERTY, secretKey);
    } else {
      throw new IllegalArgumentException(
          "Unsupported credentials provider: " + credentialsProvider);
    }
    return configuration;
  }
}
