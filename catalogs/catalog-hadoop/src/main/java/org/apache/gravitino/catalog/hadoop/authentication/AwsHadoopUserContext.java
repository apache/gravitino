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
package org.apache.gravitino.catalog.hadoop.authentication;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

import java.io.IOException;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.catalog.hadoop.authentication.aws.AwsConfig;
import org.apache.gravitino.utils.PrincipalUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;

public class AwsHadoopUserContext extends UserContext {
  private static final String CREDENTIALS_PROVIDER_PROPERTY = "fs.s3a.aws.credentials.provider";
  private static final String ACCESS_KEY_PROPERTY = "fs.s3a.access.key";
  private static final String SECRET_KEY_PROPERTY = "fs.s3a.secret.key";
  private static final String SESSION_TOKEN_PROPERTY = "fs.s3a.session.token";
  private static final String ENDPOINT_PROPERTY = "fs.s3a.endpoint";
  private static final String ANONYMOUS_CREDENTIALS_PROVIDER =
      "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider";
  private static final String TEMPORARY_CREDENTIALS_PROVIDER =
      "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider";
  private static final String SIMPLE_CREDENTIALS_PROVIDER =
      "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider";
  private final UserGroupInformation userGroupInformation;
  private final boolean enableUserImpersonation;

  AwsHadoopUserContext(UserGroupInformation userGroupInformation, boolean enableUserImpersonation) {
    this.userGroupInformation =
        requireNonNull(userGroupInformation, "userGroupInformation cannot be null");
    this.enableUserImpersonation = enableUserImpersonation;
  }

  synchronized void init(Map<String, String> properties, Configuration configuration) {
    AwsConfig awsConfig = new AwsConfig(properties);
    checkArgument(configuration != null, "hadoop conf cannot be null");

    String endpoint = awsConfig.getEndpoint();
    checkArgument(StringUtils.isNotBlank(endpoint), "aws endpoint cannot be blank");
    configuration.set(ENDPOINT_PROPERTY, endpoint);

    String credentialsProvider = awsConfig.getCredentialsProvider();
    if ("anonymous".equalsIgnoreCase(credentialsProvider)) {
      configuration.set(CREDENTIALS_PROVIDER_PROPERTY, ANONYMOUS_CREDENTIALS_PROVIDER);
    } else if ("token".equalsIgnoreCase(credentialsProvider)) {
      String accessKey = awsConfig.getAccessKey();
      checkArgument(StringUtils.isNotBlank(accessKey), "aws access key cannot be blank");
      String secretKey = awsConfig.getSecretKey();
      checkArgument(StringUtils.isNotBlank(secretKey), "aws secret key cannot be blank");
      String sessionToken = awsConfig.getSessionToken();
      checkArgument(StringUtils.isNotBlank(sessionToken), "aws session token cannot be blank");
      configuration.set(CREDENTIALS_PROVIDER_PROPERTY, TEMPORARY_CREDENTIALS_PROVIDER);
      configuration.set(ACCESS_KEY_PROPERTY, accessKey);
      configuration.set(SECRET_KEY_PROPERTY, secretKey);
      configuration.set(SESSION_TOKEN_PROPERTY, sessionToken);
    } else if ("simple".equals(credentialsProvider)) {
      String accessKey = awsConfig.getAccessKey();
      checkArgument(StringUtils.isNotBlank(accessKey), "aws access key cannot be blank");
      String secretKey = awsConfig.getSecretKey();
      checkArgument(StringUtils.isNotBlank(secretKey), "aws secret key cannot be blank");
      configuration.set(CREDENTIALS_PROVIDER_PROPERTY, SIMPLE_CREDENTIALS_PROVIDER);
      configuration.set(ACCESS_KEY_PROPERTY, accessKey);
      configuration.set(SECRET_KEY_PROPERTY, secretKey);
    } else {
      throw new IllegalArgumentException(
          "Unsupported credentials provider: " + credentialsProvider);
    }
  }

  @Override
  UserGroupInformation getUser() {
    return userGroupInformation;
  }

  @Override
  boolean enableUserImpersonation() {
    return enableUserImpersonation;
  }

  @Override
  UserGroupInformation createProxyUser() {
    return UserGroupInformation.createProxyUser(PrincipalUtils.getCurrentUserName(), getUser());
  }

  @Override
  public void close() throws IOException {}

  public AwsHadoopUserContext deepCopy() {
    return new AwsHadoopUserContext(userGroupInformation, enableUserImpersonation);
  }
}
