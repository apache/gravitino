/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.gravitino.credential;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;

/** Generic AWS IRSA credential. */
public class AwsIrsaCredential implements Credential {
  /** The credential type for AWS IRSA credentials. */
  public static final String AWS_IRSA_CREDENTIAL_TYPE = "aws-irsa";
  /** The key for AWS access key ID in credential info. */
  public static final String ACCESS_KEY_ID = "access-key-id";
  /** The key for AWS secret access key in credential info. */
  public static final String SECRET_ACCESS_KEY = "secret-access-key";
  /** The key for AWS session token in credential info. */
  public static final String SESSION_TOKEN = "session-token";

  private String accessKeyId;
  private String secretAccessKey;
  private String sessionToken;
  private long expireTimeInMs;

  /**
   * Constructs an AWS IRSA credential with the specified parameters.
   *
   * @param accessKeyId the AWS access key ID
   * @param secretAccessKey the AWS secret access key
   * @param sessionToken the AWS session token
   * @param expireTimeInMs the expiration time in milliseconds
   */
  public AwsIrsaCredential(
      String accessKeyId, String secretAccessKey, String sessionToken, long expireTimeInMs) {
    validate(accessKeyId, secretAccessKey, sessionToken);
    this.accessKeyId = accessKeyId;
    this.secretAccessKey = secretAccessKey;
    this.sessionToken = sessionToken;
    this.expireTimeInMs = expireTimeInMs;
  }

  /** Default constructor for AWS IRSA credential. */
  public AwsIrsaCredential() {}

  @Override
  public String credentialType() {
    return AWS_IRSA_CREDENTIAL_TYPE;
  }

  @Override
  public long expireTimeInMs() {
    return expireTimeInMs;
  }

  @Override
  public Map<String, String> credentialInfo() {
    ImmutableMap.Builder<String, String> builder = new ImmutableMap.Builder<>();
    builder.put(ACCESS_KEY_ID, accessKeyId);
    builder.put(SECRET_ACCESS_KEY, secretAccessKey);
    builder.put(SESSION_TOKEN, sessionToken);
    return builder.build();
  }

  @Override
  public void initialize(Map<String, String> credentialInfo, long expireTimeInMs) {
    String accessKeyId = credentialInfo.get(ACCESS_KEY_ID);
    String secretAccessKey = credentialInfo.get(SECRET_ACCESS_KEY);
    String sessionToken = credentialInfo.get(SESSION_TOKEN);
    validate(accessKeyId, secretAccessKey, sessionToken);
    this.accessKeyId = accessKeyId;
    this.secretAccessKey = secretAccessKey;
    this.sessionToken = sessionToken;
    this.expireTimeInMs = expireTimeInMs;
  }

  /**
   * Returns the AWS access key ID.
   *
   * @return the access key ID
   */
  public String accessKeyId() {
    return accessKeyId;
  }

  /**
   * Returns the AWS secret access key.
   *
   * @return the secret access key
   */
  public String secretAccessKey() {
    return secretAccessKey;
  }

  /**
   * Returns the AWS session token.
   *
   * @return the session token
   */
  public String sessionToken() {
    return sessionToken;
  }

  private void validate(String accessKeyId, String secretAccessKey, String sessionToken) {
    Preconditions.checkArgument(
        StringUtils.isNotBlank(accessKeyId), "Access key Id should not be empty");
    Preconditions.checkArgument(
        StringUtils.isNotBlank(secretAccessKey), "Secret access key should not be empty");
    Preconditions.checkArgument(
        StringUtils.isNotBlank(sessionToken), "Session token should not be empty");
  }
}
