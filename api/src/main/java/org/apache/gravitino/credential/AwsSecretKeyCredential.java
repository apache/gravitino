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
package org.apache.gravitino.credential;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;

/**
 * Static AWS access-key credential for Glue (and similar) API authentication.
 *
 * <p>Distinct from {@link S3SecretKeyCredential}: credential-info keys are {@code
 * aws-access-key-id} / {@code aws-secret-access-key}, matching Glue catalog properties.
 */
public class AwsSecretKeyCredential implements Credential {

  /** AWS secret-key credential type. */
  public static final String AWS_SECRET_KEY_CREDENTIAL_TYPE = "aws-secret-key";
  /** AWS access key ID. */
  public static final String GRAVITINO_AWS_ACCESS_KEY_ID = "aws-access-key-id";
  /** AWS secret access key. */
  public static final String GRAVITINO_AWS_SECRET_ACCESS_KEY = "aws-secret-access-key";

  private String accessKeyId;
  private String secretAccessKey;

  /**
   * Constructs an {@link AwsSecretKeyCredential}.
   *
   * @param accessKeyId the AWS access key ID
   * @param secretAccessKey the AWS secret access key
   */
  public AwsSecretKeyCredential(String accessKeyId, String secretAccessKey) {
    validate(accessKeyId, secretAccessKey, 0);
    this.accessKeyId = accessKeyId;
    this.secretAccessKey = secretAccessKey;
  }

  /** Used by the credential factory. */
  public AwsSecretKeyCredential() {}

  @Override
  public String credentialType() {
    return AWS_SECRET_KEY_CREDENTIAL_TYPE;
  }

  @Override
  public long expireTimeInMs() {
    return 0;
  }

  @Override
  public Map<String, String> credentialInfo() {
    return new ImmutableMap.Builder<String, String>()
        .put(GRAVITINO_AWS_ACCESS_KEY_ID, accessKeyId)
        .put(GRAVITINO_AWS_SECRET_ACCESS_KEY, secretAccessKey)
        .build();
  }

  @Override
  public void initialize(Map<String, String> credentialInfo, long expireTimeInMs) {
    String accessKeyId = credentialInfo.get(GRAVITINO_AWS_ACCESS_KEY_ID);
    String secretAccessKey = credentialInfo.get(GRAVITINO_AWS_SECRET_ACCESS_KEY);
    validate(accessKeyId, secretAccessKey, expireTimeInMs);
    this.accessKeyId = accessKeyId;
    this.secretAccessKey = secretAccessKey;
  }

  /**
   * Returns the AWS access key ID.
   *
   * @return access key ID
   */
  public String accessKeyId() {
    return accessKeyId;
  }

  /**
   * Returns the AWS secret access key.
   *
   * @return secret access key
   */
  public String secretAccessKey() {
    return secretAccessKey;
  }

  private void validate(String accessKeyId, String secretAccessKey, long expireTimeInMs) {
    Preconditions.checkArgument(
        StringUtils.isNotBlank(accessKeyId), "AWS access key ID should not be empty");
    Preconditions.checkArgument(
        StringUtils.isNotBlank(secretAccessKey), "AWS secret access key should not be empty");
    Preconditions.checkArgument(
        expireTimeInMs == 0, "The expiration time of AwsSecretKeyCredential should be 0");
  }
}
