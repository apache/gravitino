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

/** Tencent Cloud COS STS token credential. */
public class COSTokenCredential implements Credential {

  /** COS STS token credential type. */
  public static final String COS_TOKEN_CREDENTIAL_TYPE = "cos-token";
  /** The session access key ID (a.k.a. TmpSecretId in Tencent Cloud) used to access COS data. */
  public static final String GRAVITINO_COS_SESSION_ACCESS_KEY_ID = "cos-access-key-id";
  /**
   * The session secret access key (a.k.a. TmpSecretKey in Tencent Cloud) used to access COS data.
   */
  public static final String GRAVITINO_COS_SESSION_SECRET_ACCESS_KEY = "cos-secret-access-key";
  /** The COS security token (a.k.a. SessionToken in Tencent Cloud). */
  public static final String GRAVITINO_COS_SESSION_TOKEN = "cos-security-token";

  private String accessKeyId;
  private String secretAccessKey;
  private String securityToken;
  private long expireTimeInMs;

  /**
   * Constructs an instance of {@link COSTokenCredential} with session secret keys and a security
   * token.
   *
   * @param accessKeyId The COS session access key ID.
   * @param secretAccessKey The COS session secret access key.
   * @param securityToken The COS security token.
   * @param expireTimeInMs The COS token expire time in ms.
   */
  public COSTokenCredential(
      String accessKeyId, String secretAccessKey, String securityToken, long expireTimeInMs) {
    validate(accessKeyId, secretAccessKey, securityToken, expireTimeInMs);
    this.accessKeyId = accessKeyId;
    this.secretAccessKey = secretAccessKey;
    this.securityToken = securityToken;
    this.expireTimeInMs = expireTimeInMs;
  }

  /**
   * This is the constructor that is used by credential factory to create an instance of credential
   * according to the credential information.
   */
  public COSTokenCredential() {}

  @Override
  public String credentialType() {
    return COS_TOKEN_CREDENTIAL_TYPE;
  }

  @Override
  public long expireTimeInMs() {
    return expireTimeInMs;
  }

  @Override
  public Map<String, String> credentialInfo() {
    return (new ImmutableMap.Builder<String, String>())
        .put(GRAVITINO_COS_SESSION_ACCESS_KEY_ID, accessKeyId)
        .put(GRAVITINO_COS_SESSION_SECRET_ACCESS_KEY, secretAccessKey)
        .put(GRAVITINO_COS_SESSION_TOKEN, securityToken)
        .build();
  }

  /**
   * Initialize the credential with the credential information.
   *
   * <p>This method is invoked to deserialize the credential in client side.
   *
   * @param credentialInfo The credential information from {@link #credentialInfo}.
   * @param expireTimeInMs The expire-time from {@link #expireTimeInMs()}.
   */
  @Override
  public void initialize(Map<String, String> credentialInfo, long expireTimeInMs) {
    String accessKeyId = credentialInfo.get(GRAVITINO_COS_SESSION_ACCESS_KEY_ID);
    String secretAccessKey = credentialInfo.get(GRAVITINO_COS_SESSION_SECRET_ACCESS_KEY);
    String securityToken = credentialInfo.get(GRAVITINO_COS_SESSION_TOKEN);
    validate(accessKeyId, secretAccessKey, securityToken, expireTimeInMs);
    this.accessKeyId = accessKeyId;
    this.secretAccessKey = secretAccessKey;
    this.securityToken = securityToken;
    this.expireTimeInMs = expireTimeInMs;
  }

  /**
   * Get COS session access key ID.
   *
   * @return The COS session access key ID.
   */
  public String accessKeyId() {
    return accessKeyId;
  }

  /**
   * Get COS session secret access key.
   *
   * @return The COS session secret access key.
   */
  public String secretAccessKey() {
    return secretAccessKey;
  }

  /**
   * Get COS security token.
   *
   * @return The COS security token.
   */
  public String securityToken() {
    return securityToken;
  }

  private void validate(
      String accessKeyId, String secretAccessKey, String securityToken, long expireTimeInMs) {
    Preconditions.checkArgument(
        StringUtils.isNotBlank(accessKeyId), "COS access key Id should not be empty");
    Preconditions.checkArgument(
        StringUtils.isNotBlank(secretAccessKey), "COS secret access key should not be empty");
    Preconditions.checkArgument(
        StringUtils.isNotBlank(securityToken), "COS security token should not be empty");
    Preconditions.checkArgument(
        expireTimeInMs > 0, "The expiration time of COSTokenCredential should be greater than 0");
  }
}
