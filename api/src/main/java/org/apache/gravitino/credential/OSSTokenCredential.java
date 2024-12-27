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

/** OSS token credential. */
public class OSSTokenCredential implements Credential {

  /** OSS token credential type. */
  public static final String OSS_TOKEN_CREDENTIAL_TYPE = "oss-token";
  /** OSS access key ID used to access OSS data. */
  public static final String GRAVITINO_OSS_SESSION_ACCESS_KEY_ID = "oss-access-key-id";
  /** OSS secret access key used to access OSS data. */
  public static final String GRAVITINO_OSS_SESSION_SECRET_ACCESS_KEY = "oss-secret-access-key";
  /** OSS security token. */
  public static final String GRAVITINO_OSS_TOKEN = "oss-security-token";

  private String accessKeyId;
  private String secretAccessKey;
  private String securityToken;
  private long expireTimeInMS;

  /**
   * Constructs an instance of {@link OSSTokenCredential} with secret key and token.
   *
   * @param accessKeyId The oss access key ID.
   * @param secretAccessKey The oss secret access key.
   * @param securityToken The oss security token.
   * @param expireTimeInMS The oss token expire time in ms.
   */
  public OSSTokenCredential(
      String accessKeyId, String secretAccessKey, String securityToken, long expireTimeInMS) {
    validate(accessKeyId, secretAccessKey, securityToken, expireTimeInMS);
    this.accessKeyId = accessKeyId;
    this.secretAccessKey = secretAccessKey;
    this.securityToken = securityToken;
    this.expireTimeInMS = expireTimeInMS;
  }

  /**
   * This is the constructor that is used by credential factory to create an instance of credential
   * according to the credential information.
   */
  public OSSTokenCredential() {}

  @Override
  public String credentialType() {
    return OSS_TOKEN_CREDENTIAL_TYPE;
  }

  @Override
  public long expireTimeInMs() {
    return expireTimeInMS;
  }

  @Override
  public Map<String, String> credentialInfo() {
    return (new ImmutableMap.Builder<String, String>())
        .put(GRAVITINO_OSS_SESSION_ACCESS_KEY_ID, accessKeyId)
        .put(GRAVITINO_OSS_SESSION_SECRET_ACCESS_KEY, secretAccessKey)
        .put(GRAVITINO_OSS_TOKEN, securityToken)
        .build();
  }

  @Override
  public void initialize(Map<String, String> credentialInfo, long expireTimeInMs) {
    String accessKeyId = credentialInfo.get(GRAVITINO_OSS_SESSION_ACCESS_KEY_ID);
    String secretAccessKey = credentialInfo.get(GRAVITINO_OSS_SESSION_SECRET_ACCESS_KEY);
    String securityToken = credentialInfo.get(GRAVITINO_OSS_TOKEN);

    validate(accessKeyId, secretAccessKey, securityToken, expireTimeInMs);

    this.accessKeyId = accessKeyId;
    this.secretAccessKey = secretAccessKey;
    this.securityToken = securityToken;
    this.expireTimeInMS = expireTimeInMs;
  }

  /**
   * Get oss access key ID.
   *
   * @return The oss access key ID.
   */
  public String accessKeyId() {
    return accessKeyId;
  }

  /**
   * Get oss secret access key.
   *
   * @return The oss secret access key.
   */
  public String secretAccessKey() {
    return secretAccessKey;
  }

  /**
   * Get oss security token.
   *
   * @return The oss security token.
   */
  public String securityToken() {
    return securityToken;
  }

  private void validate(
      String accessKeyId, String secretAccessKey, String sessionToken, long expireTimeInMs) {
    Preconditions.checkArgument(
        StringUtils.isNotBlank(accessKeyId), "OSS access key Id should not be empty");
    Preconditions.checkArgument(
        StringUtils.isNotBlank(secretAccessKey), "OSS secret access key should not be empty");
    Preconditions.checkArgument(
        StringUtils.isNotBlank(sessionToken), "OSS session token should not be empty");
    Preconditions.checkArgument(
        expireTimeInMs > 0, "The expire time of OSSTokenCredential should great than 0");
  }
}
