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

/** Tencent Cloud COS secret key credential. */
public class COSSecretKeyCredential implements Credential {

  /** COS secret key credential type. */
  public static final String COS_SECRET_KEY_CREDENTIAL_TYPE = "cos-secret-key";
  /** The static access key ID (a.k.a. SecretId in Tencent Cloud) used to access COS data. */
  public static final String GRAVITINO_COS_STATIC_ACCESS_KEY_ID = "cos-access-key-id";
  /** The static secret access key (a.k.a. SecretKey in Tencent Cloud) used to access COS data. */
  public static final String GRAVITINO_COS_STATIC_SECRET_ACCESS_KEY = "cos-secret-access-key";

  private String accessKeyId;
  private String secretAccessKey;

  /**
   * Constructs an instance of {@link COSSecretKeyCredential} with the static COS access key ID and
   * secret access key.
   *
   * @param accessKeyId The COS static access key ID.
   * @param secretAccessKey The COS static secret access key.
   */
  public COSSecretKeyCredential(String accessKeyId, String secretAccessKey) {
    validate(accessKeyId, secretAccessKey, 0);
    this.accessKeyId = accessKeyId;
    this.secretAccessKey = secretAccessKey;
  }

  /**
   * This is the constructor that is used by credential factory to create an instance of credential
   * according to the credential information.
   */
  public COSSecretKeyCredential() {}

  @Override
  public String credentialType() {
    return COS_SECRET_KEY_CREDENTIAL_TYPE;
  }

  @Override
  public long expireTimeInMs() {
    return 0;
  }

  @Override
  public Map<String, String> credentialInfo() {
    return (new ImmutableMap.Builder<String, String>())
        .put(GRAVITINO_COS_STATIC_ACCESS_KEY_ID, accessKeyId)
        .put(GRAVITINO_COS_STATIC_SECRET_ACCESS_KEY, secretAccessKey)
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
    String accessKeyId = credentialInfo.get(GRAVITINO_COS_STATIC_ACCESS_KEY_ID);
    String secretAccessKey = credentialInfo.get(GRAVITINO_COS_STATIC_SECRET_ACCESS_KEY);
    validate(accessKeyId, secretAccessKey, expireTimeInMs);
    this.accessKeyId = accessKeyId;
    this.secretAccessKey = secretAccessKey;
  }

  /**
   * Get COS static access key ID.
   *
   * @return The COS access key ID.
   */
  public String accessKeyId() {
    return accessKeyId;
  }

  /**
   * Get COS static secret access key.
   *
   * @return The COS secret access key.
   */
  public String secretAccessKey() {
    return secretAccessKey;
  }

  private void validate(String accessKeyId, String secretAccessKey, long expireTimeInMs) {
    Preconditions.checkArgument(
        StringUtils.isNotBlank(accessKeyId), "COS access key ID should not be empty");
    Preconditions.checkArgument(
        StringUtils.isNotBlank(secretAccessKey), "COS secret access key should not be empty");
    Preconditions.checkArgument(
        expireTimeInMs == 0, "The expiration time of COSSecretKeyCredential is not 0");
  }
}
