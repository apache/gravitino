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
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;

/**
 * Static Alibaba Cloud DLF (Data Lake Formation) access-key credential for Paimon DLF catalogs.
 *
 * <p>Credential-info keys match Gravitino Paimon catalog properties: {@code dlf-access-key-id},
 * {@code dlf-access-key-secret}, and optionally {@code dlf-security-token}. Connectors map these to
 * Paimon REST keys {@code dlf.access-key-id}, {@code dlf.access-key-secret}, and {@code
 * dlf.security-token}.
 *
 * <p>{@code dlf-security-token} is passed through as-is. {@link #expireTimeInMs()} is always {@code
 * 0}; Gravitino does not refresh this STS token. For rotating DLF tokens, configure {@code
 * dlf-token-loader} / {@code dlf-token-path} on the Paimon catalog instead.
 */
public class DlfSecretKeyCredential implements Credential {

  /** DLF secret-key credential type. */
  public static final String DLF_SECRET_KEY_CREDENTIAL_TYPE = "dlf-secret-key";
  /** DLF access key ID. */
  public static final String GRAVITINO_DLF_ACCESS_KEY_ID = "dlf-access-key-id";
  /** DLF access key secret. */
  public static final String GRAVITINO_DLF_ACCESS_KEY_SECRET = "dlf-access-key-secret";
  /** Optional DLF security token. */
  public static final String GRAVITINO_DLF_SECURITY_TOKEN = "dlf-security-token";

  private String accessKeyId;
  private String accessKeySecret;
  @Nullable private String securityToken;

  /**
   * Constructs a {@link DlfSecretKeyCredential} without a security token.
   *
   * @param accessKeyId the DLF access key ID
   * @param accessKeySecret the DLF access key secret
   */
  public DlfSecretKeyCredential(String accessKeyId, String accessKeySecret) {
    this(accessKeyId, accessKeySecret, null);
  }

  /**
   * Constructs a {@link DlfSecretKeyCredential}.
   *
   * @param accessKeyId the DLF access key ID
   * @param accessKeySecret the DLF access key secret
   * @param securityToken optional security token
   */
  public DlfSecretKeyCredential(
      String accessKeyId, String accessKeySecret, @Nullable String securityToken) {
    validate(accessKeyId, accessKeySecret, 0);
    this.accessKeyId = accessKeyId;
    this.accessKeySecret = accessKeySecret;
    this.securityToken = securityToken;
  }

  /** Used by the credential factory. */
  public DlfSecretKeyCredential() {}

  @Override
  public String credentialType() {
    return DLF_SECRET_KEY_CREDENTIAL_TYPE;
  }

  @Override
  public long expireTimeInMs() {
    return 0;
  }

  @Override
  public Map<String, String> credentialInfo() {
    ImmutableMap.Builder<String, String> builder =
        new ImmutableMap.Builder<String, String>()
            .put(GRAVITINO_DLF_ACCESS_KEY_ID, accessKeyId)
            .put(GRAVITINO_DLF_ACCESS_KEY_SECRET, accessKeySecret);
    if (StringUtils.isNotBlank(securityToken)) {
      builder.put(GRAVITINO_DLF_SECURITY_TOKEN, securityToken);
    }
    return builder.build();
  }

  @Override
  public void initialize(Map<String, String> credentialInfo, long expireTimeInMs) {
    String accessKeyId = credentialInfo.get(GRAVITINO_DLF_ACCESS_KEY_ID);
    String accessKeySecret = credentialInfo.get(GRAVITINO_DLF_ACCESS_KEY_SECRET);
    validate(accessKeyId, accessKeySecret, expireTimeInMs);
    this.accessKeyId = accessKeyId;
    this.accessKeySecret = accessKeySecret;
    this.securityToken = credentialInfo.get(GRAVITINO_DLF_SECURITY_TOKEN);
  }

  /**
   * Returns the DLF access key ID.
   *
   * @return access key ID
   */
  public String accessKeyId() {
    return accessKeyId;
  }

  /**
   * Returns the DLF access key secret.
   *
   * @return access key secret
   */
  public String accessKeySecret() {
    return accessKeySecret;
  }

  /**
   * Returns the optional DLF security token.
   *
   * @return security token, or null
   */
  @Nullable
  public String securityToken() {
    return securityToken;
  }

  private void validate(String accessKeyId, String accessKeySecret, long expireTimeInMs) {
    Preconditions.checkArgument(
        StringUtils.isNotBlank(accessKeyId), "DLF access key ID should not be empty");
    Preconditions.checkArgument(
        StringUtils.isNotBlank(accessKeySecret), "DLF access key secret should not be empty");
    Preconditions.checkArgument(
        expireTimeInMs == 0, "The expiration time of DlfSecretKeyCredential should be 0");
  }
}
