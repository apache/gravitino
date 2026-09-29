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

import java.util.Map;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Generates static DLF access-key credentials for Paimon DLF catalogs. */
public class DlfSecretKeyCredentialProvider implements CredentialProvider {

  private static final Logger LOG = LoggerFactory.getLogger(DlfSecretKeyCredentialProvider.class);

  private String accessKeyId;
  private String accessKeySecret;
  @Nullable private String securityToken;

  @Override
  public void initialize(Map<String, String> properties) {
    if (properties == null) {
      return;
    }
    this.accessKeyId = properties.get(DlfSecretKeyCredential.GRAVITINO_DLF_ACCESS_KEY_ID);
    this.accessKeySecret = properties.get(DlfSecretKeyCredential.GRAVITINO_DLF_ACCESS_KEY_SECRET);
    this.securityToken = properties.get(DlfSecretKeyCredential.GRAVITINO_DLF_SECURITY_TOKEN);
    if (StringUtils.isNotBlank(accessKeyId) ^ StringUtils.isNotBlank(accessKeySecret)) {
      LOG.warn(
          "Incomplete DLF static credential pair for {}: both {} and {} are required;"
              + " found accessKeyIdBlank={}, accessKeySecretBlank={}",
          DlfSecretKeyCredential.DLF_SECRET_KEY_CREDENTIAL_TYPE,
          DlfSecretKeyCredential.GRAVITINO_DLF_ACCESS_KEY_ID,
          DlfSecretKeyCredential.GRAVITINO_DLF_ACCESS_KEY_SECRET,
          StringUtils.isBlank(accessKeyId),
          StringUtils.isBlank(accessKeySecret));
    }
  }

  @Override
  public void close() {}

  @Override
  public String credentialType() {
    return DlfSecretKeyCredential.DLF_SECRET_KEY_CREDENTIAL_TYPE;
  }

  @Override
  public boolean supportsScheme(String scheme) {
    return false;
  }

  @Nullable
  @Override
  public Credential getCredential(CredentialContext context) {
    if (StringUtils.isBlank(accessKeyId) || StringUtils.isBlank(accessKeySecret)) {
      return null;
    }
    return new DlfSecretKeyCredential(accessKeyId, accessKeySecret, securityToken);
  }
}
