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
import org.apache.gravitino.storage.GCSProperties;

public class GcsTokenCredential implements Credential {
  private String token;
  private long expireMs;

  public GcsTokenCredential(String sessionToken, long expireMs) {
    Preconditions.checkNotNull(sessionToken, "gcs session token should not null");
    this.token = sessionToken;
    this.expireMs = expireMs;
  }

  @Override
  public String getCredentialType() {
    return CredentialConstants.GCS_TOKEN_CREDENTIAL_TYPE;
  }

  @Override
  public long getExpireTime() {
    return expireMs;
  }

  @Override
  public Map<String, String> getCredentialInfo() {
    return (new ImmutableMap.Builder<String, String>())
        .put(GCSProperties.GRAVITINO_GCS_TOKEN, token)
        .put(GCSProperties.GRAVITINO_TOKEN_EXPIRE_MS, String.valueOf(expireMs))
        .build();
  }
}
