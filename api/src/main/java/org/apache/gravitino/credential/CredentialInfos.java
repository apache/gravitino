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

import com.google.common.collect.ImmutableMap;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Helpers for merging {@link Credential#credentialInfo()} into connector configuration.
 *
 * <p>Catalog configs that are built once and cached must not absorb expiring (token) credentials.
 */
public final class CredentialInfos {

  private CredentialInfos() {}

  /**
   * Returns a map of non-expiring credential info entries suitable for one-shot connector config.
   *
   * <p>Skips null credentials, credentials with {@code expireTimeInMs != 0}, and null {@link
   * Credential#credentialInfo()}. Later credentials overwrite earlier keys.
   *
   * @param credentials credentials from {@link SupportsCredentials#getCredentials()}, may be null
   * @return merged non-expiring credential info; never null
   */
  public static Map<String, String> nonExpiringCredentialInfo(@Nullable Credential[] credentials) {
    if (credentials == null || credentials.length == 0) {
      return ImmutableMap.of();
    }
    Map<String, String> merged = new HashMap<>();
    for (Credential credential : credentials) {
      if (credential == null
          || credential.expireTimeInMs() != 0
          || credential.credentialInfo() == null) {
        continue;
      }
      merged.putAll(credential.credentialInfo());
    }
    return ImmutableMap.copyOf(merged);
  }
}
