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
package org.apache.gravitino.maintenance.policy;

import com.google.common.collect.ImmutableSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;

/** Detects credential-shaped keys that must not appear in policy {@code jobOptions}. */
public final class MaintenanceSensitiveOptionKeys {

  private static final Set<String> SENSITIVE_SUBSTRINGS =
      ImmutableSet.of(
          "password",
          "secret",
          "token",
          "credential",
          "keytab",
          "oauthcredential",
          "client_secret",
          "private_key",
          "access_key",
          "secret_key");

  private MaintenanceSensitiveOptionKeys() {}

  /**
   * Returns whether an option key looks like a secret reference.
   *
   * @param key option key
   * @return {@code true} when the key must be rejected in policy content
   */
  public static boolean isSensitiveKey(String key) {
    if (StringUtils.isBlank(key)) {
      return false;
    }
    String normalized = key.toLowerCase(Locale.ROOT);
    for (String fragment : SENSITIVE_SUBSTRINGS) {
      if (normalized.contains(fragment)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Validates a string map for sensitive keys.
   *
   * @param options map to validate (for example {@code jobOptions})
   * @param mapName label used in error messages
   * @throws IllegalArgumentException when a sensitive key is present
   */
  public static void validateNonSensitive(Map<String, String> options, String mapName) {
    if (options == null || options.isEmpty()) {
      return;
    }
    for (String key : options.keySet()) {
      if (isSensitiveKey(key)) {
        throw new IllegalArgumentException(
            String.format(
                "%s must not contain credential-shaped key '%s'; use gravitino.conf auth overlays",
                mapName, key));
      }
    }
  }
}
