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

package org.apache.gravitino.audit;

import com.google.common.collect.ImmutableSet;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Utility methods for redacting sensitive values in audit logs.
 *
 * <p>This is the single place redaction happens: {@code customInfo()} entries are captured raw
 * everywhere they originate (dispatcher extras, request query parameters, HTTP headers, etc.) and
 * redacted only here, once, right before a log line is written — by {@link
 * org.apache.gravitino.audit.v2.SimpleAuditLogV2} and {@link JsonAuditFormatter}, the only two
 * renderers that expose {@code customInfo()}. Keeping exactly one redaction pass, applied uniformly
 * to the fully-merged map regardless of which layer contributed which key, avoids the alternative
 * of multiple redaction call sites drifting out of sync with different keyword lists.
 */
public final class AuditLogRedactor {

  /** Redacted value used for sensitive audit fields. */
  public static final String REDACTED_VALUE = "***";

  /** Case-insensitive exact matches against known internal semantic keys. */
  private static final Set<String> MASKED_CUSTOM_INFO_KEYS =
      ImmutableSet.of(
          "authorization", "cookie", "x-amz-security-token", "s3.access-key-id", "jdbc-password");

  /**
   * Case-insensitive substrings of a {@code customInfo} key that mark it as sensitive, checked in
   * addition to the exact-match {@link #MASKED_CUSTOM_INFO_KEYS}. This exists because some keys —
   * notably request query-parameter names, which become {@code customInfo} entries via {@link
   * org.apache.gravitino.listener.api.event.Event#customInfo()} — are supplied by the caller and
   * can use arbitrary naming conventions (e.g. {@code accessToken}, {@code s3-secret-access-key})
   * that an exact-match list alone would miss. Extend this set as new sensitive key names are
   * identified.
   */
  private static final Set<String> SENSITIVE_KEY_SUBSTRINGS =
      ImmutableSet.of(
          "password",
          "secret",
          "token",
          "credential",
          "apikey",
          "accesskey",
          "privatekey",
          "auth",
          "signature");

  /**
   * Case-insensitive exact matches that are never sensitive, checked before {@link
   * #SENSITIVE_KEY_SUBSTRINGS}. These are fixed key names the codebase itself chooses (e.g. {@code
   * ownCustomInfo()} in {@code AuthorizationDenialFailureEvent}, {@code HttpRequestEvent}), not
   * caller-supplied data — unlike query-parameter names, a coincidental substring match here (e.g.
   * {@code auth.method} contains "auth") is always a false positive that would destroy genuinely
   * useful audit data instead of protecting anything. Add a key here only when it is a fixed string
   * literal in the codebase, never for a name that could come from external input.
   */
  private static final Set<String> NEVER_SENSITIVE_KEYS =
      ImmutableSet.of("http.method", "http.uri", "http.status", "auth.method", "auth.expression");

  private AuditLogRedactor() {}

  /**
   * Redacts sensitive custom information values while preserving the original key order.
   *
   * @param customInfo the original custom information
   * @return a copy with sensitive values redacted
   */
  public static Map<String, String> redactCustomInfo(Map<String, String> customInfo) {
    Map<String, String> redacted = new LinkedHashMap<>();
    if (customInfo == null) {
      return redacted;
    }

    customInfo.forEach((key, value) -> redacted.put(key, redactValue(key, value)));
    return redacted;
  }

  /**
   * Redacts a value when its key is considered sensitive.
   *
   * @param key the custom information key
   * @param value the original value
   * @return the redacted value for sensitive keys, otherwise the original value
   */
  public static String redactValue(String key, String value) {
    return isSensitiveKey(key) ? REDACTED_VALUE : value;
  }

  private static boolean isSensitiveKey(String key) {
    if (key == null) {
      return false;
    }
    String lowerCaseKey = key.toLowerCase(Locale.ROOT);
    if (NEVER_SENSITIVE_KEYS.contains(lowerCaseKey)) {
      return false;
    }
    return MASKED_CUSTOM_INFO_KEYS.contains(lowerCaseKey)
        || SENSITIVE_KEY_SUBSTRINGS.stream().anyMatch(lowerCaseKey::contains);
  }
}
