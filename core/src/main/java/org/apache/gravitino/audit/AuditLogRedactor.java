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

/** Utility methods for redacting sensitive values in audit logs. */
public final class AuditLogRedactor {

  /** Redacted value used for sensitive audit fields. */
  public static final String REDACTED_VALUE = "***";

  private static final Set<String> MASKED_CUSTOM_INFO_KEYS =
      ImmutableSet.of(
          "authorization", "cookie", "x-amz-security-token", "s3.access-key-id", "jdbc-password");

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
    return isSensitiveCustomInfoKey(key) ? REDACTED_VALUE : value;
  }

  private static boolean isSensitiveCustomInfoKey(String key) {
    return key != null && MASKED_CUSTOM_INFO_KEYS.contains(key.toLowerCase(Locale.ROOT));
  }
}
