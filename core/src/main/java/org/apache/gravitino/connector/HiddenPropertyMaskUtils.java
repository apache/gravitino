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
package org.apache.gravitino.connector;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.gravitino.secret.SecretPropertyUtils;

/**
 * Read/write helpers for sensitive entity properties.
 *
 * <p><b>Read (API response)</b>: keep every property key. Replace values with {@link #MASKED_VALUE}
 * when the key is metadata-{@code hidden} (including credential keys such as {@code jdbc-password}
 * / S3 secrets) or the value is a secret-manager URN. Other properties, including
 * reserved-but-visible ones (for example {@code numFiles}, {@code creator}), are returned as-is.
 * Reserved only controls whether users may write the property; it does not remove keys from the
 * response.
 *
 * <p><b>Write (create / alter)</b>: reject any value equal to {@link #MASKED_VALUE} so the
 * placeholder is never persisted. Reserved / immutable rejection remains in {@code
 * PropertiesMetadataHelpers}.
 */
public final class HiddenPropertyMaskUtils {

  /** Placeholder returned instead of hidden / credential / secret property values. */
  public static final String MASKED_VALUE = "******";

  private HiddenPropertyMaskUtils() {}

  /** Returns true when {@code value} is the read-path masked placeholder. */
  public static boolean isMaskedPlaceholder(@Nullable String value) {
    return MASKED_VALUE.equals(value);
  }

  /**
   * Rejects property maps that use the read-path masked placeholder as a write value.
   *
   * @throws IllegalArgumentException if any property value is {@link #MASKED_VALUE}
   */
  public static void validateNoMaskedPlaceholders(Map<String, String> properties) {
    if (properties == null || properties.isEmpty()) {
      return;
    }
    for (Map.Entry<String, String> entry : properties.entrySet()) {
      if (isMaskedPlaceholder(entry.getValue())) {
        throw new IllegalArgumentException(
            String.format(
                "Property '%s' cannot be set to the masked placeholder value '%s'",
                entry.getKey(), MASKED_VALUE));
      }
    }
  }

  /**
   * Returns a mutable copy of {@code properties} with values for {@code keysToMask} replaced by
   * {@link #MASKED_VALUE}. Entries with null keys or values are dropped.
   *
   * <p>The returned map is always mutable so callers can add defaults such as {@code in-use}.
   */
  public static Map<String, String> maskHiddenProperties(
      Map<String, String> properties, Set<String> keysToMask) {
    if (properties == null || properties.isEmpty()) {
      return new HashMap<>();
    }
    Set<String> mask = keysToMask == null ? Collections.emptySet() : keysToMask;
    Map<String, String> result = new HashMap<>(properties.size());
    for (Map.Entry<String, String> entry : properties.entrySet()) {
      String key = entry.getKey();
      String value = entry.getValue();
      if (key == null || value == null) {
        continue;
      }
      result.put(key, mask.contains(key) ? MASKED_VALUE : value);
    }
    return result;
  }

  /**
   * Returns a mutable API-response copy of {@code properties}.
   *
   * <p>Values are replaced with {@link #MASKED_VALUE} when {@link
   * PropertiesMetadata#isHiddenProperty(String)} is true (credential and other sensitive keys) or
   * {@link SecretPropertyUtils#isSecretProperty(String, String)} is true. Reserved keys are not
   * removed.
   */
  public static Map<String, String> maskHiddenProperties(
      Map<String, String> properties, PropertiesMetadata metadata) {
    if (properties == null || properties.isEmpty()) {
      return new HashMap<>();
    }
    Map<String, String> result = new HashMap<>(properties.size());
    for (Map.Entry<String, String> entry : properties.entrySet()) {
      String key = entry.getKey();
      String value = entry.getValue();
      if (key == null || value == null) {
        continue;
      }
      boolean shouldMask =
          metadata.isHiddenProperty(key) || SecretPropertyUtils.isSecretProperty(key, value);
      result.put(key, shouldMask ? MASKED_VALUE : value);
    }
    return result;
  }
}
