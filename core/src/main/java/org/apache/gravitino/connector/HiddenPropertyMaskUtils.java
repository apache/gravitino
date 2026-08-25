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
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.gravitino.secret.SecretPropertyUtils;

/**
 * Read/write helpers for sensitive entity properties.
 *
 * <p><b>Read (API response)</b>:
 *
 * <ul>
 *   <li>Non-reserved hidden keys (credentials such as {@code jdbc-password} / S3 secrets) and
 *       secret-manager URNs are kept with value {@link #MASKED_VALUE}.
 *   <li>Reserved+hidden system keys (for example {@code gravitino.identifier}, Hive {@code
 *       external}) are omitted, matching the historical response shape.
 *   <li>Other properties, including reserved-but-visible ones (for example {@code numFiles}), are
 *       returned as-is. Reserved only controls write rejection for non-hidden keys.
 * </ul>
 *
 * <p><b>Write (create / alter)</b>: reject any value equal to {@link #MASKED_VALUE} so the
 * placeholder is never persisted. Reserved / immutable rejection remains in {@code
 * PropertiesMetadataHelpers}.
 */
public final class HiddenPropertyMaskUtils {

  /** Placeholder returned instead of hidden credential / secret property values. */
  public static final String MASKED_VALUE = "******";

  private HiddenPropertyMaskUtils() {}

  /** Property keys to mask vs omit when building API responses. */
  public static final class PropertyResponsePolicy {
    private final Set<String> keysToMask;
    private final Set<String> keysToOmit;

    /**
     * Creates a response policy.
     *
     * @param keysToMask keys whose values become {@link #MASKED_VALUE}
     * @param keysToOmit keys removed from the response
     */
    public PropertyResponsePolicy(Set<String> keysToMask, Set<String> keysToOmit) {
      this.keysToMask =
          keysToMask == null ? Collections.emptySet() : Collections.unmodifiableSet(keysToMask);
      this.keysToOmit =
          keysToOmit == null ? Collections.emptySet() : Collections.unmodifiableSet(keysToOmit);
    }

    /**
     * @return keys to return with {@link #MASKED_VALUE}
     */
    public Set<String> keysToMask() {
      return keysToMask;
    }

    /**
     * @return keys to drop from the response
     */
    public Set<String> keysToOmit() {
      return keysToOmit;
    }
  }

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
   * Builds a {@link PropertyResponsePolicy} from property metadata.
   *
   * <p>Non-reserved hidden keys and secret URNs are masked; reserved+hidden keys are omitted.
   */
  public static PropertyResponsePolicy buildPropertyResponsePolicy(
      Map<String, String> properties, PropertiesMetadata metadata) {
    if (properties == null || properties.isEmpty()) {
      return new PropertyResponsePolicy(Collections.emptySet(), Collections.emptySet());
    }
    Set<String> toMask = new HashSet<>();
    Set<String> toOmit = new HashSet<>();
    for (Map.Entry<String, String> entry : properties.entrySet()) {
      String key = entry.getKey();
      if (key == null) {
        continue;
      }
      boolean hidden = metadata.isHiddenProperty(key);
      boolean reserved = metadata.isReservedProperty(key);
      boolean secret = SecretPropertyUtils.isSecretProperty(key, entry.getValue());
      if (secret || (hidden && !reserved)) {
        toMask.add(key);
      } else if (hidden) {
        toOmit.add(key);
      }
    }
    return new PropertyResponsePolicy(toMask, toOmit);
  }

  /**
   * Returns a mutable copy of {@code properties} with {@code keysToMask} replaced by {@link
   * #MASKED_VALUE} and {@code keysToOmit} removed. Entries with null keys or values are dropped.
   */
  public static Map<String, String> maskHiddenProperties(
      Map<String, String> properties, Set<String> keysToMask, Set<String> keysToOmit) {
    if (properties == null || properties.isEmpty()) {
      return new HashMap<>();
    }
    Set<String> mask = keysToMask == null ? Collections.emptySet() : keysToMask;
    Set<String> omit = keysToOmit == null ? Collections.emptySet() : keysToOmit;
    Map<String, String> result = new HashMap<>(properties.size());
    for (Map.Entry<String, String> entry : properties.entrySet()) {
      String key = entry.getKey();
      String value = entry.getValue();
      if (key == null || value == null || omit.contains(key)) {
        continue;
      }
      result.put(key, mask.contains(key) ? MASKED_VALUE : value);
    }
    return result;
  }

  /**
   * Returns a mutable copy of {@code properties} with values for {@code keysToMask} replaced by
   * {@link #MASKED_VALUE}. Entries with null keys or values are dropped.
   *
   * <p>The returned map is always mutable so callers can add defaults such as {@code in-use}.
   */
  public static Map<String, String> maskHiddenProperties(
      Map<String, String> properties, Set<String> keysToMask) {
    return maskHiddenProperties(properties, keysToMask, Collections.emptySet());
  }

  /**
   * Applies a {@link PropertyResponsePolicy} to {@code properties}.
   *
   * @see #maskHiddenProperties(Map, Set, Set)
   */
  public static Map<String, String> maskHiddenProperties(
      Map<String, String> properties, PropertyResponsePolicy policy) {
    if (policy == null) {
      return maskHiddenProperties(properties, Collections.emptySet(), Collections.emptySet());
    }
    return maskHiddenProperties(properties, policy.keysToMask(), policy.keysToOmit());
  }

  /**
   * Returns a mutable API-response copy of {@code properties}.
   *
   * <p>Non-reserved hidden keys and secret URNs become {@link #MASKED_VALUE}; reserved+hidden keys
   * are omitted.
   */
  public static Map<String, String> maskHiddenProperties(
      Map<String, String> properties, PropertiesMetadata metadata) {
    Objects.requireNonNull(metadata, "metadata");
    return maskHiddenProperties(properties, buildPropertyResponsePolicy(properties, metadata));
  }
}
