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
 * Read/write helpers for sensitive and system-managed entity properties.
 *
 * <p><b>Read (API response)</b>:
 *
 * <ul>
 *   <li><b>Omit</b> keys that are both metadata-{@code reserved} and {@code hidden} (for example
 *       {@code gravitino.identifier}). Users cannot set them and UIs cannot edit them, so a masked
 *       placeholder is useless.
 *   <li><b>Mask</b> other hidden keys (credentials such as {@code jdbc-password}) and
 *       secret-manager URN values with {@link #MASKED_VALUE}, so clients can see that the property
 *       exists.
 *   <li>Return all other properties as-is, including reserved-but-visible ones (for example {@code
 *       in-use}, {@code numFiles}).
 * </ul>
 *
 * <p><b>Write (create / alter)</b>: reject any value equal to {@link #MASKED_VALUE} so the
 * placeholder is never persisted. Reserved / immutable rejection remains in {@code
 * PropertiesMetadataHelpers}.
 */
public final class HiddenPropertyMaskUtils {

  /** Placeholder returned instead of editable hidden / credential / secret property values. */
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
   * Classifies property keys for API responses.
   *
   * @return entry of {@code (keysToMask, keysToOmit)}
   */
  public static Map.Entry<Set<String>, Set<String>> classifyHiddenProperties(
      @Nullable Map<String, String> properties, PropertiesMetadata metadata) {
    Objects.requireNonNull(metadata, "metadata");
    if (properties == null || properties.isEmpty()) {
      return Map.entry(Collections.emptySet(), Collections.emptySet());
    }

    Set<String> keysToMask = new HashSet<>();
    Set<String> keysToOmit = new HashSet<>();
    for (Map.Entry<String, String> entry : properties.entrySet()) {
      String key = entry.getKey();
      String value = entry.getValue();
      if (key == null || value == null) {
        continue;
      }
      boolean hidden = metadata.isHiddenProperty(key);
      boolean reserved = metadata.isReservedProperty(key);
      if (hidden && reserved) {
        keysToOmit.add(key);
      } else if (hidden || SecretPropertyUtils.isSecretProperty(key, value)) {
        keysToMask.add(key);
      }
    }
    return Map.entry(Set.copyOf(keysToMask), Set.copyOf(keysToOmit));
  }

  /**
   * Returns a mutable copy of {@code properties} with values for {@code keysToMask} replaced by
   * {@link #MASKED_VALUE}. Entries with null keys or values are dropped. No keys are omitted.
   *
   * <p>The returned map is always mutable so callers can add defaults such as {@code in-use}.
   */
  public static Map<String, String> maskHiddenProperties(
      Map<String, String> properties, Set<String> keysToMask) {
    return maskHiddenProperties(properties, keysToMask, Collections.emptySet());
  }

  /**
   * Like {@link #maskHiddenProperties(Map, Set)}, and also drops {@code keysToOmit} from the
   * result.
   */
  public static Map<String, String> maskHiddenProperties(
      Map<String, String> properties,
      @Nullable Set<String> keysToMask,
      @Nullable Set<String> keysToOmit) {
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
   * Returns a mutable API-response copy of {@code properties}: reserved+hidden keys are omitted;
   * other hidden keys and secret-manager URN values are replaced with {@link #MASKED_VALUE}.
   */
  public static Map<String, String> maskHiddenProperties(
      Map<String, String> properties, PropertiesMetadata metadata) {
    Map.Entry<Set<String>, Set<String>> classified = classifyHiddenProperties(properties, metadata);
    return maskHiddenProperties(properties, classified.getKey(), classified.getValue());
  }
}
