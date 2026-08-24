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

/** Masks hidden entity properties for API responses and rejects masked values on write. */
public final class HiddenPropertyMaskUtils {

  /** Placeholder returned instead of hidden credential and other sensitive property values. */
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
   * Returns a copy of {@code properties} with values for {@code hiddenPropertyNames} replaced by
   * {@link #MASKED_VALUE}. Entries with null keys or values are omitted.
   */
  public static Map<String, String> maskHiddenProperties(
      Map<String, String> properties, Set<String> hiddenPropertyNames) {
    if (properties == null || properties.isEmpty()) {
      return properties == null ? Map.of() : Map.copyOf(properties);
    }
    Set<String> hidden = hiddenPropertyNames == null ? Collections.emptySet() : hiddenPropertyNames;
    Map<String, String> masked = new HashMap<>(properties.size());
    for (Map.Entry<String, String> entry : properties.entrySet()) {
      String key = entry.getKey();
      String value = entry.getValue();
      if (key == null || value == null) {
        continue;
      }
      masked.put(key, hidden.contains(key) ? MASKED_VALUE : value);
    }
    return masked;
  }

  /**
   * Returns a copy of {@code properties} with values for metadata-hidden keys replaced by {@link
   * #MASKED_VALUE}. Entries with null keys or values are omitted.
   */
  public static Map<String, String> maskHiddenProperties(
      Map<String, String> properties, PropertiesMetadata metadata) {
    if (properties == null || properties.isEmpty()) {
      return properties == null ? Map.of() : Map.copyOf(properties);
    }
    Map<String, String> masked = new HashMap<>(properties.size());
    for (Map.Entry<String, String> entry : properties.entrySet()) {
      String key = entry.getKey();
      String value = entry.getValue();
      if (key == null || value == null) {
        continue;
      }
      masked.put(
          key,
          metadata.isHiddenProperty(key) || SecretPropertyUtils.isSecretProperty(key, value)
              ? MASKED_VALUE
              : value);
    }
    return masked;
  }
}
