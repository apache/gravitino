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

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.gravitino.json.JsonUtils;
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
 *   <li><b>Mask</b> other hidden keys (credentials such as {@code jdbc-password}), secret-manager
 *       URN values, and undeclared keys whose names look sensitive (contain {@code secret}, {@code
 *       password}, {@code token}, {@code credential}, {@code access}, or {@code account},
 *       case-insensitive) with {@link #MASKED_VALUE}, so clients can see that the property exists
 *       without reading the plaintext.
 *   <li>Return all other properties as-is, including reserved-but-visible ones (for example {@code
 *       in-use}, {@code numFiles}).
 * </ul>
 *
 * <p>Recovery via {@code getSecrets} covers secret-URN values, declared {@code hidden} keys (except
 * credential-vending keys), and sensitive-named undeclared keys. Credential property keys are
 * recovered via {@code getCredentials()} instead. See {@code
 * org.apache.gravitino.secret.SupportsSecrets}.
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
   * @return keys to mask and keys to omit
   */
  public static MaskAndOmitKeys classifyHiddenProperties(
      @Nullable Map<String, String> properties, PropertiesMetadata metadata) {
    Objects.requireNonNull(metadata, "metadata");
    if (properties == null || properties.isEmpty()) {
      return MaskAndOmitKeys.empty();
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
      } else if (hidden
          || SecretPropertyUtils.isSecretProperty(key, value)
          || (!metadata.containsProperty(key) && SecretPropertyUtils.isSensitivePropertyKey(key))) {
        keysToMask.add(key);
      }
    }
    return MaskAndOmitKeys.of(Set.copyOf(keysToMask), Set.copyOf(keysToOmit));
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
   * Like {@link #maskHiddenProperties(Map, Set, Set)} using the given classification.
   *
   * @param properties the source properties
   * @param keys keys to mask and omit
   * @return a mutable API-response copy
   */
  public static Map<String, String> maskHiddenProperties(
      Map<String, String> properties, @Nullable MaskAndOmitKeys keys) {
    MaskAndOmitKeys classification = keys == null ? MaskAndOmitKeys.empty() : keys;
    return maskHiddenProperties(
        properties, classification.keysToMask(), classification.keysToOmit());
  }

  /**
   * Returns a mutable API-response copy of {@code properties}: reserved+hidden keys are omitted;
   * other hidden keys and secret-manager URN values are replaced with {@link #MASKED_VALUE}.
   */
  public static Map<String, String> maskHiddenProperties(
      Map<String, String> properties, PropertiesMetadata metadata) {
    return maskHiddenProperties(properties, classifyHiddenProperties(properties, metadata));
  }

  /**
   * Returns a mutable copy of {@code properties} with sensitive-looking values replaced by {@link
   * #MASKED_VALUE}, without requiring {@link PropertiesMetadata}. Equivalent to {@link
   * #maskSensitiveNamedProperties(Map, Set)} with no nested JSON keys.
   *
   * @param properties source map, may be null
   * @return a mutable masked copy; never null
   */
  public static Map<String, String> maskSensitiveNamedProperties(
      @Nullable Map<String, String> properties) {
    return maskSensitiveNamedProperties(properties, null);
  }

  /**
   * Returns a mutable copy of {@code properties} with sensitive-looking values replaced by {@link
   * #MASKED_VALUE}, without requiring {@link PropertiesMetadata}.
   *
   * <p>A top-level entry is masked when its key matches {@link
   * SecretPropertyUtils#isSensitivePropertyKey} or its value is a secret-manager URN ({@link
   * SecretPropertyUtils#isSecretProperty}). When {@code nestedJsonMapKeys} is non-null, values for
   * those keys are parsed as JSON and sensitive-named keys are masked at any nesting depth (objects
   * and arrays). Other values are never parsed as JSON. On parse failure for a nested JSON key, the
   * entire value is replaced with {@link #MASKED_VALUE} so secrets are not left in a broken blob.
   *
   * <p>Useful for job template {@code environments} / {@code customFields} / {@code configs} that
   * have no properties metadata declaration. Callers that know job-specific JSON-map keys (for
   * example Iceberg built-in jobs) should pass them via {@code nestedJsonMapKeys}.
   *
   * @param properties source map, may be null
   * @param nestedJsonMapKeys keys whose string values are JSON maps/objects to walk; may be null
   * @return a mutable masked copy; never null
   */
  public static Map<String, String> maskSensitiveNamedProperties(
      @Nullable Map<String, String> properties, @Nullable Set<String> nestedJsonMapKeys) {
    if (properties == null || properties.isEmpty()) {
      return new HashMap<>();
    }
    Set<String> nestedKeys = nestedJsonMapKeys == null ? Collections.emptySet() : nestedJsonMapKeys;
    Map<String, String> result = new LinkedHashMap<>(properties.size());
    for (Map.Entry<String, String> entry : properties.entrySet()) {
      String key = entry.getKey();
      String value = entry.getValue();
      if (key == null || value == null) {
        continue;
      }
      if (SecretPropertyUtils.isSensitivePropertyKey(key)
          || SecretPropertyUtils.isSecretProperty(key, value)) {
        result.put(key, MASKED_VALUE);
      } else if (nestedKeys.contains(key)) {
        result.put(key, maskNestedJsonValue(value));
      } else {
        result.put(key, value);
      }
    }
    return result;
  }

  /**
   * Parses {@code value} as JSON and masks sensitive-named keys in objects (recursively through
   * nested objects and arrays). Non-object roots and malformed JSON are replaced with {@link
   * #MASKED_VALUE} so secrets are not returned unchanged.
   */
  private static String maskNestedJsonValue(String value) {
    ObjectMapper mapper = JsonUtils.anyFieldMapper();
    try {
      Object root = mapper.readValue(value, Object.class);
      if (!(root instanceof Map) && !(root instanceof List)) {
        // Scalar JSON (string/number/boolean/null) — nothing to walk by key name.
        return value;
      }
      MaskedJson masked = maskJsonNode(root);
      return masked.changed ? mapper.writeValueAsString(masked.value) : value;
    } catch (Exception e) {
      // Malformed JSON on a caller-declared nest key — do not leave a possible secret blob as-is.
      return MASKED_VALUE;
    }
  }

  private static MaskedJson maskJsonNode(Object node) {
    if (node instanceof Map) {
      Map<?, ?> map = (Map<?, ?>) node;
      if (map.isEmpty()) {
        return new MaskedJson(node, false);
      }
      Map<String, Object> out = new LinkedHashMap<>(map.size());
      boolean changed = false;
      for (Map.Entry<?, ?> entry : map.entrySet()) {
        if (entry.getKey() == null) {
          continue;
        }
        String nestedKey = String.valueOf(entry.getKey());
        Object nestedRaw = entry.getValue();
        if (nestedRaw instanceof Map || nestedRaw instanceof List) {
          if (SecretPropertyUtils.isSensitivePropertyKey(nestedKey)) {
            out.put(nestedKey, MASKED_VALUE);
            changed = true;
          } else {
            MaskedJson child = maskJsonNode(nestedRaw);
            out.put(nestedKey, child.value);
            changed |= child.changed;
          }
        } else {
          String nestedValue = nestedRaw == null ? "" : nestedRaw.toString();
          if (SecretPropertyUtils.isSensitivePropertyKey(nestedKey)
              || SecretPropertyUtils.isSecretProperty(nestedKey, nestedValue)) {
            out.put(nestedKey, MASKED_VALUE);
            changed = true;
          } else {
            out.put(nestedKey, nestedRaw);
          }
        }
      }
      return new MaskedJson(out, changed);
    }
    if (node instanceof List) {
      List<?> list = (List<?>) node;
      List<Object> out = new ArrayList<>(list.size());
      boolean changed = false;
      for (Object item : list) {
        if (item instanceof Map || item instanceof List) {
          MaskedJson child = maskJsonNode(item);
          out.add(child.value);
          changed |= child.changed;
        } else {
          out.add(item);
        }
      }
      return new MaskedJson(out, changed);
    }
    return new MaskedJson(node, false);
  }

  /** Result of walking a JSON tree for sensitive-key masking. */
  private static final class MaskedJson {
    private final Object value;
    private final boolean changed;

    private MaskedJson(Object value, boolean changed) {
      this.value = value;
      this.changed = changed;
    }
  }
}
