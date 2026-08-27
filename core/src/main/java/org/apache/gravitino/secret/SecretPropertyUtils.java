/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.secret;

import static org.apache.gravitino.secret.SecretConstants.URN_PREFIX;

import com.google.common.base.Preconditions;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;

/**
 * Helpers for secret-related entity property handling and request validation.
 *
 * <p>This is intentionally separate from {@link SecretManager}, which owns secret lifecycle
 * (build/write/rollback) and secret-key uniqueness checks rather than property assembly.
 */
public final class SecretPropertyUtils {

  private SecretPropertyUtils() {}

  /**
   * Returns whether a property value is a Gravitino secret URN for the given key.
   *
   * @param key the property key
   * @param value the property value
   * @return true when value starts with the secret URN prefix and ends with the key
   */
  public static boolean isSecretProperty(@Nullable String key, @Nullable String value) {
    return key != null && value != null && value.startsWith(URN_PREFIX) && value.endsWith(key);
  }

  /**
   * Builds a map of secret-manager plaintext properties only.
   *
   * <p>Starting from raw entity properties:
   *
   * <ol>
   *   <li>Include every entry where {@link #isSecretProperty} is true, including keys that may also
   *       appear in credential vending (for example {@code jdbc-password} or {@code
   *       s3-secret-access-key}).
   *   <li>Resolve secret URN values to plaintext via {@link SecretManager#readSecret}.
   * </ol>
   *
   * <p>Normal non-secret properties are not included. Plaintext values that are not secret URNs are
   * not included even when the key is sensitive.
   *
   * @param secretManager secret manager used to resolve URNs
   * @param rawProperties raw entity properties (may be null)
   * @return a new secret plaintext property map; never null
   */
  public static Map<String, String> buildSecrets(
      SecretManager secretManager, @Nullable Map<String, String> rawProperties) {
    Preconditions.checkArgument(secretManager != null, "secretManager must not be null");
    if (rawProperties == null || rawProperties.isEmpty()) {
      return Map.of();
    }
    Map<String, String> secrets = new HashMap<>();
    for (Map.Entry<String, String> entry : rawProperties.entrySet()) {
      String key = entry.getKey();
      String value = entry.getValue();
      if (key == null || value == null) {
        continue;
      }
      if (isSecretProperty(key, value)) {
        secrets.put(key, secretManager.readSecret(SecretUrn.parse(value)));
      }
    }
    return secrets;
  }

  /**
   * Merges base properties with secret plaintext properties.
   *
   * <p>Returns a new mutable map containing all entries from {@code base}, then overlays {@code
   * secrets}. Null maps are treated as empty.
   *
   * @param base non-secret / default-load properties (may be null)
   * @param secrets secret plaintext properties from {@link #buildSecrets} (may be null)
   * @return a new mutable merged property map; never null
   */
  public static Map<String, String> mergeProperties(
      @Nullable Map<String, String> base, @Nullable Map<String, String> secrets) {
    Map<String, String> merged = copyEntityProperties(base);
    if (secrets != null && !secrets.isEmpty()) {
      merged.putAll(secrets);
    }
    return merged;
  }

  /**
   * Returns whether either secret map has at least one entry.
   *
   * @param secretBindings write-through bindings (may be null)
   * @param secretReferences secret locators (may be null)
   * @return true when at least one secret map is non-empty
   */
  public static boolean hasSecretMaps(
      @Nullable Map<?, ?> secretBindings, @Nullable Map<?, ?> secretReferences) {
    return (secretBindings != null && !secretBindings.isEmpty())
        || (secretReferences != null && !secretReferences.isEmpty());
  }

  /**
   * Returns a mutable copy of a property map for create-time assembly.
   *
   * <p>{@code null} becomes an empty {@link HashMap}; otherwise returns a new {@link HashMap} copy.
   * Used for request properties and for merged catalog conf (which may be unmodifiable).
   *
   * @param properties property map to copy (may be null)
   * @return a mutable property map, never null
   */
  public static Map<String, String> copyEntityProperties(@Nullable Map<String, String> properties) {
    return properties == null ? new HashMap<>() : new HashMap<>(properties);
  }

  /**
   * Returns whether {@code value} is a write-through secret URN owned by this entity property.
   *
   * <p>Write-through URNs use identifier segments {@code entityType:entityId:propertyKey}.
   *
   * @param propertyKey the property key
   * @param value the property value
   * @param entityType {@code catalog}, {@code schema}, or {@code fileset}
   * @param entityId the entity id
   * @return true when the value is a write-through URN for this entity and property
   */
  public static boolean isWriteThroughForEntity(
      @Nullable String propertyKey, @Nullable String value, String entityType, long entityId) {
    if (!isSecretProperty(propertyKey, value)) {
      return false;
    }
    try {
      SecretUrn urn = SecretUrn.parse(value);
      List<String> segments = urn.identifierSegments();
      return segments.size() == 3
          && entityType.equals(segments.get(0))
          && String.valueOf(entityId).equals(segments.get(1))
          && propertyKey.equals(segments.get(2));
    } catch (IllegalArgumentException e) {
      return false;
    }
  }

  /**
   * Validates alter {@code setProperty} plaintext: rejects blank, masked placeholder, and raw URN
   * strings.
   *
   * @param property the property key
   * @param value the plaintext value
   */
  public static void validateAlterSetPropertyValue(String property, String value) {
    Preconditions.checkArgument(StringUtils.isNotBlank(property), "property must not be blank");
    Preconditions.checkArgument(StringUtils.isNotBlank(value), "value must not be blank");
    Preconditions.checkArgument(
        !"******".equals(value), "setProperty value must not be the masked placeholder ******");
    Preconditions.checkArgument(
        !value.startsWith(URN_PREFIX),
        "setProperty value must not be a secret URN; use setSecretBinding or setSecretReference");
  }

  /**
   * Validates alter {@code setSecretBinding} plaintext.
   *
   * @param plaintext the plaintext from the binding
   */
  public static void validateAlterSecretBindingPlaintext(String plaintext) {
    Preconditions.checkArgument(plaintext != null, "plaintext must not be null");
    Preconditions.checkArgument(
        !"******".equals(plaintext),
        "setSecretBinding plaintext must not be the masked placeholder ******");
  }

  /**
   * Returns a mutable property map for create-time assembly, or {@code null} when the caller
   * supplied no properties and no secrets.
   *
   * <p>When {@code properties} is {@code null} and both secret maps are null or empty, returns
   * {@code null} so {@code validatePropertyForCreate} can skip required-key checks (historical
   * behavior). When secrets are present but {@code properties} is null, returns an empty {@link
   * HashMap} for URN assembly. Otherwise returns a new {@link HashMap} copy of {@code properties}.
   *
   * @param properties property map to copy (may be null)
   * @param secretBindings write-through bindings (may be null)
   * @param secretReferences secret locators (may be null)
   * @return a mutable property map, or null when there are no properties and no secrets
   */
  @Nullable
  public static Map<String, String> copyEntityProperties(
      @Nullable Map<String, String> properties,
      @Nullable Map<?, ?> secretBindings,
      @Nullable Map<?, ?> secretReferences) {
    if (properties == null && !hasSecretMaps(secretBindings, secretReferences)) {
      return null;
    }
    return properties == null ? new HashMap<>() : new HashMap<>(properties);
  }

  /**
   * Puts each URN string into {@code entityProperties} under the property key encoded in the URN
   * (last identifier segment).
   *
   * @param entityProperties mutable entity properties
   * @param secretUrns secret URNs whose last identifier segment is the property key
   */
  public static void putSecretUrns(
      Map<String, String> entityProperties, List<SecretUrn> secretUrns) {
    Preconditions.checkArgument(entityProperties != null, "entityProperties must not be null");
    Preconditions.checkArgument(secretUrns != null, "secretUrns must not be null");
    for (SecretUrn urn : secretUrns) {
      entityProperties.put(urn.propertyKey(), urn.toString());
    }
  }

  /**
   * Validates create-time {@code secretBindings} request shape.
   *
   * @param bindings property key → write-through binding
   */
  static void validateSecretBindings(Map<String, SecretBinding> bindings) {
    for (Map.Entry<String, SecretBinding> entry : bindings.entrySet()) {
      String key = entry.getKey();
      Preconditions.checkArgument(
          StringUtils.isNotBlank(key), "secretBindings keys must not be blank");
      Preconditions.checkArgument(
          entry.getValue() != null, "secretBindings[%s] must not be null", key);
    }
  }

  /**
   * Validates create-time {@code secretReferences} request shape.
   *
   * @param references property key → secret locator
   */
  static void validateSecretReferences(Map<String, SecretReference> references) {
    for (Map.Entry<String, SecretReference> entry : references.entrySet()) {
      String key = entry.getKey();
      Preconditions.checkArgument(
          StringUtils.isNotBlank(key), "secretReferences keys must not be blank");
      Preconditions.checkArgument(
          entry.getValue() != null, "secretReferences[%s] must not be null", key);
    }
  }
}
