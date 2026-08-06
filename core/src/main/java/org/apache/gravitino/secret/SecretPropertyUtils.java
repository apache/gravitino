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
import java.util.Collections;
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
   * Returns a mutable copy of entity properties for create-time assembly.
   *
   * <p>{@code null} becomes an empty {@link HashMap}; otherwise returns a new {@link HashMap} copy.
   *
   * @param properties entity properties from the create request (may be null)
   * @return a mutable property map, never null
   */
  public static Map<String, String> copyEntityProperties(@Nullable Map<String, String> properties) {
    return properties == null ? new HashMap<>() : new HashMap<>(properties);
  }

  /**
   * Applies each URN string into {@code entityProperties} under the property key encoded in the URN
   * (last identifier segment).
   *
   * @param entityProperties mutable entity properties
   * @param secretUrns secret URNs whose last identifier segment is the property key
   */
  public static void applySecretUrns(
      Map<String, String> entityProperties, List<SecretUrn> secretUrns) {
    Preconditions.checkArgument(entityProperties != null, "entityProperties must not be null");
    Preconditions.checkArgument(secretUrns != null, "secretUrns must not be null");
    for (SecretUrn urn : secretUrns) {
      List<String> segments = urn.identifierSegments();
      Preconditions.checkArgument(
          !segments.isEmpty(), "Secret URN must contain at least one identifier segment: %s", urn);
      entityProperties.put(segments.get(segments.size() - 1), urn.toString());
    }
  }

  /**
   * Builds a temporary property map for {@code validatePropertyForCreate}.
   *
   * <p>Includes request properties, external-reference URNs, and binding plaintext so required
   * secret keys are visible to property metadata checks. This map is not persisted; call {@link
   * #applySecretReferences} and {@link #writeBindingsAndApplyUrns} to assemble final {@code
   * entityProperties}.
   *
   * <p>Returns {@code null} when the caller passed {@code null} properties and no secrets, matching
   * skip-on-null validation.
   *
   * @param properties entity properties from the create request (may be null)
   * @param secretBindings property key → write-through binding (may be null)
   * @param secretReferences property key → secret locator (may be null)
   * @param secretManager secret manager used to resolve reference URNs
   * @return properties for validation, or {@code null} to skip validation
   */
  @Nullable
  public static Map<String, String> propertiesToValidate(
      @Nullable Map<String, String> properties,
      @Nullable Map<String, SecretBinding> secretBindings,
      @Nullable Map<String, SecretReference> secretReferences,
      SecretManager secretManager) {
    Preconditions.checkArgument(secretManager != null, "secretManager must not be null");
    Map<String, SecretBinding> bindings = emptyIfNull(secretBindings);
    Map<String, SecretReference> references = emptyIfNull(secretReferences);
    if (properties == null && bindings.isEmpty() && references.isEmpty()) {
      return null;
    }
    Map<String, String> forValidation = copyEntityProperties(properties);
    applySecretUrns(forValidation, secretManager.getSecretReferenceUrns(references));
    for (Map.Entry<String, SecretBinding> entry : bindings.entrySet()) {
      forValidation.put(entry.getKey(), entry.getValue().plaintext());
    }
    return forValidation;
  }

  /**
   * Writes external-reference URNs into {@code entityProperties}.
   *
   * <p>{@code null} or empty references are a no-op.
   *
   * @param entityProperties mutable entity properties (final values only)
   * @param secretReferences property key → secret locator (may be null)
   * @param secretManager secret manager used to resolve reference URNs
   */
  public static void applySecretReferences(
      Map<String, String> entityProperties,
      @Nullable Map<String, SecretReference> secretReferences,
      SecretManager secretManager) {
    Preconditions.checkArgument(entityProperties != null, "entityProperties must not be null");
    Preconditions.checkArgument(secretManager != null, "secretManager must not be null");
    applySecretUrns(
        entityProperties, secretManager.getSecretReferenceUrns(emptyIfNull(secretReferences)));
  }

  /**
   * Persists write-through bindings and writes their URNs into {@code entityProperties}.
   *
   * <p>{@code null} or empty bindings are a no-op and return an empty list. Returned URNs should be
   * passed to {@link SecretManager#rollbackWritten} if entity create fails.
   *
   * @param entityProperties mutable entity properties (final values only)
   * @param entityType {@code catalog}, {@code schema}, or {@code fileset}
   * @param entityId stable numeric entity id
   * @param secretBindings property key → write-through binding (may be null)
   * @param secretManager secret manager used to write secrets
   * @return write-through URNs that were persisted (may be empty)
   */
  public static List<SecretUrn> writeBindingsAndApplyUrns(
      Map<String, String> entityProperties,
      String entityType,
      long entityId,
      @Nullable Map<String, SecretBinding> secretBindings,
      SecretManager secretManager) {
    Preconditions.checkArgument(entityProperties != null, "entityProperties must not be null");
    Preconditions.checkArgument(secretManager != null, "secretManager must not be null");
    Map<String, SecretBinding> bindings = emptyIfNull(secretBindings);
    List<SecretUrn> secretUrns = secretManager.getSecretBindingUrns(entityType, entityId, bindings);
    secretManager.writeSecrets(bindings, secretUrns);
    applySecretUrns(entityProperties, secretUrns);
    return secretUrns;
  }

  private static <V> Map<String, V> emptyIfNull(@Nullable Map<String, V> map) {
    return map == null ? Map.of() : map;
  }

  /**
   * Returns a copy of {@code properties} with secret URN values replaced by plaintext from {@code
   * secretManager}.
   *
   * <p>Used by the central catalog path so connectors receive plaintext conf while entity storage
   * keeps URN strings. Non-secret entries are copied unchanged.
   *
   * @param properties entity or request properties (may be null)
   * @param secretManager the secret manager used to read plaintext
   * @return a new map with secret URNs resolved; empty map when {@code properties} is null
   */
  public static Map<String, String> resolveSecretProperties(
      @Nullable Map<String, String> properties, SecretManager secretManager) {
    Preconditions.checkArgument(secretManager != null, "secretManager must not be null");
    if (properties == null || properties.isEmpty()) {
      return properties == null ? Collections.emptyMap() : Map.copyOf(properties);
    }
    Map<String, String> resolved = new HashMap<>(properties.size());
    for (Map.Entry<String, String> entry : properties.entrySet()) {
      String key = entry.getKey();
      String value = entry.getValue();
      if (isSecretProperty(key, value)) {
        resolved.put(key, secretManager.readSecret(SecretUrn.parse(value)));
      } else {
        resolved.put(key, value);
      }
    }
    return resolved;
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
