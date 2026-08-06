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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;

/**
 * Helpers for secret-related entity property handling and request validation.
 *
 * <p>This is intentionally separate from {@link SecretManager}, which owns secret lifecycle
 * (build/write/rollback) rather than property assembly and request-shape checks.
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
   * Rejects property keys that appear in both {@code secretBindings} and {@code secretReferences}.
   *
   * @param secretBindings property key → write-through binding (may be null)
   * @param secretReferences property key → secret locator (may be null)
   */
  public static void checkNoOverlap(
      @Nullable Map<String, SecretBinding> secretBindings,
      @Nullable Map<String, SecretReference> secretReferences) {
    Set<String> bindingKeys = secretBindings == null ? Set.of() : secretBindings.keySet();
    Set<String> referenceKeys = secretReferences == null ? Set.of() : secretReferences.keySet();
    Set<String> overlap = new HashSet<>(bindingKeys);
    overlap.retainAll(referenceKeys);
    Preconditions.checkArgument(
        overlap.isEmpty(),
        "Property keys cannot appear in both secretBindings and secretReferences: %s",
        overlap);
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
   * Applies each URN string into {@code properties} under the property key encoded in the URN (last
   * identifier segment).
   *
   * @param properties mutable entity properties
   * @param secretUrns secret URNs whose last identifier segment is the property key
   */
  public static void applySecretUrns(Map<String, String> properties, List<SecretUrn> secretUrns) {
    Preconditions.checkArgument(properties != null, "properties must not be null");
    Preconditions.checkArgument(secretUrns != null, "secretUrns must not be null");
    for (SecretUrn urn : secretUrns) {
      List<String> segments = urn.identifierSegments();
      Preconditions.checkArgument(
          !segments.isEmpty(), "Secret URN must contain at least one identifier segment: %s", urn);
      properties.put(segments.get(segments.size() - 1), urn.toString());
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
