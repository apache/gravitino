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
