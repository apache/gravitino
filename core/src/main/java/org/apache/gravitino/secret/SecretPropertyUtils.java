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
