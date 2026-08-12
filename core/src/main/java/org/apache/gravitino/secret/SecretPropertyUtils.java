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
import com.google.common.collect.ImmutableSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.connector.PropertiesMetadata;

/**
 * Helpers for secret-related entity property handling and request validation.
 *
 * <p>This is intentionally separate from {@link SecretManager}, which owns secret lifecycle
 * (build/write/rollback) and secret-key uniqueness checks rather than property assembly.
 */
public final class SecretPropertyUtils {

  /**
   * Property keys owned by credential vending ({@code SupportsCredentials} / credential providers).
   * These must be omitted from the resolved-properties HTTP delivery API; callers should use {@code
   * getCredentials} instead.
   *
   * <p>Does <b>not</b> include {@code jdbc-user} / {@code jdbc-password}: those are secret-manager
   * connection secrets and must appear as plaintext when URN-backed.
   */
  public static final Set<String> CREDENTIAL_VENDING_PROPERTY_KEYS =
      ImmutableSet.of(
          // S3
          "s3-access-key-id",
          "s3-secret-access-key",
          "s3-session-token",
          // OSS
          "oss-access-key-id",
          "oss-secret-access-key",
          "oss-security-token",
          // COS
          "cos-access-key-id",
          "cos-secret-access-key",
          // AWS / Glue static keys
          "aws-access-key-id",
          "aws-secret-access-key",
          // Azure
          "azure-storage-account-key",
          "azure-client-secret",
          // GCS static credential material
          "gcs-service-account-file",
          "gcs-credential-path",
          "gcs-credential-file-path");

  private SecretPropertyUtils() {}

  /**
   * Returns whether a property key is owned by credential vending and must be omitted from resolved
   * plaintext property delivery.
   *
   * @param key the property key
   * @return true when the key is a credential-vending property
   */
  public static boolean isCredentialVendingProperty(@Nullable String key) {
    return key != null && CREDENTIAL_VENDING_PROPERTY_KEYS.contains(key);
  }

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
   * Builds a resolved plaintext property map for connectors / Lance / IRC.
   *
   * <p>Starting from raw entity properties:
   *
   * <ol>
   *   <li>Omit credential-vending keys (use {@code getCredentials}).
   *   <li>Resolve secret URN values to plaintext via {@link SecretManager#readSecret}.
   *   <li>Omit legacy hidden plaintext secrets ({@link PropertiesMetadata#isHiddenProperty}).
   *   <li>Include all other entries unchanged.
   * </ol>
   *
   * @param secretManager secret manager used to resolve URNs
   * @param rawProperties raw entity properties (may be null)
   * @param propertiesMetadata catalog/schema/fileset property metadata (may be null)
   * @return a new resolved property map; never null
   */
  public static Map<String, String> buildResolvedProperties(
      SecretManager secretManager,
      @Nullable Map<String, String> rawProperties,
      @Nullable PropertiesMetadata propertiesMetadata) {
    Preconditions.checkArgument(secretManager != null, "secretManager must not be null");
    if (rawProperties == null || rawProperties.isEmpty()) {
      return Map.of();
    }
    Map<String, String> resolved = new HashMap<>();
    for (Map.Entry<String, String> entry : rawProperties.entrySet()) {
      String key = entry.getKey();
      String value = entry.getValue();
      if (key == null || value == null) {
        continue;
      }
      if (isCredentialVendingProperty(key)) {
        continue;
      }
      if (isSecretProperty(key, value)) {
        resolved.put(key, secretManager.readSecret(SecretUrn.parse(value)));
        continue;
      }
      if (propertiesMetadata != null && propertiesMetadata.isHiddenProperty(key)) {
        continue;
      }
      resolved.put(key, value);
    }
    return resolved;
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
