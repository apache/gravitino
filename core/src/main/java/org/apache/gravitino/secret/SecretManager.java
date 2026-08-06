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

import static org.apache.gravitino.secret.SecretConstants.ATTR_ENTITY_ID;
import static org.apache.gravitino.secret.SecretConstants.ATTR_ENTITY_TYPE;
import static org.apache.gravitino.secret.SecretConstants.ATTR_PROPERTY_KEY;
import static org.apache.gravitino.secret.SecretPropertyUtils.validateSecretBindings;
import static org.apache.gravitino.secret.SecretPropertyUtils.validateSecretReferences;

import com.google.common.collect.ImmutableMap;
import java.io.Closeable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SecretManager owns write-through / external-reference secret lifecycle against configured
 * providers, and rolls back write-through secrets when entity create fails.
 */
public class SecretManager implements Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(SecretManager.class);

  private final SecretProviderRegistry registry;

  public SecretManager(Config config) {
    this.registry = new SecretProviderRegistry(config);
  }

  public SecretManager(SecretProviderRegistry registry) {
    this.registry = registry;
  }

  /**
   * Returns the secrets-provider registry owned by this manager.
   *
   * @return the registry
   */
  public SecretProviderRegistry getRegistry() {
    return registry;
  }

  /**
   * Ensures each property key appears at most once across {@code properties}, {@code
   * secretBindings}, and {@code secretReferences}.
   *
   * <p>{@code null} maps are treated as empty.
   *
   * @param properties entity properties from the create request (may be null)
   * @param secretBindings property key → write-through binding (may be null)
   * @param secretReferences property key → secret locator (may be null)
   */
  public void checkSecretKeys(
      @Nullable Map<String, String> properties,
      @Nullable Map<String, SecretBinding> secretBindings,
      @Nullable Map<String, SecretReference> secretReferences) {
    Set<String> keys = new HashSet<>();
    int count = 0;
    if (properties != null) {
      keys.addAll(properties.keySet());
      count += properties.size();
    }
    if (secretBindings != null) {
      keys.addAll(secretBindings.keySet());
      count += secretBindings.size();
    }
    if (secretReferences != null) {
      keys.addAll(secretReferences.keySet());
      count += secretReferences.size();
    }
    if (keys.size() != count) {
      throw new IllegalArgumentException(
          "Duplicate property key across properties, secretBindings and secretReferences");
    }
  }

  /**
   * Builds external-reference URNs from {@code secretReferences} without writing secret material.
   *
   * <p>Callers must put the returned URN strings into properties themselves (e.g. via {@link
   * SecretPropertyUtils#putSecretUrns}). External-ref URNs are owned outside Gravitino and must not
   * be passed to {@link #rollbackWritten}.
   *
   * @param secretReferences property key → secret locator (null or empty returns an empty list)
   * @return external-reference URNs (insertion order)
   */
  public List<SecretUrn> getSecretReferenceUrns(
      @Nullable Map<String, SecretReference> secretReferences) {
    if (secretReferences == null || secretReferences.isEmpty()) {
      return List.of();
    }
    validateSecretReferences(secretReferences);

    List<SecretUrn> urns = new ArrayList<>(secretReferences.size());
    for (Map.Entry<String, SecretReference> entry : secretReferences.entrySet()) {
      String key = entry.getKey();
      SecretReference locator = entry.getValue();
      String providerName = locator.provider();
      SecretProvider provider = registry.getProvider(providerName);
      try {
        SecretUrn urn = provider.buildReferenceUrn(key, locator.attributes());
        ensureUrnEndsWithPropertyKey(urn, key);
        urns.add(urn);
      } catch (UnsupportedOperationException e) {
        throw new IllegalArgumentException(
            "Provider \""
                + providerName
                + "\" does not support secretReferences for key \""
                + key
                + "\"",
            e);
      }
    }
    return List.copyOf(urns);
  }

  /**
   * Builds write-through URNs from {@code secretBindings} without writing secret material.
   *
   * <p>Callers should pass the returned URNs to {@link #writeSecrets} to persist plaintext from
   * each binding's value, then put URNs into properties (e.g. via {@link
   * SecretPropertyUtils#putSecretUrns}).
   *
   * @param entityType {@code catalog}, {@code schema}, or {@code fileset}
   * @param entityId stable numeric entity id
   * @param secretBindings property key → write-through binding (null or empty returns an empty
   *     list)
   * @return write-through URNs (insertion order)
   */
  public List<SecretUrn> getSecretBindingUrns(
      String entityType, long entityId, @Nullable Map<String, SecretBinding> secretBindings) {
    if (StringUtils.isBlank(entityType)) {
      throw new IllegalArgumentException("entityType must not be blank");
    }
    if (secretBindings == null || secretBindings.isEmpty()) {
      return List.of();
    }
    validateSecretBindings(secretBindings);

    List<SecretUrn> urns = new ArrayList<>(secretBindings.size());
    for (Map.Entry<String, SecretBinding> entry : secretBindings.entrySet()) {
      String key = entry.getKey();
      String providerName = entry.getValue().provider();
      // Ensure the provider is registered before building the URN.
      registry.getProvider(providerName);
      Map<String, String> attributes =
          ImmutableMap.of(
              ATTR_ENTITY_TYPE, entityType,
              ATTR_ENTITY_ID, String.valueOf(entityId),
              ATTR_PROPERTY_KEY, key);
      SecretUrn urn = SecretUrn.buildWriteThrough(providerName, attributes);
      ensureUrnEndsWithPropertyKey(urn, key);
      urns.add(urn);
    }
    return List.copyOf(urns);
  }

  /**
   * Writes plaintext secrets from {@code secretBindings} values into the write-through providers
   * for {@code secretUrns} (e.g. Vault).
   *
   * <p>{@code secretUrns} must come from {@link #getSecretBindingUrns}. On failure, already-written
   * URNs are rolled back. Callers must put URN strings into properties themselves (e.g. via {@link
   * SecretPropertyUtils#putSecretUrns}).
   *
   * @param secretBindings property key → write-through binding (null or empty is a no-op)
   * @param secretUrns write-through URNs from {@link #getSecretBindingUrns}
   */
  public void writeSecrets(
      @Nullable Map<String, SecretBinding> secretBindings, @Nullable List<SecretUrn> secretUrns) {
    if (secretBindings == null || secretBindings.isEmpty()) {
      if (secretUrns != null && !secretUrns.isEmpty()) {
        throw new IllegalArgumentException("secretUrns must be empty when bindings are empty");
      }
      return;
    }
    if (secretUrns == null || secretUrns.isEmpty()) {
      throw new IllegalArgumentException("secretUrns must not be null or empty");
    }
    validateSecretBindings(secretBindings);

    List<SecretUrn> written = new ArrayList<>(secretUrns.size());
    try {
      for (SecretUrn urn : secretUrns) {
        List<String> segments = urn.identifierSegments();
        if (segments.size() != 3) {
          throw new IllegalArgumentException(
              "Write-through secret URN must have entityType, entityId, propertyKey segments: "
                  + urn);
        }
        String entityType = segments.get(0);
        String entityId = segments.get(1);
        String propertyKey = urn.propertyKey();
        SecretBinding binding = secretBindings.get(propertyKey);
        if (binding == null) {
          throw new IllegalArgumentException(
              "No secretBindings entry for property key \"" + propertyKey + "\"");
        }
        Map<String, String> attributes =
            ImmutableMap.of(
                ATTR_ENTITY_TYPE, entityType,
                ATTR_ENTITY_ID, entityId,
                ATTR_PROPERTY_KEY, propertyKey);
        SecretUrn writtenUrn =
            registry.getProvider(urn.providerName()).writeSecret(binding.plaintext(), attributes);
        if (!urn.equals(writtenUrn)) {
          throw new IllegalArgumentException(
              "Provider returned unexpected URN: expected " + urn + ", got " + writtenUrn);
        }
        written.add(writtenUrn);
      }
    } catch (RuntimeException e) {
      rollbackWritten(written);
      throw e;
    }
  }

  /**
   * Best-effort delete of write-through secrets after a failed create.
   *
   * <p>Only write-through URNs that were persisted by {@link #writeSecrets} may be passed. Do not
   * roll back external reference URNs from {@link #getSecretReferenceUrns}.
   *
   * @param secretUrns write-through URNs from {@link #getSecretBindingUrns}
   */
  public void rollbackWritten(@Nullable List<SecretUrn> secretUrns) {
    if (secretUrns == null || secretUrns.isEmpty()) {
      return;
    }
    for (SecretUrn urn : secretUrns) {
      try {
        registry.getProvider(urn.providerName()).deleteSecret(urn);
      } catch (Exception e) {
        LOG.warn("Failed to roll back written secret {}", urn, e);
      }
    }
  }

  /**
   * Reads plaintext for a secret URN via the provider named in the URN.
   *
   * @param urn the secret URN
   * @return the secret plaintext
   */
  public String readSecret(SecretUrn urn) {
    if (urn == null) {
      throw new IllegalArgumentException("urn must not be null");
    }
    return registry.getProvider(urn.providerName()).readSecret(urn);
  }

  @Override
  public void close() {
    registry.close();
  }

  private static void ensureUrnEndsWithPropertyKey(SecretUrn urn, String propertyKey) {
    if (!urn.propertyKey().equals(propertyKey)) {
      throw new IllegalArgumentException(
          "Built secret URN must end with property key \"" + propertyKey + "\": " + urn);
    }
  }
}
