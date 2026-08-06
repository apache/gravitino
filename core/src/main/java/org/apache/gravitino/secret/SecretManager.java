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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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
   * Builds external-reference URNs from {@code secretReferences} without writing secret material.
   *
   * <p>Callers must put the returned URN strings into properties themselves (e.g. via {@link
   * SecretPropertyUtils#applySecretUrns}). External-ref URNs are owned outside Gravitino and must
   * not be passed to {@link #rollbackWritten}.
   *
   * @param secretReferences property key → secret locator
   * @return external-reference URNs (insertion order)
   */
  public List<SecretUrn> getSecretReferenceUrns(Map<String, SecretReference> secretReferences) {
    Preconditions.checkArgument(
        secretReferences != null && !secretReferences.isEmpty(),
        "secretReferences must not be null or empty");
    validateSecretReferences(secretReferences);

    List<SecretUrn> urns = new ArrayList<>(secretReferences.size());
    for (Map.Entry<String, SecretReference> entry : secretReferences.entrySet()) {
      String key = entry.getKey();
      SecretReference locator = entry.getValue();
      String providerName = locator.provider();
      SecretProvider provider = registry.getProvider(providerName);
      try {
        SecretUrn urn = provider.buildReferenceUrn(key, locator.attributes());
        validateUrnEndsWithPropertyKey(urn, key);
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
   * SecretPropertyUtils#applySecretUrns}).
   *
   * @param entityType {@code catalog}, {@code schema}, or {@code fileset}
   * @param entityId stable numeric entity id
   * @param secretBindings property key → write-through binding
   * @return write-through URNs (insertion order)
   */
  public List<SecretUrn> getSecretBindingUrns(
      String entityType, long entityId, Map<String, SecretBinding> secretBindings) {
    Preconditions.checkArgument(StringUtils.isNotBlank(entityType), "entityType must not be blank");
    Preconditions.checkArgument(
        secretBindings != null && !secretBindings.isEmpty(),
        "secretBindings must not be null or empty");
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
      validateUrnEndsWithPropertyKey(urn, key);
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
   * SecretPropertyUtils#applySecretUrns}).
   *
   * @param secretBindings property key → write-through binding
   * @param secretUrns write-through URNs from {@link #getSecretBindingUrns}
   */
  public void writeSecrets(Map<String, SecretBinding> secretBindings, List<SecretUrn> secretUrns) {
    Preconditions.checkArgument(
        secretBindings != null && !secretBindings.isEmpty(),
        "secretBindings must not be null or empty");
    Preconditions.checkArgument(
        secretUrns != null && !secretUrns.isEmpty(), "secretUrns must not be null or empty");
    validateSecretBindings(secretBindings);

    List<SecretUrn> written = new ArrayList<>(secretUrns.size());
    try {
      for (SecretUrn urn : secretUrns) {
        List<String> segments = urn.identifierSegments();
        Preconditions.checkArgument(
            segments.size() == 3,
            "Write-through secret URN must have entityType, entityId, propertyKey segments: %s",
            urn);
        String entityType = segments.get(0);
        String entityId = segments.get(1);
        String propertyKey = segments.get(2);
        SecretBinding binding = secretBindings.get(propertyKey);
        Preconditions.checkArgument(
            binding != null, "No secretBindings entry for property key \"%s\"", propertyKey);
        String plaintext = binding.plaintext();
        Map<String, String> attributes =
            ImmutableMap.of(
                ATTR_ENTITY_TYPE, entityType,
                ATTR_ENTITY_ID, entityId,
                ATTR_PROPERTY_KEY, propertyKey);
        SecretUrn writtenUrn =
            registry.getProvider(urn.providerName()).writeSecret(plaintext, attributes);
        Preconditions.checkArgument(
            urn.equals(writtenUrn),
            "Provider returned unexpected URN: expected %s, got %s",
            urn,
            writtenUrn);
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
    Preconditions.checkArgument(urn != null, "urn must not be null");
    return registry.getProvider(urn.providerName()).readSecret(urn);
  }

  @Override
  public void close() {
    registry.close();
  }

  private static void validateUrnEndsWithPropertyKey(SecretUrn urn, String propertyKey) {
    Preconditions.checkArgument(
        urn.toString().endsWith(propertyKey),
        "Built secret URN must end with property key \"%s\": %s",
        propertyKey,
        urn);
  }
}
