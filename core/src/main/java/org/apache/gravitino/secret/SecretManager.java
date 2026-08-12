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
import java.util.HashMap;
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
   * secretBindings}, and {@code secretReferences}, and that {@code properties} do not contain raw
   * secret URN values.
   *
   * <p>{@code null} maps are treated as empty. Callers must bind/reference secrets via the typed
   * maps rather than embedding {@code urn:gravitino-secret:} values in properties.
   *
   * @param properties entity properties from the create request (may be null)
   * @param secretBindings property key → write-through binding (may be null)
   * @param secretReferences property key → secret locator (may be null)
   */
  public void checkSecretKeys(
      @Nullable Map<String, String> properties,
      @Nullable Map<String, SecretBinding> secretBindings,
      @Nullable Map<String, SecretReference> secretReferences) {
    // Reject raw URNs in request properties: they bypass typed secretBindings/secretReferences
    // and can be used to resolve another entity's secret.
    if (properties != null && !properties.isEmpty()) {
      for (Map.Entry<String, String> entry : properties.entrySet()) {
        String key = entry.getKey();
        String value = entry.getValue();
        if (SecretPropertyUtils.isSecretProperty(key, value)) {
          throw new IllegalArgumentException(
              "Property \""
                  + key
                  + "\" must not contain a raw gravitino secret URN; use secretBindings or"
                  + " secretReferences instead");
        }
      }
    }
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
   * Checks secret keys and puts reference / write-through URN strings into {@code
   * targetProperties}.
   *
   * <p>Callers typically validate {@code targetProperties}, then call {@link #writeSecrets} with
   * the returned secret materials, and {@link #rollbackSecrets} on failure. {@code properties} is
   * used only for key uniqueness checks (e.g. the original request map); {@code targetProperties}
   * is the mutable map that will be stored (may already contain merged catalog conf).
   *
   * @param properties properties used for key uniqueness checks (may be null)
   * @param targetProperties mutable properties that receive URN values (may be null only when both
   *     secret maps are null or empty)
   * @param entityType {@code catalog}, {@code schema}, or {@code fileset}
   * @param entityId stable numeric entity id
   * @param secretBindings property key → write-through binding (may be null)
   * @param secretReferences property key → secret locator (may be null)
   * @return write-through secret materials for {@link #writeSecrets} / {@link #rollbackSecrets}
   *     (empty when there are no bindings)
   */
  public List<SecretMaterial> assembleSecretMaterials(
      @Nullable Map<String, String> properties,
      @Nullable Map<String, String> targetProperties,
      String entityType,
      long entityId,
      @Nullable Map<String, SecretBinding> secretBindings,
      @Nullable Map<String, SecretReference> secretReferences) {
    checkSecretKeys(properties, secretBindings, secretReferences);
    if (!SecretPropertyUtils.hasSecretMaps(secretBindings, secretReferences)) {
      return List.of();
    }
    Preconditions.checkArgument(
        targetProperties != null, "targetProperties must not be null when secrets are present");
    Map<String, SecretBinding> bindings = secretBindings == null ? Map.of() : secretBindings;
    Map<String, SecretReference> references =
        secretReferences == null ? Map.of() : secretReferences;
    SecretPropertyUtils.putSecretUrns(targetProperties, buildSecretReferenceUrns(references));
    List<SecretUrn> bindingUrns = buildSecretBindingUrns(entityType, entityId, bindings);
    SecretPropertyUtils.putSecretUrns(targetProperties, bindingUrns);
    return toSecretMaterials(bindingUrns, bindings);
  }

  /**
   * Builds external-reference URNs from {@code secretReferences} without writing secret material.
   *
   * <p>Callers must put the returned URN strings into properties themselves (e.g. via {@link
   * SecretPropertyUtils#putSecretUrns}). External-ref URNs are owned outside Gravitino and must not
   * be passed to {@link #rollbackSecrets}.
   *
   * @param secretReferences property key → secret locator (empty returns an empty list; must not be
   *     null)
   * @return external-reference URNs (insertion order)
   */
  public List<SecretUrn> buildSecretReferenceUrns(Map<String, SecretReference> secretReferences) {
    Preconditions.checkArgument(secretReferences != null, "secretReferences must not be null");
    if (secretReferences.isEmpty()) {
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
   * <p>Callers typically pass the returned URNs to {@link #writeSecrets} via {@link
   * #assembleSecretMaterials}, or put URN strings into properties themselves (e.g. via {@link
   * SecretPropertyUtils#putSecretUrns}).
   *
   * @param entityType {@code catalog}, {@code schema}, or {@code fileset}
   * @param entityId stable numeric entity id
   * @param secretBindings property key → write-through binding (empty returns an empty list; must
   *     not be null)
   * @return write-through URNs (insertion order)
   */
  public List<SecretUrn> buildSecretBindingUrns(
      String entityType, long entityId, Map<String, SecretBinding> secretBindings) {
    Preconditions.checkArgument(StringUtils.isNotBlank(entityType), "entityType must not be blank");
    Preconditions.checkArgument(secretBindings != null, "secretBindings must not be null");
    if (secretBindings.isEmpty()) {
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
      validateUrnEndsWithPropertyKey(urn, key);
      urns.add(urn);
    }
    return List.copyOf(urns);
  }

  /**
   * Writes plaintext secrets into the write-through providers for each {@link SecretMaterial} (e.g.
   * Vault).
   *
   * <p>{@code secretMaterials} must come from {@link #assembleSecretMaterials}. On failure,
   * already-written URNs are rolled back. When using {@link #assembleSecretMaterials}, URN strings
   * are already in properties; otherwise callers must put them themselves (e.g. via {@link
   * SecretPropertyUtils#putSecretUrns}).
   *
   * @param secretMaterials write-through secret materials (empty is a no-op; must not be null)
   */
  public void writeSecrets(List<SecretMaterial> secretMaterials) {
    Preconditions.checkArgument(secretMaterials != null, "secretMaterials must not be null");
    if (secretMaterials.isEmpty()) {
      return;
    }

    List<SecretUrn> written = new ArrayList<>(secretMaterials.size());
    try {
      for (SecretMaterial material : secretMaterials) {
        SecretUrn urn = material.urn();
        List<String> segments = urn.identifierSegments();
        Preconditions.checkArgument(
            segments.size() == 3,
            "Write-through secret URN must have entityType, entityId, propertyKey segments: %s",
            urn);
        String entityType = segments.get(0);
        String entityId = segments.get(1);
        String propertyKey = urn.propertyKey();
        Map<String, String> attributes =
            ImmutableMap.of(
                ATTR_ENTITY_TYPE, entityType,
                ATTR_ENTITY_ID, entityId,
                ATTR_PROPERTY_KEY, propertyKey);
        SecretUrn writtenUrn =
            registry.getProvider(urn.providerName()).writeSecret(material.plaintext(), attributes);
        if (!urn.equals(writtenUrn)) {
          throw new IllegalArgumentException(
              "Provider returned unexpected URN: expected " + urn + ", got " + writtenUrn);
        }
        written.add(writtenUrn);
      }
    } catch (RuntimeException e) {
      deleteSecrets(written);
      throw e;
    }
  }

  /**
   * Best-effort delete of write-through secret materials after a failed create.
   *
   * <p>Only secrets that were persisted by {@link #writeSecrets} may be passed. Do not roll back
   * external reference URNs from {@link #buildSecretReferenceUrns}.
   *
   * @param secretMaterials write-through secret materials (must not be null)
   */
  public void rollbackSecrets(List<SecretMaterial> secretMaterials) {
    Preconditions.checkArgument(secretMaterials != null, "secretMaterials must not be null");
    if (secretMaterials.isEmpty()) {
      return;
    }
    List<SecretUrn> urns = new ArrayList<>(secretMaterials.size());
    for (SecretMaterial material : secretMaterials) {
      urns.add(material.urn());
    }
    deleteSecrets(urns);
  }

  private void deleteSecrets(List<SecretUrn> secretUrns) {
    if (secretUrns.isEmpty()) {
      return;
    }
    for (SecretUrn urn : secretUrns) {
      try {
        registry.getProvider(urn.providerName()).deleteSecret(urn);
      } catch (Exception e) {
        LOG.warn("Failed to delete secret {}", urn, e);
      }
    }
  }

  /**
   * Best-effort delete of write-through secrets whose URN values appear in entity properties.
   *
   * <p>Used when dropping an entity so create-time write-through secrets are not left orphaned
   * (including CatalogHookDispatcher post-hook rollback via {@code dropCatalog}). External
   * reference URNs from {@code secretReferences} are left untouched.
   *
   * @param properties persisted entity properties (may be null)
   */
  public void deleteSecretsFromProperties(@Nullable Map<String, String> properties) {
    if (properties == null || properties.isEmpty()) {
      return;
    }
    List<SecretUrn> writeThrough = new ArrayList<>();
    for (Map.Entry<String, String> entry : properties.entrySet()) {
      if (!SecretPropertyUtils.isSecretProperty(entry.getKey(), entry.getValue())) {
        continue;
      }
      try {
        SecretUrn urn = SecretUrn.parse(entry.getValue());
        // Write-through URNs are entityType:entityId:propertyKey (3 segments).
        if (urn.identifierSegments().size() == 3) {
          writeThrough.add(urn);
        }
      } catch (IllegalArgumentException e) {
        LOG.warn("Skipping invalid secret URN in properties for key {}", entry.getKey(), e);
      }
    }
    deleteSecrets(writeThrough);
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

  /**
   * Returns a copy of {@code properties} with secret URN values replaced by plaintext.
   *
   * <p>Used so connectors receive plaintext conf while entity storage keeps URN strings. Non-secret
   * entries are copied unchanged.
   *
   * @param properties entity or request properties (may be null)
   * @return a new map with secret URNs replaced by plaintext; empty map when {@code properties} is
   *     null
   */
  public Map<String, String> toPlaintextProperties(@Nullable Map<String, String> properties) {
    if (properties == null || properties.isEmpty()) {
      return properties == null ? Map.of() : Map.copyOf(properties);
    }
    Map<String, String> plaintext = new HashMap<>(properties.size());
    for (Map.Entry<String, String> entry : properties.entrySet()) {
      String key = entry.getKey();
      String value = entry.getValue();
      if (SecretPropertyUtils.isSecretProperty(key, value)) {
        plaintext.put(key, readSecret(SecretUrn.parse(value)));
      } else {
        plaintext.put(key, value);
      }
    }
    return plaintext;
  }

  @Override
  public void close() {
    registry.close();
  }

  private static List<SecretMaterial> toSecretMaterials(
      List<SecretUrn> bindingUrns, Map<String, SecretBinding> bindings) {
    List<SecretMaterial> secretMaterials = new ArrayList<>(bindingUrns.size());
    for (SecretUrn urn : bindingUrns) {
      String propertyKey = urn.propertyKey();
      SecretBinding binding = bindings.get(propertyKey);
      Preconditions.checkArgument(
          binding != null, "No secretBindings entry for property key \"%s\"", propertyKey);
      secretMaterials.add(new SecretMaterial(urn, binding.plaintext()));
    }
    return List.copyOf(secretMaterials);
  }

  private static void validateUrnEndsWithPropertyKey(SecretUrn urn, String propertyKey) {
    Preconditions.checkArgument(
        urn.toString().endsWith(propertyKey),
        "Built secret URN must end with property key \"%s\": %s",
        propertyKey,
        urn);
  }
}
