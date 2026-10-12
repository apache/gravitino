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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.secret;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.gravitino.CatalogChange;
import org.apache.gravitino.Config;
import org.apache.gravitino.secret.memory.InMemorySecretsProvider;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestSecretManagerAlter {

  @Test
  void testAlterSetSecretBindingWritesAndReturnsUrn() {
    try (SecretManager secretManager = memorySecretManager()) {
      Map<String, String> props = new HashMap<>(Map.of("jdbc-user", "root"));
      List<SecretMaterial> written = new ArrayList<>();
      List<SecretUrn> replacedUrns = new ArrayList<>();
      String urn =
          secretManager.alterSetSecretBinding(
              props,
              "catalog",
              42L,
              "jdbc-password",
              new SecretBinding("memory", "s3cr3t"),
              written,
              replacedUrns);
      Assertions.assertTrue(replacedUrns.isEmpty());

      Assertions.assertTrue(
          SecretPropertyUtils.isWriteThroughForEntity("jdbc-password", urn, "catalog", 42L));
      Assertions.assertEquals(urn, props.get("jdbc-password"));
      Assertions.assertEquals(1, written.size());
      Assertions.assertEquals(
          "s3cr3t",
          secretManager.getRegistry().getProvider("memory").readSecret(written.get(0).urn()));
    }
  }

  @Test
  void testAlterRemovePropertyDefersWriteThroughSecretDeletion() {
    try (SecretManager secretManager = memorySecretManager()) {
      Map<String, String> props = new HashMap<>();
      List<SecretMaterial> written = new ArrayList<>();
      List<SecretUrn> replacedUrns = new ArrayList<>();
      String urn =
          secretManager.alterSetSecretBinding(
              props,
              "catalog",
              7L,
              "jdbc-password",
              new SecretBinding("memory", "old"),
              written,
              replacedUrns);

      secretManager.alterRemoveProperty(props, "catalog", 7L, "jdbc-password", replacedUrns);

      Assertions.assertFalse(props.containsKey("jdbc-password"));
      // Deletion is deferred until the alter commits: the removed URN must stay
      // resolvable while the alter may still abort.
      Assertions.assertEquals(
          "old",
          secretManager.getRegistry().getProvider("memory").readSecret(SecretUrn.parse(urn)));
      Assertions.assertEquals(1, replacedUrns.size());
      Assertions.assertEquals(SecretUrn.parse(urn), replacedUrns.get(0));

      // After the alter commits, the caller deletes the collected URN.
      secretManager.deleteSecrets(replacedUrns);
      Assertions.assertThrows(
          IllegalArgumentException.class,
          () -> secretManager.getRegistry().getProvider("memory").readSecret(SecretUrn.parse(urn)));
    }
  }

  @Test
  void testAlterRemovePropertyKeepsExternalReferenceSecret() {
    try (SecretManager secretManager = memorySecretManager()) {
      // External-ref URNs are not entityType:entityId:propertyKey write-through shapes.
      String externalUrn = "urn:gravitino-secret:memory:path:kv/jdbc-password:jdbc-password";
      Assertions.assertTrue(SecretPropertyUtils.isSecretProperty("jdbc-password", externalUrn));
      Assertions.assertFalse(
          SecretPropertyUtils.isWriteThroughForEntity("jdbc-password", externalUrn, "catalog", 7L));

      Map<String, String> props = new HashMap<>();
      props.put("jdbc-password", externalUrn);

      // Seed an unrelated write-through secret that must survive removing the external-ref key.
      List<SecretMaterial> written = new ArrayList<>();
      String ownedUrn =
          secretManager.alterSetSecretBinding(
              new HashMap<>(),
              "catalog",
              7L,
              "other-secret",
              new SecretBinding("memory", "keep-me"),
              written,
              new ArrayList<>());

      secretManager.alterRemoveProperty(props, "catalog", 7L, "jdbc-password", new ArrayList<>());

      Assertions.assertEquals(
          "keep-me",
          secretManager.getRegistry().getProvider("memory").readSecret(SecretUrn.parse(ownedUrn)));
    }
  }

  @Test
  void testSchemaAlterRemovePropertyDefersWriteThroughSecretDeletion() {
    try (SecretManager secretManager = memorySecretManager()) {
      Map<String, String> props = new HashMap<>();
      List<SecretMaterial> written = new ArrayList<>();
      List<SecretUrn> replacedUrns = new ArrayList<>();
      String urn =
          secretManager.alterSetSecretBinding(
              props, "schema", 9L, "k2", new SecretBinding("memory", "old"), written, replacedUrns);

      secretManager.alterRemoveProperty(props, "schema", 9L, "k2", replacedUrns);

      Assertions.assertEquals(
          "old",
          secretManager.getRegistry().getProvider("memory").readSecret(SecretUrn.parse(urn)));
      secretManager.deleteSecrets(replacedUrns);
      Assertions.assertThrows(
          IllegalArgumentException.class,
          () -> secretManager.getRegistry().getProvider("memory").readSecret(SecretUrn.parse(urn)));
    }
  }

  @Test
  void testFilesetAlterRemovePropertyDefersWriteThroughSecretDeletion() {
    try (SecretManager secretManager = memorySecretManager()) {
      Map<String, String> props = new HashMap<>();
      List<SecretMaterial> written = new ArrayList<>();
      List<SecretUrn> replacedUrns = new ArrayList<>();
      String urn =
          secretManager.alterSetSecretBinding(
              props,
              "fileset",
              11L,
              "k2",
              new SecretBinding("memory", "old"),
              written,
              replacedUrns);

      secretManager.alterRemoveProperty(props, "fileset", 11L, "k2", replacedUrns);

      Assertions.assertEquals(
          "old",
          secretManager.getRegistry().getProvider("memory").readSecret(SecretUrn.parse(urn)));
      secretManager.deleteSecrets(replacedUrns);
      Assertions.assertThrows(
          IllegalArgumentException.class,
          () -> secretManager.getRegistry().getProvider("memory").readSecret(SecretUrn.parse(urn)));
    }
  }

  @Test
  void testRejectMaskedSetPropertyAndRawUrn() {
    try (SecretManager secretManager = memorySecretManager()) {
      Assertions.assertThrows(
          IllegalArgumentException.class,
          () ->
              secretManager.alterSetProperty(
                  new HashMap<>(), "catalog", 1L, "jdbc-password", "******"));
      Assertions.assertThrows(
          IllegalArgumentException.class,
          () ->
              secretManager.alterSetProperty(
                  new HashMap<>(),
                  "catalog",
                  1L,
                  "jdbc-password",
                  "urn:gravitino-secret:memory:catalog:1:jdbc-password"));
    }
  }

  @Test
  void testFailedPrepareKeepsReplacedSecretReadable() {
    try (SecretManager secretManager = memorySecretManager()) {
      Map<String, String> current = new HashMap<>();
      List<SecretMaterial> written = new ArrayList<>();
      List<SecretUrn> replacedUrns = new ArrayList<>();
      String urn =
          secretManager.alterSetSecretBinding(
              current,
              "catalog",
              7L,
              "jdbc-password",
              new SecretBinding("memory", "old"),
              written,
              replacedUrns);

      // The batch fails at its second change, after the first change already
      // removed the old secret. The alter aborts, so the persisted property still
      // references the old URN and its material must still resolve.
      Assertions.assertThrows(
          IllegalArgumentException.class,
          () ->
              SecretAlterChanges.prepareCatalogChanges(
                  secretManager,
                  current,
                  7L,
                  CatalogChange.removeProperty("jdbc-password"),
                  CatalogChange.setProperty("other", "******")));

      Assertions.assertEquals(
          "old",
          secretManager.getRegistry().getProvider("memory").readSecret(SecretUrn.parse(urn)));
    }
  }

  @Test
  void testFailedSameProviderRotationKeepsUrnResolvable() {
    try (SecretManager secretManager = memorySecretManager()) {
      Map<String, String> current = new HashMap<>();
      List<SecretMaterial> written = new ArrayList<>();
      List<SecretUrn> replacedUrns = new ArrayList<>();
      String urn =
          secretManager.alterSetSecretBinding(
              current,
              "catalog",
              8L,
              "jdbc-password",
              new SecretBinding("memory", "old"),
              written,
              replacedUrns);

      // The batch fails at its second change. The first change overwrote the
      // deterministic URN in place; the failed alter must not delete that URN,
      // because the persisted entity still references it.
      Assertions.assertThrows(
          IllegalArgumentException.class,
          () ->
              SecretAlterChanges.prepareCatalogChanges(
                  secretManager,
                  current,
                  8L,
                  CatalogChange.setSecretBinding(
                      "jdbc-password", new SecretBinding("memory", "new")),
                  CatalogChange.setProperty("other", "******")));

      Assertions.assertNotNull(
          secretManager.getRegistry().getProvider("memory").readSecret(SecretUrn.parse(urn)));
    }
  }

  @Test
  void testFailedRemoveAndRebindKeepsUrnResolvable() {
    try (SecretManager secretManager = memorySecretManager()) {
      Map<String, String> current = new HashMap<>();
      List<SecretMaterial> written = new ArrayList<>();
      List<SecretUrn> replacedUrns = new ArrayList<>();
      String urn =
          secretManager.alterSetSecretBinding(
              current,
              "catalog",
              9L,
              "jdbc-password",
              new SecretBinding("memory", "old"),
              written,
              replacedUrns);

      // Remove and re-bind the same key in one batch, then fail the batch. The
      // re-bind rewrote the deterministic URN the persisted entity still references;
      // rolling that material back would dangle the URN.
      Assertions.assertThrows(
          IllegalArgumentException.class,
          () ->
              SecretAlterChanges.prepareCatalogChanges(
                  secretManager,
                  current,
                  9L,
                  CatalogChange.removeProperty("jdbc-password"),
                  CatalogChange.setSecretBinding(
                      "jdbc-password", new SecretBinding("memory", "new")),
                  CatalogChange.setProperty("other", "******")));

      Assertions.assertNotNull(
          secretManager.getRegistry().getProvider("memory").readSecret(SecretUrn.parse(urn)));
    }
  }

  @Test
  void testSuccessfulPrepareCollectsAndFiltersReplacedUrns() {
    try (SecretManager secretManager = memorySecretManager()) {
      Map<String, String> current = new HashMap<>();
      List<SecretMaterial> written = new ArrayList<>();
      List<SecretUrn> replacedUrns = new ArrayList<>();
      String urn =
          secretManager.alterSetSecretBinding(
              current,
              "catalog",
              10L,
              "jdbc-password",
              new SecretBinding("memory", "old"),
              written,
              replacedUrns);

      // Removing the key then re-binding it in the same batch: the deterministic
      // URN is still referenced by the final properties, so the success path must
      // NOT collect it for deletion.
      Pair<CatalogChange[], SecretMaterialsHolder> result =
          SecretAlterChanges.prepareCatalogChanges(
              secretManager,
              current,
              10L,
              CatalogChange.removeProperty("jdbc-password"),
              CatalogChange.setSecretBinding("jdbc-password", new SecretBinding("memory", "new")));
      Assertions.assertTrue(result.getRight().getReplacedUrns().isEmpty());

      // A pure removal does collect the URN, and post-commit deletion removes it.
      Pair<CatalogChange[], SecretMaterialsHolder> removed =
          SecretAlterChanges.prepareCatalogChanges(
              secretManager, current, 10L, CatalogChange.removeProperty("jdbc-password"));
      Assertions.assertEquals(List.of(SecretUrn.parse(urn)), removed.getRight().getReplacedUrns());
      removed.getRight().deleteReplaced(secretManager);
      Assertions.assertThrows(
          IllegalArgumentException.class,
          () -> secretManager.getRegistry().getProvider("memory").readSecret(SecretUrn.parse(urn)));
    }
  }

  @Test
  void testPostPrepareRollbackKeepsReboundUrnResolvable() {
    try (SecretManager secretManager = memorySecretManager()) {
      Map<String, String> current = new HashMap<>();
      List<SecretMaterial> written = new ArrayList<>();
      List<SecretUrn> replacedUrns = new ArrayList<>();
      String urn =
          secretManager.alterSetSecretBinding(
              current,
              "catalog",
              11L,
              "jdbc-password",
              new SecretBinding("memory", "old"),
              written,
              replacedUrns);

      // Prepare succeeds; the alter itself then fails (e.g. the catalog rejects it).
      // The dispatcher-level rollback uses the holder's written list, which must not
      // contain the deterministic URN the persisted entity still references.
      Pair<CatalogChange[], SecretMaterialsHolder> result =
          SecretAlterChanges.prepareCatalogChanges(
              secretManager,
              current,
              11L,
              CatalogChange.removeProperty("jdbc-password"),
              CatalogChange.setSecretBinding("jdbc-password", new SecretBinding("memory", "new")));

      secretManager.rollbackSecrets(result.getRight().get());

      Assertions.assertNotNull(
          secretManager.getRegistry().getProvider("memory").readSecret(SecretUrn.parse(urn)));
    }
  }

  private static SecretManager memorySecretManager() {
    Config config = new Config(false) {};
    Properties properties = new Properties();
    properties.setProperty(SecretProviderRegistry.GRAVITINO_SECRET_PROVIDERS, "memory");
    properties.setProperty(
        SecretProviderRegistry.GRAVITINO_SECRET_PROVIDER_PREFIX
            + "memory."
            + SecretProviderRegistry.CLASS_NAME,
        InMemorySecretsProvider.class.getName());
    config.loadFromProperties(properties);
    return new SecretManager(config);
  }
}
