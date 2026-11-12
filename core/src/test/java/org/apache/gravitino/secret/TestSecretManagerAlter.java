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
      List<SecretVersion> pending = new ArrayList<>();
      List<SecretVersion> replacedVersions = new ArrayList<>();
      List<SecretUrn> replacedUrns = new ArrayList<>();
      String urn =
          secretManager.alterSetSecretBinding(
              props,
              "catalog",
              42L,
              "jdbc-password",
              new SecretBinding("memory", "s3cr3t"),
              written,
              pending,
              replacedVersions,
              replacedUrns);

      Assertions.assertTrue(
          SecretPropertyUtils.isWriteThroughForEntity("jdbc-password", urn, "catalog", 42L));
      Assertions.assertEquals(urn, props.get("jdbc-password"));
      Assertions.assertEquals(1, written.size());
      Assertions.assertTrue(pending.isEmpty());
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
      List<SecretVersion> pending = new ArrayList<>();
      List<SecretVersion> replacedVersions = new ArrayList<>();
      List<SecretUrn> replacedUrns = new ArrayList<>();
      String urn =
          secretManager.alterSetSecretBinding(
              props,
              "catalog",
              7L,
              "jdbc-password",
              new SecretBinding("memory", "old"),
              written,
              pending,
              replacedVersions,
              replacedUrns);

      List<SecretVersion> toDelete = new ArrayList<>();
      secretManager.alterRemoveProperty(props, "catalog", 7L, "jdbc-password", toDelete);

      Assertions.assertFalse(props.containsKey("jdbc-password"));
      Assertions.assertEquals(1, toDelete.size());
      Assertions.assertEquals(
          "old",
          secretManager.getRegistry().getProvider("memory").readSecret(SecretUrn.parse(urn)));

      SecretMaterialsHolder holder = new SecretMaterialsHolder();
      holder.setReplacedVersions(toDelete);
      holder.commit(secretManager);
      Assertions.assertThrows(
          IllegalArgumentException.class,
          () -> secretManager.getRegistry().getProvider("memory").readSecret(SecretUrn.parse(urn)));
    }
  }

  @Test
  void testFailedSameProviderRotationKeepsOldValue() {
    try (SecretManager secretManager = memorySecretManager()) {
      Map<String, String> props = new HashMap<>();
      Pair<CatalogChange[], SecretMaterialsHolder> first =
          SecretAlterChanges.prepareCatalogChanges(
              secretManager,
              props,
              3L,
              CatalogChange.setSecretBinding("jdbc-password", new SecretBinding("memory", "old")));
      first.getRight().commit(secretManager);
      String urn =
          first.getLeft()[0] instanceof CatalogChange.SetProperty
              ? ((CatalogChange.SetProperty) first.getLeft()[0]).getValue()
              : null;
      Assertions.assertNotNull(urn);
      props.put("jdbc-password", urn);

      Assertions.assertThrows(
          IllegalArgumentException.class,
          () ->
              SecretAlterChanges.prepareCatalogChanges(
                  secretManager,
                  props,
                  3L,
                  CatalogChange.setSecretBinding(
                      "jdbc-password", new SecretBinding("memory", "new")),
                  CatalogChange.setProperty("jdbc-password", "******")));

      Assertions.assertEquals(
          "old",
          secretManager.getRegistry().getProvider("memory").readSecret(SecretUrn.parse(urn)));
    }
  }

  @Test
  void testFailedPrepareKeepsReplacedSecretReadable() {
    try (SecretManager secretManager = memorySecretManager()) {
      Map<String, String> props = new HashMap<>();
      Pair<CatalogChange[], SecretMaterialsHolder> first =
          SecretAlterChanges.prepareCatalogChanges(
              secretManager,
              props,
              5L,
              CatalogChange.setSecretBinding("jdbc-password", new SecretBinding("memory", "keep")));
      first.getRight().commit(secretManager);
      String urn = ((CatalogChange.SetProperty) first.getLeft()[0]).getValue();
      props.put("jdbc-password", urn);

      Assertions.assertThrows(
          IllegalArgumentException.class,
          () ->
              SecretAlterChanges.prepareCatalogChanges(
                  secretManager,
                  props,
                  5L,
                  CatalogChange.removeProperty("jdbc-password"),
                  CatalogChange.setProperty("other", "******")));

      Assertions.assertEquals(
          "keep",
          secretManager.getRegistry().getProvider("memory").readSecret(SecretUrn.parse(urn)));
    }
  }

  @Test
  void testRotationWritesPendingVersionAndPromotesOnCommit() {
    try (SecretManager secretManager = memorySecretManager()) {
      Map<String, String> props = new HashMap<>();
      Pair<CatalogChange[], SecretMaterialsHolder> create =
          SecretAlterChanges.prepareCatalogChanges(
              secretManager,
              props,
              9L,
              CatalogChange.setSecretBinding("jdbc-password", new SecretBinding("memory", "old")));
      create.getRight().commit(secretManager);
      String urn = ((CatalogChange.SetProperty) create.getLeft()[0]).getValue();
      props.put("jdbc-password", urn);

      Pair<CatalogChange[], SecretMaterialsHolder> rotate =
          SecretAlterChanges.prepareCatalogChanges(
              secretManager,
              props,
              9L,
              CatalogChange.setSecretBinding("jdbc-password", new SecretBinding("memory", "new")));
      Assertions.assertEquals(
          "old",
          secretManager.getRegistry().getProvider("memory").readSecret(SecretUrn.parse(urn)));
      Assertions.assertEquals(1, rotate.getRight().getPendingVersions().size());

      rotate.getRight().commit(secretManager);
      Assertions.assertEquals(
          "new",
          secretManager.getRegistry().getProvider("memory").readSecret(SecretUrn.parse(urn)));
      Assertions.assertEquals(urn, ((CatalogChange.SetProperty) rotate.getLeft()[0]).getValue());
    }
  }

  @Test
  void testRemoveAndRebindKeepsRewrittenSecretAfterCommit() {
    try (SecretManager secretManager = memorySecretManager()) {
      Map<String, String> props = new HashMap<>();
      Pair<CatalogChange[], SecretMaterialsHolder> create =
          SecretAlterChanges.prepareCatalogChanges(
              secretManager,
              props,
              11L,
              CatalogChange.setSecretBinding("jdbc-password", new SecretBinding("memory", "old")));
      create.getRight().commit(secretManager);
      String urn = ((CatalogChange.SetProperty) create.getLeft()[0]).getValue();
      props.put("jdbc-password", urn);

      Pair<CatalogChange[], SecretMaterialsHolder> rebind =
          SecretAlterChanges.prepareCatalogChanges(
              secretManager,
              props,
              11L,
              CatalogChange.removeProperty("jdbc-password"),
              CatalogChange.setSecretBinding(
                  "jdbc-password", new SecretBinding("memory", "rebound")));
      rebind.getRight().commit(secretManager);

      Assertions.assertEquals(
          "rebound",
          secretManager.getRegistry().getProvider("memory").readSecret(SecretUrn.parse(urn)));
    }
  }

  @Test
  void testAlterRemovePropertyKeepsExternalReferenceSecret() {
    try (SecretManager secretManager = memorySecretManager()) {
      String externalUrn = "urn:gravitino-secret:memory:path:kv/jdbc-password:jdbc-password";
      Assertions.assertTrue(SecretPropertyUtils.isSecretProperty("jdbc-password", externalUrn));
      Assertions.assertFalse(
          SecretPropertyUtils.isWriteThroughForEntity("jdbc-password", externalUrn, "catalog", 7L));

      Map<String, String> props = new HashMap<>();
      props.put("jdbc-password", externalUrn);

      List<SecretMaterial> written = new ArrayList<>();
      List<SecretVersion> pending = new ArrayList<>();
      List<SecretVersion> replacedVersions = new ArrayList<>();
      List<SecretUrn> replacedUrns = new ArrayList<>();
      String ownedUrn =
          secretManager.alterSetSecretBinding(
              new HashMap<>(),
              "catalog",
              7L,
              "other-secret",
              new SecretBinding("memory", "keep-me"),
              written,
              pending,
              replacedVersions,
              replacedUrns);

      secretManager.alterRemoveProperty(props, "catalog", 7L, "jdbc-password", new ArrayList<>());

      Assertions.assertEquals(
          "keep-me",
          secretManager.getRegistry().getProvider("memory").readSecret(SecretUrn.parse(ownedUrn)));
    }
  }

  @Test
  void testSchemaAlterRemovePropertyDeletesWriteThroughSecretAfterCommit() {
    try (SecretManager secretManager = memorySecretManager()) {
      Map<String, String> props = new HashMap<>();
      List<SecretMaterial> written = new ArrayList<>();
      List<SecretVersion> pending = new ArrayList<>();
      List<SecretVersion> replacedVersions = new ArrayList<>();
      List<SecretUrn> replacedUrns = new ArrayList<>();
      String urn =
          secretManager.alterSetSecretBinding(
              props,
              "schema",
              9L,
              "k2",
              new SecretBinding("memory", "old"),
              written,
              pending,
              replacedVersions,
              replacedUrns);

      List<SecretVersion> toDelete = new ArrayList<>();
      secretManager.alterRemoveProperty(props, "schema", 9L, "k2", toDelete);
      SecretMaterialsHolder holder = new SecretMaterialsHolder();
      holder.setReplacedVersions(toDelete);
      holder.commit(secretManager);

      Assertions.assertThrows(
          IllegalArgumentException.class,
          () -> secretManager.getRegistry().getProvider("memory").readSecret(SecretUrn.parse(urn)));
    }
  }

  @Test
  void testFilesetAlterRemovePropertyDeletesWriteThroughSecretAfterCommit() {
    try (SecretManager secretManager = memorySecretManager()) {
      Map<String, String> props = new HashMap<>();
      List<SecretMaterial> written = new ArrayList<>();
      List<SecretVersion> pending = new ArrayList<>();
      List<SecretVersion> replacedVersions = new ArrayList<>();
      List<SecretUrn> replacedUrns = new ArrayList<>();
      String urn =
          secretManager.alterSetSecretBinding(
              props,
              "fileset",
              11L,
              "k2",
              new SecretBinding("memory", "old"),
              written,
              pending,
              replacedVersions,
              replacedUrns);

      List<SecretVersion> toDelete = new ArrayList<>();
      secretManager.alterRemoveProperty(props, "fileset", 11L, "k2", toDelete);
      SecretMaterialsHolder holder = new SecretMaterialsHolder();
      holder.setReplacedVersions(toDelete);
      holder.commit(secretManager);

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
                  new HashMap<>(),
                  "catalog",
                  1L,
                  "jdbc-password",
                  "******",
                  new ArrayList<>(),
                  new ArrayList<>()));
      Assertions.assertThrows(
          IllegalArgumentException.class,
          () ->
              secretManager.alterSetProperty(
                  new HashMap<>(),
                  "catalog",
                  1L,
                  "jdbc-password",
                  "urn:gravitino-secret:memory:catalog:1:jdbc-password",
                  new ArrayList<>(),
                  new ArrayList<>()));
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
