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

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.gravitino.Config;
import org.apache.gravitino.secret.memory.InMemorySecretsProvider;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestSecretManager {

  private static final Map<String, SecretBinding> BINDINGS =
      Map.of("jdbc-password", new SecretBinding("memory", "s3cr3t"));
  private static final Map<String, SecretReference> REFERENCES =
      Map.of("jdbc-password", new SecretReference("memory", Map.of("path", "secret/data/x")));

  @Test
  void testStageSecretsForCreate() {
    try (SecretManager sm = memorySecretManager()) {
      Map<String, String> props = new HashMap<>(Map.of("jdbc-user", "root"));
      Map<String, String> plaintexts =
          sm.assembleSecretPlaintexts(
              Map.of("jdbc-user", "root"), props, "catalog", 42L, BINDINGS, Map.of());
      Assertions.assertEquals(Map.of("jdbc-password", "s3cr3t"), plaintexts);
      Assertions.assertEquals("root", props.get("jdbc-user"));
      SecretUrn urn = SecretUrn.parse(props.get("jdbc-password"));
      Assertions.assertTrue(
          SecretPropertyUtils.isWriteThroughForEntity(
              "jdbc-password", urn.toString(), "catalog", 42L));

      SecretVersionChanges staged = sm.stageSecrets(null, props, "catalog", 42L, plaintexts);
      // A create releases nothing and its URN has no previous version, so commit switches nothing.
      Assertions.assertEquals(1, staged.written().size());
      Assertions.assertNull(staged.written().get(0).previousId());
      Assertions.assertTrue(staged.released().isEmpty());
      Assertions.assertEquals("s3cr3t", sm.readSecret(urn));
      sm.commitSecrets(staged);
      Assertions.assertEquals("s3cr3t", sm.readSecret(urn));
    }
  }

  @Test
  void testRollbackSecretsAfterFailedCreate() {
    try (SecretManager sm = memorySecretManager()) {
      Map<String, String> props = new HashMap<>();
      Map<String, String> plaintexts =
          sm.assembleSecretPlaintexts(Map.of(), props, "catalog", 42L, BINDINGS, Map.of());
      SecretVersionChanges staged = sm.stageSecrets(null, props, "catalog", 42L, plaintexts);
      SecretUrn urn = SecretUrn.parse(props.get("jdbc-password"));
      Assertions.assertEquals("s3cr3t", sm.readSecret(urn));

      sm.rollbackSecrets(staged);
      Assertions.assertThrows(IllegalArgumentException.class, () -> sm.readSecret(urn));
      Assertions.assertNull(sm.getRegistry().getProvider("memory").currentSecretVersion(urn));
    }
  }

  @Test
  void testCreateWithUnversionedProviderDeletesVersionsWrittenBefore() {
    try (SecretManager sm = memorySecretManager()) {
      Map<String, SecretBinding> bindings = new LinkedHashMap<>();
      bindings.put("jdbc-password", new SecretBinding("memory", "s3cr3t"));
      bindings.put("token", new SecretBinding("unversioned", "t0ken"));
      Map<String, String> props = new HashMap<>();
      Map<String, String> plaintexts =
          sm.assembleSecretPlaintexts(Map.of(), props, "catalog", 42L, bindings, Map.of());

      UnsupportedOperationException error =
          Assertions.assertThrows(
              UnsupportedOperationException.class,
              () -> sm.stageSecrets(null, props, "catalog", 42L, plaintexts));
      Assertions.assertTrue(error.getMessage().contains("\"unversioned\""), error.getMessage());
      Assertions.assertTrue(error.getMessage().contains("\"token\""), error.getMessage());
      SecretUrn urn = SecretUrn.parse(props.get("jdbc-password"));
      Assertions.assertNull(sm.getRegistry().getProvider("memory").currentSecretVersion(urn));
    }
  }

  @Test
  void testValidateSecretsBeforeStageRejectsBlankPlaintext() {
    try (SecretManager sm = memorySecretManager()) {
      Map<String, String> props = new HashMap<>();
      List<SecretUrn> urns = sm.buildSecretBindingUrns("catalog", 42L, BINDINGS);
      SecretPropertyUtils.putSecretUrns(props, urns);
      IllegalArgumentException ex =
          Assertions.assertThrows(
              IllegalArgumentException.class,
              () -> sm.validateSecretsBeforeStage(props, Map.of("jdbc-password", "")));
      Assertions.assertTrue(ex.getMessage().contains("plaintext must not be blank"));
      Assertions.assertThrows(IllegalArgumentException.class, () -> sm.readSecret(urns.get(0)));
    }
  }

  @Test
  void testValidateSecretsBeforeStageRejectsMissingReference() {
    try (SecretManager sm = memorySecretManager()) {
      // memory provider cannot build reference URNs; use a write-through URN that was never
      // written.
      Map<String, String> props = new HashMap<>();
      List<SecretUrn> urns = sm.buildSecretBindingUrns("catalog", 42L, BINDINGS);
      SecretPropertyUtils.putSecretUrns(props, urns);
      IllegalArgumentException ex =
          Assertions.assertThrows(
              IllegalArgumentException.class, () -> sm.validateSecretsBeforeStage(props, Map.of()));
      Assertions.assertTrue(
          ex.getMessage().contains("Secret not found") || ex.getMessage().contains("not found"),
          ex.getMessage());
    }
  }

  @Test
  void testValidateSecretsBeforeStageAcceptsBindingOverlay() {
    try (SecretManager sm = memorySecretManager()) {
      Map<String, String> props = new HashMap<>();
      Map<String, String> plaintexts =
          sm.assembleSecretPlaintexts(Map.of(), props, "catalog", 42L, BINDINGS, Map.of());
      Assertions.assertDoesNotThrow(() -> sm.validateSecretsBeforeStage(props, plaintexts));
      Assertions.assertThrows(
          IllegalArgumentException.class,
          () -> sm.readSecret(SecretUrn.parse(props.get("jdbc-password"))));
    }
  }

  @Test
  void testToPlaintextProperties() {
    try (SecretManager sm = memorySecretManager()) {
      Assertions.assertTrue(sm.toPlaintextProperties(null).isEmpty());
      Assertions.assertTrue(sm.toPlaintextProperties(Map.of()).isEmpty());

      Map<String, String> props = new HashMap<>(Map.of("jdbc-user", "root"));
      List<SecretUrn> urns = sm.buildSecretBindingUrns("catalog", 42L, BINDINGS);
      SecretPropertyUtils.putSecretUrns(props, urns);
      sm.stageSecrets(null, props, "catalog", 42L, Map.of("jdbc-password", "s3cr3t"));

      String urn = props.get("jdbc-password");
      Map<String, String> plaintext = sm.toPlaintextProperties(props);
      Assertions.assertEquals("root", plaintext.get("jdbc-user"));
      Assertions.assertEquals("s3cr3t", plaintext.get("jdbc-password"));
      Assertions.assertEquals(urn, props.get("jdbc-password"));
    }
  }

  @Test
  void testToPlaintextOrMaskedWhenMissing() {
    try (SecretManager sm = memorySecretManager()) {
      Map<String, String> props = new HashMap<>();
      List<SecretUrn> urns = sm.buildSecretBindingUrns("catalog", 42L, BINDINGS);
      SecretPropertyUtils.putSecretUrns(props, urns);
      Assertions.assertThrows(
          IllegalArgumentException.class, () -> sm.toPlaintextProperties(props));
      Map<String, String> masked = sm.toPlaintextOrMasked(props);
      Assertions.assertEquals(
          org.apache.gravitino.connector.HiddenPropertyMaskUtils.MASKED_VALUE,
          masked.get("jdbc-password"));
    }
  }

  @Test
  void testAssembleRejectsMaskedPlaceholderPlaintext() {
    try (SecretManager sm = memorySecretManager()) {
      Map<String, String> entityProps = new HashMap<>();
      Map<String, SecretBinding> bindings =
          Map.of(
              "jdbc-password",
              new SecretBinding(
                  "memory", org.apache.gravitino.connector.HiddenPropertyMaskUtils.MASKED_VALUE));
      IllegalArgumentException ex =
          Assertions.assertThrows(
              IllegalArgumentException.class,
              () ->
                  sm.assembleSecretPlaintexts(
                      Map.of(), entityProps, "catalog", 42L, bindings, Map.of()));
      Assertions.assertTrue(ex.getMessage().contains("masked placeholder"));
      Assertions.assertTrue(ex.getMessage().contains("jdbc-password"));
    }
  }

  @Test
  void testRejectReferenceUrns() {
    try (SecretManager sm = memorySecretManager()) {
      Assertions.assertThrows(
          IllegalArgumentException.class, () -> sm.buildSecretReferenceUrns(REFERENCES));
    }
  }

  @Test
  void testCheckSecretKeys() {
    try (SecretManager sm = memorySecretManager()) {
      Assertions.assertThrows(
          IllegalArgumentException.class, () -> sm.checkSecretKeys(Map.of(), BINDINGS, REFERENCES));
      Assertions.assertThrows(
          IllegalArgumentException.class,
          () -> sm.checkSecretKeys(Map.of("jdbc-password", "plain"), BINDINGS, Map.of()));

      List<SecretUrn> urns = sm.buildSecretBindingUrns("catalog", 42L, BINDINGS);
      Assertions.assertThrows(
          IllegalArgumentException.class,
          () ->
              sm.checkSecretKeys(
                  Map.of("jdbc-password", urns.get(0).toString()), Map.of(), Map.of()));

      Assertions.assertThrows(
          IllegalArgumentException.class, () -> new SecretBinding(" ", "s3cr3t"));
    }
  }

  @Test
  void testBindingRedacts() {
    SecretBinding binding = new SecretBinding("memory", "s3cr3t");
    Assertions.assertFalse(binding.toString().contains("s3cr3t"));
    Assertions.assertTrue(binding.toString().contains("***"));
  }

  @Test
  void testDeleteBindingsFromProperties() {
    try (SecretManager sm = memorySecretManager()) {
      List<SecretUrn> urns = sm.buildSecretBindingUrns("catalog", 99L, BINDINGS);
      Map<String, String> props = new HashMap<>();
      SecretPropertyUtils.putSecretUrns(props, urns);
      sm.stageSecrets(null, props, "catalog", 99L, Map.of("jdbc-password", "s3cr3t"));
      sm.deleteSecretsFromProperties(props);
      Assertions.assertThrows(IllegalArgumentException.class, () -> sm.readSecret(urns.get(0)));
    }
  }

  private static SecretManager memorySecretManager() {
    Config config = new Config(false) {};
    Properties properties = new Properties();
    properties.setProperty(SecretProviderRegistry.GRAVITINO_SECRET_PROVIDERS, "memory,unversioned");
    properties.setProperty(
        SecretProviderRegistry.GRAVITINO_SECRET_PROVIDER_PREFIX
            + "memory."
            + SecretProviderRegistry.CLASS_NAME,
        InMemorySecretsProvider.class.getName());
    properties.setProperty(
        SecretProviderRegistry.GRAVITINO_SECRET_PROVIDER_PREFIX
            + "unversioned."
            + SecretProviderRegistry.CLASS_NAME,
        TestSecretManagerAlter.UnversionedSecretsProvider.class.getName());
    config.loadFromProperties(properties);
    return new SecretManager(config);
  }
}
