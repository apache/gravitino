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
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.gravitino.Config;
import org.apache.gravitino.secret.memory.InMemorySecretsProvider;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestSecretManager {

  @Test
  void testWriteSecrets() {
    try (SecretManager secretManager = memorySecretManager()) {
      Map<String, String> properties = new HashMap<>(Map.of("jdbc-user", "root"));
      Map<String, SecretBinding> secretBindings =
          Map.of("jdbc-password", new SecretBinding("memory", "s3cr3t"));
      List<SecretUrn> secretUrns =
          secretManager.getSecretBindingUrns("catalog", 42L, secretBindings);
      secretManager.writeSecrets(secretBindings, secretUrns);
      SecretPropertyUtils.putSecretUrns(properties, secretUrns);

      String urn = properties.get("jdbc-password");
      Assertions.assertTrue(SecretPropertyUtils.isSecretProperty("jdbc-password", urn));
      Assertions.assertEquals("root", properties.get("jdbc-user"));
      Assertions.assertEquals(1, secretUrns.size());
      Assertions.assertEquals(
          "s3cr3t",
          secretManager.getRegistry().getProvider("memory").readSecret(secretUrns.get(0)));
      Assertions.assertEquals("s3cr3t", secretManager.readSecret(secretUrns.get(0)));
    }
  }

  @Test
  void testToPlaintextPropertiesReplacesUrnsWithPlaintext() {
    try (SecretManager secretManager = memorySecretManager()) {
      Map<String, String> properties = new HashMap<>();
      properties.put("jdbc-user", "root");
      Map<String, SecretBinding> secretBindings =
          Map.of("jdbc-password", new SecretBinding("memory", "s3cr3t"));
      List<SecretUrn> secretUrns =
          secretManager.getSecretBindingUrns("catalog", 42L, secretBindings);
      SecretPropertyUtils.putSecretUrns(properties, secretUrns);
      secretManager.writeSecrets(secretBindings, secretUrns);

      String urn = properties.get("jdbc-password");
      Assertions.assertTrue(SecretPropertyUtils.isSecretProperty("jdbc-password", urn));

      Map<String, String> plaintext = secretManager.toPlaintextProperties(properties);

      Assertions.assertEquals("root", plaintext.get("jdbc-user"));
      Assertions.assertEquals("s3cr3t", plaintext.get("jdbc-password"));
      // Stored properties keep the URN.
      Assertions.assertEquals(urn, properties.get("jdbc-password"));
    }
  }

  @Test
  void testToPlaintextPropertiesNullOrEmpty() {
    try (SecretManager secretManager = memorySecretManager()) {
      Assertions.assertTrue(secretManager.toPlaintextProperties(null).isEmpty());
      Assertions.assertTrue(secretManager.toPlaintextProperties(Map.of()).isEmpty());
    }
  }

  @Test
  void testGetSecretReferenceUrnsRejectedByMemoryProvider() {
    try (SecretManager secretManager = memorySecretManager()) {
      Assertions.assertThrows(
          IllegalArgumentException.class,
          () ->
              secretManager.getSecretReferenceUrns(
                  Map.of(
                      "jdbc-password",
                      new SecretReference("memory", Map.of("path", "secret/data/x")))));
    }
  }

  @Test
  void testRejectOverlapAndInvalidBinding() {
    try (SecretManager secretManager = memorySecretManager()) {
      Assertions.assertThrows(
          IllegalArgumentException.class,
          () ->
              secretManager.checkSecretKeys(
                  Map.of(),
                  Map.of("jdbc-password", new SecretBinding("memory", "s3cr3t")),
                  Map.of(
                      "jdbc-password",
                      new SecretReference("memory", Map.of("path", "secret/data/x")))));
      Assertions.assertThrows(
          IllegalArgumentException.class,
          () ->
              secretManager.checkSecretKeys(
                  Map.of("jdbc-password", "plain"),
                  Map.of("jdbc-password", new SecretBinding("memory", "s3cr3t")),
                  Map.of()));

      Assertions.assertThrows(
          IllegalArgumentException.class, () -> new SecretBinding(" ", "s3cr3t"));

      Assertions.assertThrows(
          IllegalArgumentException.class,
          () ->
              secretManager.getSecretReferenceUrns(
                  Map.of(
                      "jdbc-password",
                      new SecretReference("memory", Map.of("path", "secret/data/x")))));
    }
  }

  @Test
  void testSecretBindingToStringRedactsPlaintext() {
    SecretBinding binding = new SecretBinding("memory", "s3cr3t");
    Assertions.assertFalse(binding.toString().contains("s3cr3t"));
    Assertions.assertTrue(binding.toString().contains("***"));
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
