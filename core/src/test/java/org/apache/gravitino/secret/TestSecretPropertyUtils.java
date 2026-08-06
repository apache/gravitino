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

public class TestSecretPropertyUtils {

  @Test
  void testResolveSecretPropertiesReplacesUrnsWithPlaintext() {
    try (SecretManager secretManager = memorySecretManager()) {
      Map<String, String> properties = new HashMap<>();
      properties.put("jdbc-user", "root");
      Map<String, SecretBinding> secretBindings =
          Map.of("jdbc-password", new SecretBinding("memory", "s3cr3t"));
      List<SecretUrn> secretUrns =
          secretManager.getSecretBindingUrns("catalog", 42L, secretBindings);
      secretManager.writeSecrets(secretBindings, secretUrns);
      SecretPropertyUtils.applySecretUrns(properties, secretUrns);

      String urn = properties.get("jdbc-password");
      Assertions.assertTrue(SecretPropertyUtils.isSecretProperty("jdbc-password", urn));

      Map<String, String> resolved =
          SecretPropertyUtils.resolveSecretProperties(properties, secretManager);

      Assertions.assertEquals("root", resolved.get("jdbc-user"));
      Assertions.assertEquals("s3cr3t", resolved.get("jdbc-password"));
      // Stored properties keep the URN.
      Assertions.assertEquals(urn, properties.get("jdbc-password"));
    }
  }

  @Test
  void testResolveSecretPropertiesNullOrEmpty() {
    try (SecretManager secretManager = memorySecretManager()) {
      Assertions.assertTrue(
          SecretPropertyUtils.resolveSecretProperties(null, secretManager).isEmpty());
      Assertions.assertTrue(
          SecretPropertyUtils.resolveSecretProperties(Map.of(), secretManager).isEmpty());
    }
  }

  @Test
  void testCheckSecretKeysAndAssembleEntityProperties() {
    try (SecretManager secretManager = memorySecretManager()) {
      Map<String, String> properties = Map.of("jdbc-user", "root");
      Map<String, SecretBinding> secretBindings =
          Map.of("jdbc-password", new SecretBinding("memory", "s3cr3t"));

      SecretPropertyUtils.checkSecretKeys(properties, secretBindings, Map.of());

      Map<String, String> toValidate =
          SecretPropertyUtils.propertiesToValidate(
              properties, secretBindings, Map.of(), secretManager);
      Assertions.assertEquals("root", toValidate.get("jdbc-user"));
      Assertions.assertEquals("s3cr3t", toValidate.get("jdbc-password"));

      Map<String, String> entityProperties = SecretPropertyUtils.copyEntityProperties(properties);
      SecretPropertyUtils.applySecretReferences(entityProperties, Map.of(), secretManager);
      List<SecretUrn> secretUrns =
          SecretPropertyUtils.writeBindingsAndApplyUrns(
              entityProperties, "catalog", 42L, secretBindings, secretManager);

      Assertions.assertEquals("root", entityProperties.get("jdbc-user"));
      Assertions.assertTrue(
          SecretPropertyUtils.isSecretProperty(
              "jdbc-password", entityProperties.get("jdbc-password")));
      Assertions.assertEquals(1, secretUrns.size());
      Assertions.assertEquals("s3cr3t", secretManager.readSecret(secretUrns.get(0)));
    }
  }

  @Test
  void testCheckSecretKeysRejectsOverlap() {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            SecretPropertyUtils.checkSecretKeys(
                Map.of(),
                Map.of("jdbc-password", new SecretBinding("memory", "s3cr3t")),
                Map.of(
                    "jdbc-password",
                    new SecretReference("memory", Map.of("path", "secret/data/x")))));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            SecretPropertyUtils.checkSecretKeys(
                Map.of("jdbc-password", "plain"),
                Map.of("jdbc-password", new SecretBinding("memory", "s3cr3t")),
                Map.of()));
  }

  @Test
  void testPropertiesToValidateSkipOnNull() {
    try (SecretManager secretManager = memorySecretManager()) {
      Assertions.assertNull(
          SecretPropertyUtils.propertiesToValidate(null, Map.of(), Map.of(), secretManager));
      Map<String, String> toValidate =
          SecretPropertyUtils.propertiesToValidate(
              null, Map.of("k", new SecretBinding("memory", "v")), Map.of(), secretManager);
      Assertions.assertEquals("v", toValidate.get("k"));
    }
  }

  @Test
  void testCopyEntityProperties() {
    Assertions.assertTrue(SecretPropertyUtils.copyEntityProperties(null).isEmpty());
    Map<String, String> original = Map.of("a", "b");
    Map<String, String> copy = SecretPropertyUtils.copyEntityProperties(original);
    Assertions.assertEquals(original, copy);
    copy.put("c", "d");
    Assertions.assertFalse(original.containsKey("c"));
  }

  @Test
  void testWriteBindingsNoOpOnEmpty() {
    try (SecretManager secretManager = memorySecretManager()) {
      Map<String, String> entityProperties = new HashMap<>(Map.of("jdbc-user", "root"));
      List<SecretUrn> secretUrns =
          SecretPropertyUtils.writeBindingsAndApplyUrns(
              entityProperties, "schema", 1L, Map.of(), secretManager);
      Assertions.assertTrue(secretUrns.isEmpty());
      Assertions.assertEquals("root", entityProperties.get("jdbc-user"));
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
