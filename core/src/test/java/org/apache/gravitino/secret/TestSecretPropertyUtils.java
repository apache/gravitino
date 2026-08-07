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
  void testAssembleEntityPropertiesThenWrite() {
    try (SecretManager secretManager = memorySecretManager()) {
      Map<String, String> properties = Map.of("jdbc-user", "root");
      Map<String, SecretBinding> secretBindings =
          Map.of("jdbc-password", new SecretBinding("memory", "s3cr3t"));

      Map<String, String> entityProperties = SecretPropertyUtils.copyEntityProperties(properties);
      List<SecretUrn> secretUrns =
          secretManager.assembleSecretUrns(
              properties, entityProperties, "catalog", 42L, secretBindings, Map.of());
      secretManager.writeSecrets(secretBindings, secretUrns);

      Assertions.assertEquals("root", entityProperties.get("jdbc-user"));
      Assertions.assertTrue(
          SecretPropertyUtils.isSecretProperty(
              "jdbc-password", entityProperties.get("jdbc-password")));
      Assertions.assertEquals(1, secretUrns.size());
      Assertions.assertEquals("s3cr3t", secretManager.readSecret(secretUrns.get(0)));
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
  void testEmptySecretsAreNoOp() {
    try (SecretManager secretManager = memorySecretManager()) {
      Map<String, String> entityProperties = new HashMap<>(Map.of("jdbc-user", "root"));
      List<SecretUrn> secretUrns =
          secretManager.assembleSecretUrns(
              entityProperties, entityProperties, "schema", 1L, Map.of(), Map.of());
      secretManager.writeSecrets(Map.of(), secretUrns);
      Assertions.assertTrue(secretUrns.isEmpty());
      Assertions.assertEquals("root", entityProperties.get("jdbc-user"));
    }
  }

  @Test
  void testAssembleForSchemaAndFilesetEntityTypes() {
    try (SecretManager secretManager = memorySecretManager()) {
      Map<String, SecretBinding> bindings =
          Map.of("k2", new SecretBinding("memory", "schema-secret"));
      Map<String, String> schemaProps =
          SecretPropertyUtils.copyEntityProperties(Map.of("k1", "v1"));
      List<SecretUrn> schemaUrns =
          secretManager.assembleSecretUrns(
              Map.of("k1", "v1"), schemaProps, "schema", 11L, bindings, Map.of());
      secretManager.writeSecrets(bindings, schemaUrns);
      Assertions.assertTrue(SecretPropertyUtils.isSecretProperty("k2", schemaProps.get("k2")));
      Assertions.assertEquals("schema-secret", secretManager.readSecret(schemaUrns.get(0)));

      Map<String, SecretBinding> filesetBindings =
          Map.of("k2", new SecretBinding("memory", "fileset-secret"));
      Map<String, String> filesetProps =
          SecretPropertyUtils.copyEntityProperties(Map.of("k1", "v1"));
      List<SecretUrn> filesetUrns =
          secretManager.assembleSecretUrns(
              Map.of("k1", "v1"), filesetProps, "fileset", 22L, filesetBindings, Map.of());
      secretManager.writeSecrets(filesetBindings, filesetUrns);
      Assertions.assertTrue(SecretPropertyUtils.isSecretProperty("k2", filesetProps.get("k2")));
      Assertions.assertEquals("fileset-secret", secretManager.readSecret(filesetUrns.get(0)));
    }
  }

  @Test
  void testAssembleWriteThenRollback() {
    try (SecretManager secretManager = memorySecretManager()) {
      Map<String, SecretBinding> bindings =
          Map.of("jdbc-password", new SecretBinding("memory", "s3cr3t"));
      Map<String, String> entityProperties =
          SecretPropertyUtils.copyEntityProperties(Map.of("jdbc-user", "root"));
      List<SecretUrn> secretUrns =
          secretManager.assembleSecretUrns(
              Map.of("jdbc-user", "root"), entityProperties, "catalog", 42L, bindings, Map.of());
      secretManager.writeSecrets(bindings, secretUrns);
      Assertions.assertEquals("s3cr3t", secretManager.readSecret(secretUrns.get(0)));

      secretManager.rollbackWritten(secretUrns);
      Assertions.assertThrows(
          IllegalArgumentException.class, () -> secretManager.readSecret(secretUrns.get(0)));
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
