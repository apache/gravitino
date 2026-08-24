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
  void testAssembleAndWrite() {
    try (SecretManager sm = memorySecretManager()) {
      Map<String, String> properties = Map.of("jdbc-user", "root");
      Map<String, SecretBinding> bindings =
          Map.of("jdbc-password", new SecretBinding("memory", "s3cr3t"));
      Map<String, String> entityProps = SecretPropertyUtils.copyEntityProperties(properties);
      List<SecretMaterial> writes =
          sm.assembleSecretMaterials(properties, entityProps, "catalog", 42L, bindings, Map.of());
      sm.writeSecrets(writes);

      Assertions.assertEquals("root", entityProps.get("jdbc-user"));
      Assertions.assertTrue(
          SecretPropertyUtils.isSecretProperty("jdbc-password", entityProps.get("jdbc-password")));
      Assertions.assertEquals(1, writes.size());
      Assertions.assertEquals("s3cr3t", sm.readSecret(writes.get(0).urn()));
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
  void testEmptySecretsNoOp() {
    try (SecretManager sm = memorySecretManager()) {
      Map<String, String> entityProps = new HashMap<>(Map.of("jdbc-user", "root"));
      List<SecretMaterial> writes =
          sm.assembleSecretMaterials(entityProps, entityProps, "schema", 1L, Map.of(), Map.of());
      sm.writeSecrets(writes);
      Assertions.assertTrue(writes.isEmpty());
      Assertions.assertEquals("root", entityProps.get("jdbc-user"));
    }
  }

  @Test
  void testBuildSecrets() {
    try (SecretManager sm = memorySecretManager()) {
      Map<String, String> entityProps = new HashMap<>();
      entityProps.put("jdbc-url", "jdbc:mysql://localhost/db");
      entityProps.put("jdbc-user", "root");
      Map<String, SecretBinding> bindings =
          Map.of(
              "jdbc-password",
              new SecretBinding("memory", "s3cr3t"),
              "custom-secret",
              new SecretBinding("memory", "custom-value"),
              "s3-secret-access-key",
              new SecretBinding("memory", "s3-secret-value"));
      List<SecretMaterial> writes =
          sm.assembleSecretMaterials(
              Map.of("jdbc-url", "jdbc:mysql://localhost/db", "jdbc-user", "root"),
              entityProps,
              "catalog",
              42L,
              bindings,
              Map.of());
      sm.writeSecrets(writes);

      entityProps.put("s3-access-key-id", "AKIA");
      entityProps.put("visible", "ok");

      Map<String, String> secrets = SecretPropertyUtils.buildSecrets(sm, entityProps);

      // All secret-URN entries, including keys also used by credential vending
      Assertions.assertEquals("custom-value", secrets.get("custom-secret"));
      Assertions.assertEquals("s3cr3t", secrets.get("jdbc-password"));
      Assertions.assertEquals("s3-secret-value", secrets.get("s3-secret-access-key"));
      Assertions.assertFalse(secrets.containsKey("jdbc-user"));
      Assertions.assertFalse(secrets.containsKey("jdbc-url"));
      Assertions.assertFalse(secrets.containsKey("visible"));
      Assertions.assertFalse(secrets.containsKey("s3-access-key-id"));
    }
  }

  @Test
  void testBuildSecretsNullAndEmpty() {
    try (SecretManager sm = memorySecretManager()) {
      Assertions.assertTrue(SecretPropertyUtils.buildSecrets(sm, null).isEmpty());
      Assertions.assertTrue(SecretPropertyUtils.buildSecrets(sm, Map.of()).isEmpty());
    }
  }

  @Test
  void testMergeProperties() {
    Map<String, String> merged =
        SecretPropertyUtils.mergeProperties(Map.of("a", "1"), Map.of("b", "2", "a", "override"));
    Assertions.assertEquals("override", merged.get("a"));
    Assertions.assertEquals("2", merged.get("b"));
    Assertions.assertTrue(SecretPropertyUtils.mergeProperties(null, null).isEmpty());
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
