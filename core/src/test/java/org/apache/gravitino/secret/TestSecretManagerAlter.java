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
      String urn =
          secretManager.alterSetSecretBinding(
              props,
              "catalog",
              42L,
              "jdbc-password",
              new SecretBinding("memory", "s3cr3t"),
              written);

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
  void testAlterRemovePropertyDeletesWriteThroughSecret() {
    try (SecretManager secretManager = memorySecretManager()) {
      Map<String, String> props = new HashMap<>();
      List<SecretMaterial> written = new ArrayList<>();
      String urn =
          secretManager.alterSetSecretBinding(
              props, "catalog", 7L, "jdbc-password", new SecretBinding("memory", "old"), written);

      secretManager.alterRemoveProperty(props, "catalog", 7L, "jdbc-password");

      Assertions.assertFalse(props.containsKey("jdbc-password"));
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
              written);

      secretManager.alterRemoveProperty(props, "catalog", 7L, "jdbc-password");

      Assertions.assertEquals(
          "keep-me",
          secretManager.getRegistry().getProvider("memory").readSecret(SecretUrn.parse(ownedUrn)));
    }
  }

  @Test
  void testSchemaAlterRemovePropertyDeletesWriteThroughSecret() {
    try (SecretManager secretManager = memorySecretManager()) {
      Map<String, String> props = new HashMap<>();
      List<SecretMaterial> written = new ArrayList<>();
      String urn =
          secretManager.alterSetSecretBinding(
              props, "schema", 9L, "k2", new SecretBinding("memory", "old"), written);

      secretManager.alterRemoveProperty(props, "schema", 9L, "k2");

      Assertions.assertThrows(
          IllegalArgumentException.class,
          () -> secretManager.getRegistry().getProvider("memory").readSecret(SecretUrn.parse(urn)));
    }
  }

  @Test
  void testFilesetAlterRemovePropertyDeletesWriteThroughSecret() {
    try (SecretManager secretManager = memorySecretManager()) {
      Map<String, String> props = new HashMap<>();
      List<SecretMaterial> written = new ArrayList<>();
      String urn =
          secretManager.alterSetSecretBinding(
              props, "fileset", 11L, "k2", new SecretBinding("memory", "old"), written);

      secretManager.alterRemoveProperty(props, "fileset", 11L, "k2");

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
