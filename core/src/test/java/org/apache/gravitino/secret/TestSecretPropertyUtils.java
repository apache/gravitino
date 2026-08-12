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
import org.apache.gravitino.connector.PropertiesMetadata;
import org.apache.gravitino.connector.PropertyEntry;
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
  void testIsCredentialVendingProperty() {
    Assertions.assertTrue(SecretPropertyUtils.isCredentialVendingProperty("s3-access-key-id"));
    Assertions.assertTrue(SecretPropertyUtils.isCredentialVendingProperty("s3-secret-access-key"));
    Assertions.assertTrue(SecretPropertyUtils.isCredentialVendingProperty("aws-access-key-id"));
    Assertions.assertTrue(
        SecretPropertyUtils.isCredentialVendingProperty("azure-storage-account-key"));
    Assertions.assertFalse(SecretPropertyUtils.isCredentialVendingProperty("jdbc-password"));
    Assertions.assertFalse(SecretPropertyUtils.isCredentialVendingProperty("jdbc-user"));
    Assertions.assertFalse(SecretPropertyUtils.isCredentialVendingProperty(null));
  }

  @Test
  void testBuildResolvedProperties() {
    try (SecretManager sm = memorySecretManager()) {
      Map<String, String> entityProps = new HashMap<>();
      entityProps.put("jdbc-url", "jdbc:mysql://localhost/db");
      entityProps.put("jdbc-user", "root");
      Map<String, SecretBinding> bindings =
          Map.of("jdbc-password", new SecretBinding("memory", "s3cr3t"));
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
      entityProps.put("s3-secret-access-key", "SECRET");
      entityProps.put("legacy-hidden", "plaintext-secret");
      entityProps.put("visible", "ok");

      PropertiesMetadata metadata =
          new PropertiesMetadata() {
            @Override
            public Map<String, PropertyEntry<?>> propertyEntries() {
              return Map.of(
                  "legacy-hidden",
                  PropertyEntry.stringOptionalPropertyEntry(
                      "legacy-hidden", "legacy", true /* immutable */, null, true /* hidden */));
            }
          };

      Map<String, String> resolved =
          SecretPropertyUtils.buildResolvedProperties(sm, entityProps, metadata);

      Assertions.assertEquals("jdbc:mysql://localhost/db", resolved.get("jdbc-url"));
      Assertions.assertEquals("root", resolved.get("jdbc-user"));
      Assertions.assertEquals("s3cr3t", resolved.get("jdbc-password"));
      Assertions.assertEquals("ok", resolved.get("visible"));
      Assertions.assertFalse(resolved.containsKey("s3-access-key-id"));
      Assertions.assertFalse(resolved.containsKey("s3-secret-access-key"));
      Assertions.assertFalse(resolved.containsKey("legacy-hidden"));
    }
  }

  @Test
  void testBuildResolvedPropertiesNullAndEmpty() {
    try (SecretManager sm = memorySecretManager()) {
      Assertions.assertTrue(SecretPropertyUtils.buildResolvedProperties(sm, null, null).isEmpty());
      Assertions.assertTrue(
          SecretPropertyUtils.buildResolvedProperties(sm, Map.of(), null).isEmpty());
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
