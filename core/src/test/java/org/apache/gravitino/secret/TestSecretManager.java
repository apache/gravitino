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

  private static final Map<String, SecretBinding> BINDINGS =
      Map.of("jdbc-password", new SecretBinding("memory", "s3cr3t"));
  private static final Map<String, SecretReference> REFERENCES =
      Map.of("jdbc-password", new SecretReference("memory", Map.of("path", "secret/data/x")));

  @Test
  void testWriteSecrets() {
    try (SecretManager sm = memorySecretManager()) {
      Map<String, String> props = new HashMap<>(Map.of("jdbc-user", "root"));
      List<SecretUrn> urns = sm.buildSecretBindingUrns("catalog", 42L, BINDINGS);
      sm.writeSecrets(List.of(new SecretMaterial(urns.get(0), "s3cr3t")));
      SecretPropertyUtils.putSecretUrns(props, urns);

      String urn = props.get("jdbc-password");
      Assertions.assertTrue(SecretPropertyUtils.isSecretProperty("jdbc-password", urn));
      Assertions.assertEquals("root", props.get("jdbc-user"));
      Assertions.assertEquals(1, urns.size());
      Assertions.assertEquals(
          "s3cr3t", sm.getRegistry().getProvider("memory").readSecret(urns.get(0)));
      Assertions.assertEquals("s3cr3t", sm.readSecret(urns.get(0)));
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
      sm.writeSecrets(List.of(new SecretMaterial(urns.get(0), "s3cr3t")));

      String urn = props.get("jdbc-password");
      Map<String, String> plaintext = sm.toPlaintextProperties(props);
      Assertions.assertEquals("root", plaintext.get("jdbc-user"));
      Assertions.assertEquals("s3cr3t", plaintext.get("jdbc-password"));
      Assertions.assertEquals(urn, props.get("jdbc-password"));
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
  void testSecretMaterialRedacts() {
    try (SecretManager sm = memorySecretManager()) {
      List<SecretUrn> urns = sm.buildSecretBindingUrns("catalog", 42L, BINDINGS);
      SecretMaterial material = new SecretMaterial(urns.get(0), "s3cr3t");
      Assertions.assertEquals(urns.get(0), material.urn());
      Assertions.assertEquals("s3cr3t", material.plaintext());
      Assertions.assertFalse(material.toString().contains("s3cr3t"));
      Assertions.assertTrue(material.toString().contains("***"));
    }
  }

  @Test
  void testDeleteBindingsFromProperties() {
    try (SecretManager sm = memorySecretManager()) {
      List<SecretUrn> urns = sm.buildSecretBindingUrns("catalog", 99L, BINDINGS);
      sm.writeSecrets(List.of(new SecretMaterial(urns.get(0), "s3cr3t")));
      Map<String, String> props = new HashMap<>();
      SecretPropertyUtils.putSecretUrns(props, urns);
      sm.deleteSecretsFromProperties(props);
      Assertions.assertThrows(IllegalArgumentException.class, () -> sm.readSecret(urns.get(0)));
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
