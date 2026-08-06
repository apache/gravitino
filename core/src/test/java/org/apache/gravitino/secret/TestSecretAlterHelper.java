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
import java.util.Map;
import java.util.Properties;
import org.apache.gravitino.CatalogChange;
import org.apache.gravitino.Config;
import org.apache.gravitino.secret.memory.InMemorySecretsProvider;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestSecretAlterHelper {

  @Test
  void testSetSecretBindingRewritesToSetProperty() {
    try (SecretManager secretManager = memorySecretManager()) {
      Map<String, String> props = new HashMap<>(Map.of("jdbc-user", "root"));
      SecretAlterHelper.Result<CatalogChange> result =
          SecretAlterHelper.applyCatalogChanges(
              secretManager,
              props,
              42L,
              CatalogChange.setSecretBinding(
                  "jdbc-password", new SecretBinding("memory", "s3cr3t")));

      Assertions.assertEquals(1, result.changes().length);
      Assertions.assertInstanceOf(CatalogChange.SetProperty.class, result.changes()[0]);
      CatalogChange.SetProperty setProperty = (CatalogChange.SetProperty) result.changes()[0];
      Assertions.assertEquals("jdbc-password", setProperty.getProperty());
      Assertions.assertTrue(
          SecretPropertyUtils.isWriteThroughForEntity(
              "jdbc-password", setProperty.getValue(), "catalog", 42L));
      Assertions.assertEquals(1, result.writtenUrns().size());
      Assertions.assertEquals(
          "s3cr3t",
          secretManager
              .getRegistry()
              .getProvider("memory")
              .readSecret(result.writtenUrns().get(0)));
    }
  }

  @Test
  void testRemovePropertyDeletesWriteThroughSecret() {
    try (SecretManager secretManager = memorySecretManager()) {
      Map<String, String> props = new HashMap<>();
      SecretAlterHelper.Result<CatalogChange> create =
          SecretAlterHelper.applyCatalogChanges(
              secretManager,
              props,
              7L,
              CatalogChange.setSecretBinding("jdbc-password", new SecretBinding("memory", "old")));
      String urn = ((CatalogChange.SetProperty) create.changes()[0]).getValue();
      props.put("jdbc-password", urn);

      SecretAlterHelper.applyCatalogChanges(
          secretManager, props, 7L, CatalogChange.removeProperty("jdbc-password"));

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
              SecretAlterHelper.applyCatalogChanges(
                  secretManager,
                  Map.of(),
                  1L,
                  CatalogChange.setProperty("jdbc-password", "******")));
      Assertions.assertThrows(
          IllegalArgumentException.class,
          () ->
              SecretAlterHelper.applyCatalogChanges(
                  secretManager,
                  Map.of(),
                  1L,
                  CatalogChange.setProperty(
                      "jdbc-password", "urn:gravitino-secret:memory:catalog:1:jdbc-password")));
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
