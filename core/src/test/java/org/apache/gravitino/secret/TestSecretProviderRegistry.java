/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.gravitino.secret;

import java.util.List;
import java.util.Properties;
import org.apache.gravitino.Config;
import org.apache.gravitino.secret.memory.InMemorySecretsProvider;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestSecretProviderRegistry {

  @Test
  public void testEmptyProviders() {
    Config config = new Config(false) {};
    try (SecretProviderRegistry registry = new SecretProviderRegistry(config)) {
      Assertions.assertTrue(registry.listProviders().isEmpty());
      Assertions.assertFalse(registry.contains("memory"));
      Assertions.assertThrows(IllegalArgumentException.class, () -> registry.getProvider("memory"));
    }
  }

  @Test
  public void testLoadInMemoryProvider() {
    Config config = configWithMemoryProvider(null);
    try (SecretProviderRegistry registry = new SecretProviderRegistry(config)) {
      List<SecretProviderInfo> infos = registry.listProviders();
      Assertions.assertEquals(1, infos.size());
      Assertions.assertEquals(new SecretProviderInfo("memory", "memory", null), infos.get(0));
      Assertions.assertTrue(registry.contains("memory"));
      Assertions.assertEquals("memory", registry.getProvider("memory").type());
    }
  }

  @Test
  public void testOptionalUri() {
    Config config = configWithMemoryProvider("https://secrets.example.com");
    try (SecretProviderRegistry registry = new SecretProviderRegistry(config)) {
      Assertions.assertEquals(
          new SecretProviderInfo("memory", "memory", "https://secrets.example.com"),
          registry.listProviders().get(0));
    }
  }

  @Test
  public void testDuplicateProviderNameRejected() {
    Config config = new Config(false) {};
    Properties properties = new Properties();
    properties.setProperty(SecretProviderRegistry.GRAVITINO_SECRET_PROVIDERS, "memory,memory");
    config.loadFromProperties(properties);
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> new SecretProviderRegistry(config));
  }

  @Test
  public void testMissingClassNameRejected() {
    Config config = new Config(false) {};
    Properties properties = new Properties();
    properties.setProperty(SecretProviderRegistry.GRAVITINO_SECRET_PROVIDERS, "memory");
    config.loadFromProperties(properties);
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> new SecretProviderRegistry(config));
  }

  @Test
  public void testClosedRegistryRejectsAccess() {
    Config config = configWithMemoryProvider(null);
    SecretProviderRegistry registry = new SecretProviderRegistry(config);
    registry.close();
    Assertions.assertThrows(IllegalStateException.class, registry::listProviders);
    Assertions.assertThrows(IllegalStateException.class, () -> registry.getProvider("memory"));
  }

  private static Config configWithMemoryProvider(String uri) {
    Config config = new Config(false) {};
    Properties properties = new Properties();
    properties.setProperty(SecretProviderRegistry.GRAVITINO_SECRET_PROVIDERS, "memory");
    properties.setProperty(
        SecretProviderRegistry.GRAVITINO_SECRET_PROVIDER_PREFIX
            + "memory."
            + SecretProviderRegistry.CLASS_NAME,
        InMemorySecretsProvider.class.getName());
    if (uri != null) {
      properties.setProperty(
          SecretProviderRegistry.GRAVITINO_SECRET_PROVIDER_PREFIX
              + "memory."
              + SecretProviderRegistry.URI,
          uri);
    }
    config.loadFromProperties(properties);
    return config;
  }
}
