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
package org.apache.gravitino;

import java.util.Properties;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.secret.SecretManager;
import org.apache.gravitino.secret.SecretProviderRegistry;
import org.apache.gravitino.secret.memory.InMemorySecretsProvider;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestGravitinoEnvSecretProviderRegistry {

  @Test
  void testEmptyRegistryIsOptionalAndClosedWithEnvironment() throws IllegalAccessException {
    TestGravitinoEnv env = new TestGravitinoEnv();
    Assertions.assertThrows(IllegalStateException.class, env::secretProviderRegistry);
    Assertions.assertThrows(IllegalStateException.class, env::secretManager);

    SecretManager secretManager = new SecretManager(new Config(false) {});
    FieldUtils.writeField(env, "secretManager", secretManager, true);

    Assertions.assertSame(secretManager, env.secretManager());
    Assertions.assertSame(secretManager.getRegistry(), env.secretProviderRegistry());
    Assertions.assertTrue(secretManager.getRegistry().listProviders().isEmpty());

    env.shutdown();

    Assertions.assertSame(secretManager, env.secretManager());
    Assertions.assertThrows(
        IllegalStateException.class, () -> secretManager.getRegistry().listProviders());
  }

  @Test
  void testBaseEnvironmentInitializesSecretProviderRegistry() {
    TestGravitinoEnv env = new TestGravitinoEnv();
    Config config = new Config(false) {};
    Properties properties = new Properties();
    properties.setProperty(SecretProviderRegistry.GRAVITINO_SECRET_PROVIDERS, "memory");
    properties.setProperty(
        SecretProviderRegistry.GRAVITINO_SECRET_PROVIDER_PREFIX
            + "memory."
            + SecretProviderRegistry.CLASS_NAME,
        InMemorySecretsProvider.class.getName());
    config.loadFromProperties(properties);

    env.initializeBaseComponents(config);
    SecretManager secretManager = env.secretManager();
    SecretProviderRegistry registry = env.secretProviderRegistry();
    Assertions.assertSame(secretManager.getRegistry(), registry);
    Assertions.assertEquals(1, registry.listProviders().size());
    Assertions.assertEquals("memory", registry.getProvider("memory").type());

    env.shutdown();

    Assertions.assertSame(secretManager, env.secretManager());
    Assertions.assertThrows(IllegalStateException.class, registry::listProviders);
  }

  private static final class TestGravitinoEnv extends GravitinoEnv {}
}
