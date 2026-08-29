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
package org.apache.gravitino.credential;

import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.Config;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.TestCatalog;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.CatalogEntity;
import org.apache.gravitino.secret.SecretConstants;
import org.apache.gravitino.secret.SecretManager;
import org.apache.gravitino.secret.SecretMaterial;
import org.apache.gravitino.secret.SecretProviderRegistry;
import org.apache.gravitino.secret.SecretUrn;
import org.apache.gravitino.secret.memory.InMemorySecretsProvider;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Verifies that catalog credential vending resolves secret URNs to plaintext so {@code
 * getCredentials} / {@link JdbcCredential} return usable passwords.
 */
public class TestBaseCatalogCredentialSecrets {

  @AfterEach
  public void tearDown() throws IllegalAccessException {
    FieldUtils.writeField(GravitinoEnv.getInstance(), "secretManager", null, true);
  }

  @Test
  void testCatalogCredentialManagerResolvesJdbcPasswordUrn() throws Exception {
    try (SecretManager secretManager = memorySecretManager()) {
      FieldUtils.writeField(GravitinoEnv.getInstance(), "secretManager", secretManager, true);

      SecretUrn urn =
          SecretUrn.buildWriteThrough(
              "memory",
              Map.of(
                  SecretConstants.ATTR_ENTITY_TYPE,
                  "catalog",
                  SecretConstants.ATTR_ENTITY_ID,
                  "1",
                  SecretConstants.ATTR_PROPERTY_KEY,
                  "jdbc-password"));
      secretManager.writeSecrets(java.util.List.of(new SecretMaterial(urn, "plain-jdbc-pass")));

      Map<String, String> props =
          Map.of(
              CredentialConstants.CREDENTIAL_PROVIDERS,
              JdbcCredential.JDBC_CREDENTIAL_TYPE,
              JdbcCredential.GRAVITINO_JDBC_USER,
              "iceberg",
              JdbcCredential.GRAVITINO_JDBC_PASSWORD,
              urn.toString());

      CatalogEntity entity =
          CatalogEntity.builder()
              .withId(1L)
              .withName("jdbc-secret-catalog")
              .withNamespace(Namespace.of("metalake"))
              .withType(Catalog.Type.RELATIONAL)
              .withProvider("test")
              .withProperties(props)
              .withAuditInfo(
                  AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build())
              .build();

      TestCatalog catalog = new TestCatalog().withCatalogEntity(entity).withCatalogConf(props);

      Optional<Credential> credential =
          catalog
              .catalogCredentialManager()
              .getCredential(
                  JdbcCredential.JDBC_CREDENTIAL_TYPE, new CatalogCredentialContext("user"));

      Assertions.assertTrue(credential.isPresent());
      Assertions.assertInstanceOf(JdbcCredential.class, credential.get());
      JdbcCredential jdbc = (JdbcCredential) credential.get();
      Assertions.assertEquals("iceberg", jdbc.jdbcUser());
      Assertions.assertEquals("plain-jdbc-pass", jdbc.jdbcPassword());
      Assertions.assertFalse(jdbc.jdbcPassword().startsWith("urn:gravitino-secret:"));
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
