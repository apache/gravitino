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
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.TestCatalog;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.CatalogEntity;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Verifies that catalog credential vending uses catalog conf (plaintext) rather than raw entity
 * properties that may still store secret URNs.
 */
public class TestBaseCatalogCredentialSecrets {

  @Test
  void testCatalogCredentialManagerUsesPlaintextConfOverEntityUrn() {
    String urn = "urn:gravitino-secret:memory:catalog:1:jdbc-password";
    Map<String, String> entityProps = new HashMap<>();
    entityProps.put(CredentialConstants.CREDENTIAL_PROVIDERS, JdbcCredential.JDBC_CREDENTIAL_TYPE);
    entityProps.put(JdbcCredential.GRAVITINO_JDBC_USER, "iceberg");
    entityProps.put(JdbcCredential.GRAVITINO_JDBC_PASSWORD, urn);

    // CatalogManager sets conf via SecretManager.toPlaintextProperties(entity props).
    Map<String, String> confProps = new HashMap<>(entityProps);
    confProps.put(JdbcCredential.GRAVITINO_JDBC_PASSWORD, "plain-jdbc-pass");

    CatalogEntity entity =
        CatalogEntity.builder()
            .withId(1L)
            .withName("jdbc-secret-catalog")
            .withNamespace(Namespace.of("metalake"))
            .withType(Catalog.Type.RELATIONAL)
            .withProvider("test")
            .withProperties(entityProps)
            .withAuditInfo(
                AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build())
            .build();

    TestCatalog catalog = new TestCatalog().withCatalogEntity(entity).withCatalogConf(confProps);

    Assertions.assertEquals(
        "plain-jdbc-pass",
        catalog.propertiesWithCredentialProviders().get(JdbcCredential.GRAVITINO_JDBC_PASSWORD));

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
  }
}
