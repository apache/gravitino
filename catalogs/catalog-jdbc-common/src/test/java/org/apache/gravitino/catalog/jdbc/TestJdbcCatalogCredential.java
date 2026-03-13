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
package org.apache.gravitino.catalog.jdbc;

import com.google.common.collect.Maps;
import java.time.Instant;
import java.util.Map;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.catalog.jdbc.config.JdbcConfig;
import org.apache.gravitino.catalog.jdbc.converter.JdbcColumnDefaultValueConverter;
import org.apache.gravitino.catalog.jdbc.converter.JdbcTypeConverter;
import org.apache.gravitino.catalog.jdbc.operation.JdbcDatabaseOperations;
import org.apache.gravitino.catalog.jdbc.operation.JdbcTableOperations;
import org.apache.gravitino.credential.CredentialConstants;
import org.apache.gravitino.credential.JdbcCredential;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.CatalogEntity;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Tests for JdbcCatalog credential provider functionality. */
public class TestJdbcCatalogCredential {

  /** A concrete implementation of JdbcCatalog for testing purposes. */
  private static class TestableJdbcCatalog extends JdbcCatalog {

    @Override
    public String shortName() {
      return "jdbc-test";
    }

    @Override
    protected JdbcTypeConverter createJdbcTypeConverter() {
      return null;
    }

    @Override
    protected JdbcDatabaseOperations createJdbcDatabaseOperations() {
      return null;
    }

    @Override
    protected JdbcTableOperations createJdbcTableOperations() {
      return null;
    }

    @Override
    protected JdbcColumnDefaultValueConverter createJdbcColumnDefaultValueConverter() {
      return null;
    }
  }

  @Test
  void testJdbcCatalogDefaultCredentialProviders() {
    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("creator").withCreateTime(Instant.now()).build();

    // Test JDBC catalog with jdbc-user and jdbc-password
    Map<String, String> jdbcProps = Maps.newHashMap();
    jdbcProps.put(JdbcConfig.JDBC_URL.getKey(), "jdbc:mysql://localhost:3306/test");
    jdbcProps.put(JdbcConfig.JDBC_DRIVER.getKey(), "com.mysql.cj.jdbc.Driver");
    jdbcProps.put(JdbcConfig.USERNAME.getKey(), "test-user");
    jdbcProps.put(JdbcConfig.PASSWORD.getKey(), "test-password");

    CatalogEntity jdbcEntity =
        CatalogEntity.builder()
            .withId(1L)
            .withName("jdbc-catalog")
            .withNamespace(Namespace.of("metalake"))
            .withType(TestableJdbcCatalog.Type.RELATIONAL)
            .withProvider("jdbc-mysql")
            .withAuditInfo(auditInfo)
            .withProperties(jdbcProps)
            .build();

    TestableJdbcCatalog jdbcCatalog = new TestableJdbcCatalog();
    jdbcCatalog.withCatalogConf(jdbcProps).withCatalogEntity(jdbcEntity);
    Map<String, String> properties = jdbcCatalog.propertiesWithCredentialProviders();

    // Should have jdbc credential provider
    String credentialProviders = properties.get(CredentialConstants.CREDENTIAL_PROVIDERS);
    Assertions.assertNotNull(credentialProviders);
    Assertions.assertEquals(JdbcCredential.JDBC_CREDENTIAL_TYPE, credentialProviders);
  }

  @Test
  void testJdbcCatalogNoCredentialProvidersWithoutPassword() {
    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("creator").withCreateTime(Instant.now()).build();

    // Test JDBC catalog without password - should not add credential provider
    Map<String, String> jdbcProps = Maps.newHashMap();
    jdbcProps.put(JdbcConfig.JDBC_URL.getKey(), "jdbc:mysql://localhost:3306/test");
    jdbcProps.put(JdbcConfig.JDBC_DRIVER.getKey(), "com.mysql.cj.jdbc.Driver");
    jdbcProps.put(JdbcConfig.USERNAME.getKey(), "test-user");

    CatalogEntity jdbcEntity =
        CatalogEntity.builder()
            .withId(2L)
            .withName("jdbc-catalog-no-password")
            .withNamespace(Namespace.of("metalake"))
            .withType(TestableJdbcCatalog.Type.RELATIONAL)
            .withProvider("jdbc-mysql")
            .withAuditInfo(auditInfo)
            .withProperties(jdbcProps)
            .build();

    TestableJdbcCatalog jdbcCatalog = new TestableJdbcCatalog();
    jdbcCatalog.withCatalogConf(jdbcProps).withCatalogEntity(jdbcEntity);
    Map<String, String> properties = jdbcCatalog.propertiesWithCredentialProviders();

    // Should not have credential providers
    String credentialProviders = properties.get(CredentialConstants.CREDENTIAL_PROVIDERS);
    Assertions.assertNull(credentialProviders);
  }

  @Test
  void testJdbcCatalogExplicitCredentialProvidersNotOverridden() {
    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("creator").withCreateTime(Instant.now()).build();

    // Test that explicit credential-providers setting is not overridden
    Map<String, String> explicitProps = Maps.newHashMap();
    explicitProps.put(JdbcConfig.JDBC_URL.getKey(), "jdbc:mysql://localhost:3306/test");
    explicitProps.put(JdbcConfig.JDBC_DRIVER.getKey(), "com.mysql.cj.jdbc.Driver");
    explicitProps.put(JdbcConfig.USERNAME.getKey(), "test-user");
    explicitProps.put(JdbcConfig.PASSWORD.getKey(), "test-password");
    explicitProps.put(CredentialConstants.CREDENTIAL_PROVIDERS, "custom-provider");

    CatalogEntity explicitEntity =
        CatalogEntity.builder()
            .withId(3L)
            .withName("explicit-catalog")
            .withNamespace(Namespace.of("metalake"))
            .withType(TestableJdbcCatalog.Type.RELATIONAL)
            .withProvider("jdbc-mysql")
            .withAuditInfo(auditInfo)
            .withProperties(explicitProps)
            .build();

    TestableJdbcCatalog explicitCatalog = new TestableJdbcCatalog();
    explicitCatalog.withCatalogConf(explicitProps).withCatalogEntity(explicitEntity);
    Map<String, String> properties = explicitCatalog.propertiesWithCredentialProviders();

    // Should keep explicit credential providers, not override
    String credentialProviders = properties.get(CredentialConstants.CREDENTIAL_PROVIDERS);
    Assertions.assertEquals("custom-provider", credentialProviders);
  }
}
