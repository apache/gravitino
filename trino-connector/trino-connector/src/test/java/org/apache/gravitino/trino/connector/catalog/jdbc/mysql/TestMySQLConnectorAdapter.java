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
package org.apache.gravitino.trino.connector.catalog.jdbc.mysql;

import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.gravitino.Audit;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.credential.Credential;
import org.apache.gravitino.credential.JdbcCredential;
import org.apache.gravitino.credential.SupportsCredentials;
import org.apache.gravitino.trino.connector.catalog.jdbc.JDBCCatalogPropertyConverter;
import org.apache.gravitino.trino.connector.metadata.GravitinoCatalog;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link MySQLConnectorAdapter#buildInternalConnectorConfig}. */
public class TestMySQLConnectorAdapter {

  private static final String JDBC_URL = "jdbc:mysql://localhost:3306/";

  private static final Audit AUDIT_STUB =
      new Audit() {
        @Override
        public String creator() {
          return "test";
        }

        @Override
        public Instant createTime() {
          return Instant.EPOCH;
        }

        @Override
        public String lastModifier() {
          return null;
        }

        @Override
        public Instant lastModifiedTime() {
          return null;
        }
      };

  private static class PlainCatalogStub implements Catalog {
    private final Map<String, String> properties;

    PlainCatalogStub(Map<String, String> properties) {
      this.properties = properties;
    }

    @Override
    public String name() {
      return "test";
    }

    @Override
    public Type type() {
      return Type.RELATIONAL;
    }

    @Override
    public String provider() {
      return "jdbc-mysql";
    }

    @Override
    public String comment() {
      return null;
    }

    @Override
    public Map<String, String> properties() {
      return properties;
    }

    @Override
    public Audit auditInfo() {
      return AUDIT_STUB;
    }
  }

  private static class CredentialCatalogStub extends PlainCatalogStub
      implements SupportsCredentials {

    private final Credential[] credentials;

    CredentialCatalogStub(Map<String, String> properties, Credential... credentials) {
      super(properties);
      this.credentials = credentials;
    }

    @Override
    public Credential[] getCredentials() {
      return credentials;
    }
  }

  private Map<String, String> baseProperties() {
    Map<String, String> props = new HashMap<>();
    props.put("jdbc-url", JDBC_URL);
    props.put("jdbc-user", "prop-user");
    props.put("jdbc-password", "prop-password");
    return props;
  }

  @Test
  void testCredentialVendingOverridesProperties() throws Exception {
    Map<String, String> props = new HashMap<>();
    props.put("jdbc-url", JDBC_URL);
    // jdbc-user and jdbc-password are hidden on the server, so absent here

    Catalog catalog =
        new CredentialCatalogStub(props, new JdbcCredential("vended-user", "vended-password"));
    GravitinoCatalog gravitinoCatalog = new GravitinoCatalog("metalake", catalog);

    Map<String, String> config =
        new MySQLConnectorAdapter().buildInternalConnectorConfig(gravitinoCatalog);

    Assertions.assertEquals(
        "vended-user", config.get(JDBCCatalogPropertyConverter.JDBC_CONNECTION_USER_KEY));
    Assertions.assertEquals(
        "vended-password", config.get(JDBCCatalogPropertyConverter.JDBC_CONNECTION_PASSWORD_KEY));
  }

  @Test
  void testFallsBackToPropertiesWhenNoJdbcCredential() throws Exception {
    Catalog catalog = new CredentialCatalogStub(baseProperties() /* no credentials */);
    GravitinoCatalog gravitinoCatalog = new GravitinoCatalog("metalake", catalog);

    Map<String, String> config =
        new MySQLConnectorAdapter().buildInternalConnectorConfig(gravitinoCatalog);

    Assertions.assertEquals(
        "prop-user", config.get(JDBCCatalogPropertyConverter.JDBC_CONNECTION_USER_KEY));
    Assertions.assertEquals(
        "prop-password", config.get(JDBCCatalogPropertyConverter.JDBC_CONNECTION_PASSWORD_KEY));
  }

  @Test
  void testFallsBackToPropertiesWhenCatalogDoesNotSupportCredentials() throws Exception {
    Catalog catalog = new PlainCatalogStub(baseProperties());
    GravitinoCatalog gravitinoCatalog = new GravitinoCatalog("metalake", catalog);

    Map<String, String> config =
        new MySQLConnectorAdapter().buildInternalConnectorConfig(gravitinoCatalog);

    Assertions.assertEquals(
        "prop-user", config.get(JDBCCatalogPropertyConverter.JDBC_CONNECTION_USER_KEY));
    Assertions.assertEquals(
        "prop-password", config.get(JDBCCatalogPropertyConverter.JDBC_CONNECTION_PASSWORD_KEY));
  }

  @Test
  void testNullOriginalCatalogFallsBackToProperties() throws Exception {
    // Simulates a GravitinoCatalog reconstructed from JSON (originalCatalog == null)
    GravitinoCatalog gravitinoCatalog =
        new GravitinoCatalog(
            "metalake", "jdbc-mysql", "test", baseProperties(), System.currentTimeMillis());

    Map<String, String> config =
        new MySQLConnectorAdapter().buildInternalConnectorConfig(gravitinoCatalog);

    Assertions.assertEquals(
        "prop-user", config.get(JDBCCatalogPropertyConverter.JDBC_CONNECTION_USER_KEY));
    Assertions.assertEquals(
        "prop-password", config.get(JDBCCatalogPropertyConverter.JDBC_CONNECTION_PASSWORD_KEY));
  }

  @Test
  void testGetOriginalCatalogIsNullForJsonConstructor() {
    GravitinoCatalog gravitinoCatalog =
        new GravitinoCatalog(
            "metalake", "jdbc-mysql", "test", Collections.singletonMap("jdbc-url", JDBC_URL), 0L);
    Assertions.assertNull(gravitinoCatalog.getOriginalCatalog());
  }
}
