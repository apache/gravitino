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

package org.apache.gravitino.flink.connector.jdbc;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.gravitino.Audit;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.credential.Credential;
import org.apache.gravitino.credential.JdbcCredential;
import org.apache.gravitino.credential.SupportsCredentials;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link GravitinoJdbcCatalog#applyJdbcCredential}. */
public class TestGravitinoJdbcCatalog {

  private static class PlainCatalogStub implements Catalog {
    @Override
    public String name() {
      return "stub";
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
      return Collections.emptyMap();
    }

    @Override
    public Audit auditInfo() {
      return null;
    }
  }

  private static class CredentialCatalogStub extends PlainCatalogStub
      implements SupportsCredentials {

    private final Credential[] credentials;

    CredentialCatalogStub(Credential... credentials) {
      this.credentials = credentials;
    }

    @Override
    public Credential[] getCredentials() {
      return credentials;
    }

    @Override
    public SupportsCredentials supportsCredentials() {
      return this;
    }
  }

  @Test
  void testCredentialVendingOverridesOptions() {
    Map<String, String> options = new HashMap<>();
    options.put(JdbcPropertiesConstants.FLINK_JDBC_USER, "prop-user");
    options.put(JdbcPropertiesConstants.FLINK_JDBC_PASSWORD, "prop-password");

    Catalog catalog =
        new CredentialCatalogStub(new JdbcCredential("vended-user", "vended-password"));
    GravitinoJdbcCatalog.applyJdbcCredential(catalog, options);

    Assertions.assertEquals("vended-user", options.get(JdbcPropertiesConstants.FLINK_JDBC_USER));
    Assertions.assertEquals(
        "vended-password", options.get(JdbcPropertiesConstants.FLINK_JDBC_PASSWORD));
  }

  @Test
  void testFallsBackToOptionsWhenNoJdbcCredential() {
    Map<String, String> options = new HashMap<>();
    options.put(JdbcPropertiesConstants.FLINK_JDBC_USER, "prop-user");
    options.put(JdbcPropertiesConstants.FLINK_JDBC_PASSWORD, "prop-password");

    GravitinoJdbcCatalog.applyJdbcCredential(new CredentialCatalogStub(), options);

    Assertions.assertEquals("prop-user", options.get(JdbcPropertiesConstants.FLINK_JDBC_USER));
    Assertions.assertEquals(
        "prop-password", options.get(JdbcPropertiesConstants.FLINK_JDBC_PASSWORD));
  }

  @Test
  void testFallsBackToOptionsWhenCatalogDoesNotSupportCredentials() {
    Map<String, String> options = new HashMap<>();
    options.put(JdbcPropertiesConstants.FLINK_JDBC_USER, "prop-user");
    options.put(JdbcPropertiesConstants.FLINK_JDBC_PASSWORD, "prop-password");

    GravitinoJdbcCatalog.applyJdbcCredential(new PlainCatalogStub(), options);

    Assertions.assertEquals("prop-user", options.get(JdbcPropertiesConstants.FLINK_JDBC_USER));
    Assertions.assertEquals(
        "prop-password", options.get(JdbcPropertiesConstants.FLINK_JDBC_PASSWORD));
  }
}
