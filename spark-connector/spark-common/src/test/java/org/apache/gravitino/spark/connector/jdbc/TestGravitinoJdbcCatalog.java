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

package org.apache.gravitino.spark.connector.jdbc;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.Audit;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.credential.Credential;
import org.apache.gravitino.credential.JdbcCredential;
import org.apache.gravitino.credential.SupportsCredentials;
import org.apache.gravitino.rel.types.Types;
import org.apache.gravitino.spark.connector.catalog.GravitinoCatalogManager;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;

/** Unit tests for {@link GravitinoJdbcCatalog#applyJdbcCredential}. */
public class TestGravitinoJdbcCatalog {

  /**
   * Minimal Catalog stub — returns nothing, used to verify the instanceof check for
   * SupportsCredentials.
   */
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

  /** Catalog that also implements SupportsCredentials — simulates the upgraded server. */
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

  @ParameterizedTest
  @ValueSource(
      strings = {
        "jdbc-postgresql",
        "jdbc-mysql",
        "jdbc-doris",
        "jdbc-starrocks",
        "jdbc-oceanbase",
        "jdbc-oracle"
      })
  void testTimestampMappingAndReaderOptionStayAligned(String provider) throws Exception {
    try (MockedStatic<GravitinoCatalogManager> global = mockStatic(GravitinoCatalogManager.class)) {
      global.when(GravitinoCatalogManager::get).thenReturn(mock(GravitinoCatalogManager.class));
      Catalog source = mock(Catalog.class);
      SupportsCredentials credentials = mock(SupportsCredentials.class);
      when(source.provider()).thenReturn(provider);
      when(source.supportsCredentials()).thenReturn(credentials);
      when(credentials.getCredentials())
          .thenReturn(new Credential[] {new JdbcCredential("user", "password")});
      GravitinoJdbcCatalog catalog = new GravitinoJdbcCatalog();
      FieldUtils.writeField(catalog, "gravitinoCatalogClient", source, true);
      TableCatalog physical =
          catalog.createAndInitSparkCatalog(
              "test",
              new CaseInsensitiveStringMap(Map.of()),
              Map.of(
                  "jdbc-url",
                  "jdbc:postgresql://localhost/db",
                  "jdbc-driver",
                  "org.postgresql.Driver"));
      JDBCOptions options = (JDBCOptions) FieldUtils.readField(physical, "options", true);
      boolean postgres = "jdbc-postgresql".equals(provider);
      Assertions.assertEquals(postgres, options.preferTimestampNTZ());
      Assertions.assertEquals(
          postgres ? DataTypes.TimestampNTZType : DataTypes.TimestampType,
          catalog.getSparkTypeConverter().toSparkType(Types.TimestampType.withoutTimeZone()));
      Assertions.assertEquals(
          DataTypes.TimestampType,
          catalog.getSparkTypeConverter().toSparkType(Types.TimestampType.withTimeZone()));
      Assertions.assertEquals("password", options.asConnectionProperties().getProperty("password"));
      verify(credentials).getCredentials();
    }
  }

  @Test
  void testCredentialVendingOverridesProperties() {
    Map<String, String> props = new HashMap<>();
    props.put(JdbcPropertiesConstants.SPARK_JDBC_USER, "prop-user");
    props.put(JdbcPropertiesConstants.SPARK_JDBC_PASSWORD, "prop-password");

    Catalog catalog =
        new CredentialCatalogStub(new JdbcCredential("vended-user", "vended-password"));
    GravitinoJdbcCatalog.applyJdbcCredential(catalog, props);

    Assertions.assertEquals("vended-user", props.get(JdbcPropertiesConstants.SPARK_JDBC_USER));
    Assertions.assertEquals(
        "vended-password", props.get(JdbcPropertiesConstants.SPARK_JDBC_PASSWORD));
  }

  @Test
  void testFallsBackToPropertiesWhenNoJdbcCredential() {
    Map<String, String> props = new HashMap<>();
    props.put(JdbcPropertiesConstants.SPARK_JDBC_USER, "prop-user");
    props.put(JdbcPropertiesConstants.SPARK_JDBC_PASSWORD, "prop-password");

    GravitinoJdbcCatalog.applyJdbcCredential(new CredentialCatalogStub(), props);

    Assertions.assertEquals("prop-user", props.get(JdbcPropertiesConstants.SPARK_JDBC_USER));
    Assertions.assertEquals(
        "prop-password", props.get(JdbcPropertiesConstants.SPARK_JDBC_PASSWORD));
  }

  @Test
  void testFallsBackToPropertiesWhenCatalogDoesNotSupportCredentials() {
    Map<String, String> props = new HashMap<>();
    props.put(JdbcPropertiesConstants.SPARK_JDBC_USER, "prop-user");
    props.put(JdbcPropertiesConstants.SPARK_JDBC_PASSWORD, "prop-password");

    GravitinoJdbcCatalog.applyJdbcCredential(new PlainCatalogStub(), props);

    Assertions.assertEquals("prop-user", props.get(JdbcPropertiesConstants.SPARK_JDBC_USER));
    Assertions.assertEquals(
        "prop-password", props.get(JdbcPropertiesConstants.SPARK_JDBC_PASSWORD));
  }
}
