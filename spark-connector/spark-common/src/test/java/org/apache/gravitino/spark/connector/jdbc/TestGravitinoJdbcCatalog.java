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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.Audit;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.credential.Credential;
import org.apache.gravitino.credential.JdbcCredential;
import org.apache.gravitino.credential.SupportsCredentials;
import org.apache.gravitino.spark.connector.catalog.GravitinoCatalogManager;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
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

  @Test
  void testInitializationRotationIsComparedWithActualJdbcPassword() throws Exception {
    GravitinoCatalogManager manager = mock(GravitinoCatalogManager.class);
    try (MockedStatic<GravitinoCatalogManager> global = mockStatic(GravitinoCatalogManager.class)) {
      global.when(GravitinoCatalogManager::get).thenReturn(manager);
      GravitinoJdbcCatalog catalog = new GravitinoJdbcCatalog();
      Catalog source = catalog("jdbc:postgresql://localhost/db", "before", Instant.EPOCH);
      when(source.supportsCredentials().getCredentials())
          .thenReturn(
              new Credential[] {new JdbcCredential("user", "before")},
              new Credential[] {new JdbcCredential("user", "rotated")});
      FieldUtils.writeField(catalog, "gravitinoCatalogClient", source, true);
      TableCatalog physical =
          catalog.createAndInitSparkCatalog(
              "test", new CaseInsensitiveStringMap(Map.of()), source.properties());
      JDBCOptions options = (JDBCOptions) FieldUtils.readField(physical, "options", true);
      String actualPassword = options.asConnectionProperties().getProperty("password");
      Assertions.assertTrue(Set.of("before", "rotated").contains(actualPassword));
      when(manager.reloadGravitinoCatalogInfo(any())).thenReturn(source);
      // The guard must compare against the password actually installed in Spark, even if the
      // source rotates credentials between initialization-time RPCs.
      if ("rotated".equals(actualPassword)) {
        Assertions.assertDoesNotThrow(catalog::checkCatalogUnchanged);
      } else {
        Assertions.assertThrows(IllegalStateException.class, catalog::checkCatalogUnchanged);
      }
    }
  }

  @Test
  void testConfigurationAndCredentialChangesFailClosed() throws Exception {
    GravitinoCatalogManager manager = mock(GravitinoCatalogManager.class);
    try (MockedStatic<GravitinoCatalogManager> global = mockStatic(GravitinoCatalogManager.class)) {
      global.when(GravitinoCatalogManager::get).thenReturn(manager);
      GravitinoJdbcCatalog catalog = new GravitinoJdbcCatalog();
      Catalog initial = catalog("jdbc:postgresql://localhost/db", "password-one", Instant.EPOCH);
      FieldUtils.writeField(catalog, "gravitinoCatalogClient", initial, true);
      catalog.createAndInitSparkCatalog(
          "test", new CaseInsensitiveStringMap(Map.of()), initial.properties());
      when(manager.reloadGravitinoCatalogInfo(any())).thenReturn(initial);
      Assertions.assertDoesNotThrow(catalog::checkCatalogUnchanged);
      for (Catalog changed :
          new Catalog[] {
            catalog("jdbc:postgresql://other/db", "password-one", Instant.EPOCH),
            catalog("jdbc:postgresql://localhost/db", "password-two", Instant.EPOCH),
            catalog("jdbc:postgresql://localhost/db", "password-one", Instant.EPOCH.plusSeconds(1))
          }) {
        when(manager.reloadGravitinoCatalogInfo(any())).thenReturn(changed);
        IllegalStateException error =
            Assertions.assertThrows(
                IllegalStateException.class,
                () -> catalog.loadSparkTable(Identifier.of(new String[] {"public"}, "t")));
        Assertions.assertTrue(error.getMessage().contains("restart the Spark session"));
        Assertions.assertFalse(error.getMessage().contains("password-one"));
        Assertions.assertFalse(error.getMessage().contains("password-two"));
      }
      when(manager.reloadGravitinoCatalogInfo(any()))
          .thenThrow(new IllegalStateException("server unavailable"));
      Assertions.assertThrows(
          IllegalStateException.class,
          () -> catalog.loadSparkTable(Identifier.of(new String[] {"public"}, "t")));
    }
  }

  private Catalog catalog(String url, String password, Instant createdAt) {
    Catalog catalog = mock(Catalog.class);
    when(catalog.provider()).thenReturn("jdbc-postgresql");
    when(catalog.properties())
        .thenReturn(
            Map.of(
                "jdbc-url",
                url,
                "jdbc-user",
                "user",
                "jdbc-password",
                "******",
                "jdbc-driver",
                "org.postgresql.Driver"));
    Audit audit = mock(Audit.class);
    when(audit.createTime()).thenReturn(createdAt);
    when(catalog.auditInfo()).thenReturn(audit);
    SupportsCredentials credentials = mock(SupportsCredentials.class);
    when(credentials.getCredentials())
        .thenReturn(new Credential[] {new JdbcCredential("user", password)});
    when(catalog.supportsCredentials()).thenReturn(credentials);
    return catalog;
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
