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
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.credential.Credential;
import org.apache.gravitino.credential.JdbcCredential;
import org.apache.gravitino.credential.SupportsCredentials;
import org.apache.gravitino.spark.connector.catalog.GravitinoCatalogManager;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

/** Tests that a table the catalog user cannot read reports the source's error. */
public class TestGravitinoJdbcCatalogSourceError {

  private static final String URL = "jdbc:h2:mem:spark_jdbc_source_error;DB_CLOSE_DELAY=-1";
  private static final String ADMIN = "SA";
  private static final String NO_RIGHTS = "NORIGHTS";
  private static final String PASSWORD = "pw";
  private static final Identifier TABLE = Identifier.of(new String[] {"S"}, "T");

  private static Connection keepAlive;

  @BeforeAll
  static void setUp() throws SQLException {
    keepAlive = DriverManager.getConnection(URL, ADMIN, "");
    try (Statement statement = keepAlive.createStatement()) {
      statement.execute("CREATE SCHEMA S");
      statement.execute("CREATE TABLE S.T (ID INT)");
      statement.execute("CREATE USER " + NO_RIGHTS + " PASSWORD '" + PASSWORD + "'");
    }
  }

  @AfterAll
  static void tearDown() throws SQLException {
    try (Statement statement = keepAlive.createStatement()) {
      statement.execute("SHUTDOWN");
    }
  }

  @Test
  void testSourceErrorNamesTheMissingPrivilege() {
    SQLException error = GravitinoJdbcCatalog.sourceError(options(NO_RIGHTS, PASSWORD), TABLE);
    Assertions.assertNotNull(error);
    Assertions.assertTrue(error.getMessage().contains("rights"), error.getMessage());
  }

  @Test
  void testNoSourceErrorWhenTheSchemaCanBeRead() {
    Assertions.assertNull(GravitinoJdbcCatalog.sourceError(options(ADMIN, ""), TABLE));
  }

  @Test
  void testNoSourceErrorWithoutUrl() {
    Assertions.assertNull(GravitinoJdbcCatalog.sourceError(new HashMap<>(), TABLE));
  }

  @Test
  void testLoadReportsTheSourceErrorInsteadOfNotFound() throws Exception {
    try (MockedStatic<GravitinoCatalogManager> global = mockStatic(GravitinoCatalogManager.class)) {
      global.when(GravitinoCatalogManager::get).thenReturn(mock(GravitinoCatalogManager.class));
      SupportsCredentials credentials = mock(SupportsCredentials.class);
      when(credentials.getCredentials())
          .thenReturn(new Credential[] {new JdbcCredential(NO_RIGHTS, PASSWORD)});
      Catalog source = mock(Catalog.class);
      when(source.provider()).thenReturn("jdbc-h2");
      when(source.supportsCredentials()).thenReturn(credentials);
      GravitinoJdbcCatalog catalog =
          new GravitinoJdbcCatalog() {
            @Override
            protected Catalog gravitinoCatalog() {
              return source;
            }
          };

      Map<String, String> properties = new HashMap<>();
      properties.put("jdbc-url", URL);
      properties.put("jdbc-driver", "org.h2.Driver");
      TableCatalog physical =
          catalog.createAndInitSparkCatalog(
              "test", new CaseInsensitiveStringMap(new HashMap<>()), properties);
      FieldUtils.writeField(catalog, "sparkCatalog", physical, true);

      // Spark 3.x reports the failed schema read as a missing table, which the catalog replaces
      // with the source's error. Spark 4.0 already reports it as FAILED_JDBC with the source
      // error as the cause. Either way the source's reason must reach the caller.
      Exception error =
          Assertions.assertThrows(Exception.class, () -> catalog.loadSparkTable(TABLE));
      Assertions.assertFalse(
          String.valueOf(error.getMessage()).contains("Failed to load the real sparkTable"),
          error.getMessage());
      Assertions.assertNotNull(
          sqlCause(error), "the source's SQL error must be in the cause chain");
    }
  }

  private static SQLException sqlCause(Throwable error) {
    for (Throwable t = error; t != null; t = t.getCause()) {
      if (t instanceof SQLException) {
        return (SQLException) t;
      }
    }
    return null;
  }

  private static Map<String, String> options(String user, String password) {
    Map<String, String> options = new HashMap<>();
    options.put("url", URL);
    options.put("driver", "org.h2.Driver");
    options.put("user", user);
    options.put("password", password);
    return options;
  }
}
