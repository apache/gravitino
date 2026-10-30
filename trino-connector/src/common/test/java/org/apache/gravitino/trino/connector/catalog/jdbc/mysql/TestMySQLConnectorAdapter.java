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

import java.util.Collections;
import java.util.Map;
import org.apache.gravitino.credential.Credential;
import org.apache.gravitino.credential.JdbcCredential;
import org.apache.gravitino.trino.connector.catalog.jdbc.JDBCCatalogPropertyConverter;
import org.apache.gravitino.trino.connector.metadata.GravitinoCatalog;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link MySQLConnectorAdapter#buildInternalConnectorConfig}. */
public class TestMySQLConnectorAdapter {

  private static final String JDBC_URL = "jdbc:mysql://localhost:3306/";

  private GravitinoCatalog catalogWithProps(Map<String, String> props) {
    return new GravitinoCatalog("metalake", "jdbc-mysql", "test", props, 0L);
  }

  @Test
  void testCredentialVendingOverridesProperties() throws Exception {
    GravitinoCatalog catalog = catalogWithProps(Collections.singletonMap("jdbc-url", JDBC_URL));
    Credential[] credentials = {new JdbcCredential("vended-user", "vended-password")};

    Map<String, String> config =
        new MySQLConnectorAdapter().buildInternalConnectorConfig(catalog, credentials);

    Assertions.assertEquals(
        "vended-user", config.get(JDBCCatalogPropertyConverter.JDBC_CONNECTION_USER_KEY));
    Assertions.assertEquals(
        "vended-password", config.get(JDBCCatalogPropertyConverter.JDBC_CONNECTION_PASSWORD_KEY));
  }

  @Test
  void testFallsBackToPropertiesWhenNoCredentials() throws Exception {
    Map<String, String> props =
        Map.of("jdbc-url", JDBC_URL, "jdbc-user", "prop-user", "jdbc-password", "prop-password");
    GravitinoCatalog catalog = catalogWithProps(props);

    Map<String, String> config =
        new MySQLConnectorAdapter().buildInternalConnectorConfig(catalog, new Credential[0]);

    Assertions.assertEquals(
        "prop-user", config.get(JDBCCatalogPropertyConverter.JDBC_CONNECTION_USER_KEY));
    Assertions.assertEquals(
        "prop-password", config.get(JDBCCatalogPropertyConverter.JDBC_CONNECTION_PASSWORD_KEY));
  }
}
