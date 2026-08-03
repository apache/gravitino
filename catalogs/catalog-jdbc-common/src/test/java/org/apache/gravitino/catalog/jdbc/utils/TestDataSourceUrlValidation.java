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
package org.apache.gravitino.catalog.jdbc.utils;

import com.google.common.collect.Maps;
import java.sql.SQLException;
import java.util.HashMap;
import javax.sql.DataSource;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.gravitino.catalog.jdbc.config.JdbcConfig;
import org.apache.gravitino.exceptions.GravitinoRuntimeException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestDataSourceUrlValidation {

  @Test
  public void testCreateDataSource() throws SQLException {
    HashMap<String, String> properties = Maps.newHashMap();
    properties.put(JdbcConfig.JDBC_DRIVER.getKey(), "org.sqlite.JDBC");
    properties.put(JdbcConfig.JDBC_URL.getKey(), "jdbc:sqlite::memory:");
    properties.put(JdbcConfig.USERNAME.getKey(), "test");
    properties.put(JdbcConfig.PASSWORD.getKey(), "test");

    DataSource dataSource =
        Assertions.assertDoesNotThrow(() -> DataSourceUtils.createDataSource(properties));
    Assertions.assertTrue(dataSource instanceof org.apache.commons.dbcp2.BasicDataSource);
    ((BasicDataSource) dataSource).close();
  }

  @Test
  public void testSimilarNamedPropertyIsNotRejected() throws SQLException {
    // The unsafe-pool-property guard matches the whole key (equalsIgnoreCase), so a legitimate key
    // that merely contains an unsafe name as a substring must still build. Guards against a
    // regression to contains/startsWith matching.
    HashMap<String, String> properties = Maps.newHashMap();
    properties.put(JdbcConfig.JDBC_DRIVER.getKey(), "org.sqlite.JDBC");
    properties.put(JdbcConfig.JDBC_URL.getKey(), "jdbc:sqlite::memory:");
    properties.put(JdbcConfig.USERNAME.getKey(), "test");
    properties.put(JdbcConfig.PASSWORD.getKey(), "test");
    properties.put("connectionFactoryClassNameHelper", "com.example.Fine");

    DataSource dataSource =
        Assertions.assertDoesNotThrow(() -> DataSourceUtils.createDataSource(properties));
    Assertions.assertTrue(dataSource instanceof BasicDataSource);
    ((BasicDataSource) dataSource).close();
  }

  @Test
  public void testRejectMysqlAllowLoadLocalInfile() {
    HashMap<String, String> properties = Maps.newHashMap();
    properties.put(JdbcConfig.JDBC_DRIVER.getKey(), "com.mysql.cj.jdbc.Driver");
    properties.put(
        JdbcConfig.JDBC_URL.getKey(), "jdbc:mysql://localhost:3306/test?allowLoadLocalInfile=true");
    properties.put(JdbcConfig.USERNAME.getKey(), "test");
    properties.put(JdbcConfig.PASSWORD.getKey(), "test");

    GravitinoRuntimeException gre =
        Assertions.assertThrows(
            GravitinoRuntimeException.class, () -> DataSourceUtils.createDataSource(properties));
    // createDataSource re-wraps the validation failure, so assert on the cause to prove the
    // security check fired (not some unrelated pool/driver error). The reported name is the
    // canonical (lower-case) entry from the unsafe-parameter list.
    Assertions.assertNotNull(gre.getCause());
    Assertions.assertEquals(
        "Unsafe MySQL parameter 'allowloadlocalinfile' detected in JDBC configuration",
        gre.getCause().getMessage());
  }

  @Test
  public void testRejectPostgresSocketFactory() {
    HashMap<String, String> properties = Maps.newHashMap();
    properties.put(JdbcConfig.JDBC_DRIVER.getKey(), "org.postgresql.Driver");
    properties.put(
        JdbcConfig.JDBC_URL.getKey(),
        "jdbc:postgresql:///test?socketFactory=java.io.FileOutputStream&socketFactoryArg=/tmp/x");
    properties.put(JdbcConfig.USERNAME.getKey(), "test");
    properties.put(JdbcConfig.PASSWORD.getKey(), "test");

    GravitinoRuntimeException gre =
        Assertions.assertThrows(
            GravitinoRuntimeException.class, () -> DataSourceUtils.createDataSource(properties));
    Assertions.assertNotNull(gre.getCause());
    Assertions.assertEquals(
        "Unsafe PostgreSQL parameter 'socketFactory' detected in JDBC configuration",
        gre.getCause().getMessage());
  }

  @Test
  public void testRejectEncodedAllowLoadLocalInfile() {
    HashMap<String, String> properties = Maps.newHashMap();
    properties.put(JdbcConfig.JDBC_DRIVER.getKey(), "org.postgresql.Driver");
    properties.put(
        JdbcConfig.JDBC_URL.getKey(),
        "jdbc:mysql://address=(host=172.18.0.1)(port=3309)(%61llowLoadLocalInfile=true),172.18.0.1:3306/test");
    properties.put(JdbcConfig.USERNAME.getKey(), "test");
    properties.put(JdbcConfig.PASSWORD.getKey(), "test");

    GravitinoRuntimeException gre =
        Assertions.assertThrows(
            GravitinoRuntimeException.class, () -> DataSourceUtils.createDataSource(properties));
    Assertions.assertNotNull(gre.getCause());
    Assertions.assertEquals(
        "Unsafe MySQL parameter 'allowloadlocalinfile' detected in JDBC configuration",
        gre.getCause().getMessage());
  }

  @Test
  public void testRejectMysqlUnsafeParamInConnectionProperties() {
    // DBCP2 forwards the "connectionProperties" value straight to the JDBC driver, so an unsafe
    // MySQL parameter smuggled here must still be rejected. (In the catalog path a user reaches
    // this key via the "gravitino.bypass." prefix, which is stripped before the config map is
    // handed to the validator.)
    HashMap<String, String> properties = Maps.newHashMap();
    properties.put(JdbcConfig.JDBC_DRIVER.getKey(), "com.mysql.cj.jdbc.Driver");
    properties.put(JdbcConfig.JDBC_URL.getKey(), "jdbc:mysql://localhost:3306/test");
    properties.put(JdbcConfig.USERNAME.getKey(), "test");
    properties.put(JdbcConfig.PASSWORD.getKey(), "test");
    properties.put("connectionProperties", "autoDeserialize=true");

    GravitinoRuntimeException gre =
        Assertions.assertThrows(
            GravitinoRuntimeException.class, () -> DataSourceUtils.createDataSource(properties));
    // createDataSource re-wraps the validation failure, so assert on the cause to prove the
    // security check fired (not some unrelated pool/driver error).
    Assertions.assertNotNull(gre.getCause());
    Assertions.assertEquals(
        "Unsafe MySQL parameter 'autoDeserialize' detected in JDBC configuration",
        gre.getCause().getMessage());
  }

  @Test
  public void testRejectH2Url() {
    HashMap<String, String> properties = Maps.newHashMap();
    properties.put(JdbcConfig.JDBC_DRIVER.getKey(), "org.postgresql.Driver");
    properties.put(
        JdbcConfig.JDBC_URL.getKey(),
        "jdbc:h2:mem:test;INIT=CREATE ALIAS EXEC AS 'String f() throws Exception"
            + " { Runtime.getRuntime().exec(\"id\"); return \"ok\"; }'\\;CALL EXEC()");
    properties.put(JdbcConfig.USERNAME.getKey(), "test");
    properties.put(JdbcConfig.PASSWORD.getKey(), "test");

    GravitinoRuntimeException gre =
        Assertions.assertThrows(
            GravitinoRuntimeException.class, () -> DataSourceUtils.createDataSource(properties));
    Assertions.assertEquals(
        "H2 JDBC URL is not allowed in catalog configuration", gre.getMessage());
  }

  @Test
  public void testRejectH2UrlCaseInsensitive() {
    HashMap<String, String> properties = Maps.newHashMap();
    properties.put(JdbcConfig.JDBC_DRIVER.getKey(), "org.postgresql.Driver");
    properties.put(JdbcConfig.JDBC_URL.getKey(), "JDBC:H2:mem:test");
    properties.put(JdbcConfig.USERNAME.getKey(), "test");
    properties.put(JdbcConfig.PASSWORD.getKey(), "test");

    GravitinoRuntimeException gre =
        Assertions.assertThrows(
            GravitinoRuntimeException.class, () -> DataSourceUtils.createDataSource(properties));
    Assertions.assertEquals(
        "H2 JDBC URL is not allowed in catalog configuration", gre.getMessage());
  }

  @Test
  public void testRejectH2Driver() {
    HashMap<String, String> properties = Maps.newHashMap();
    properties.put(JdbcConfig.JDBC_DRIVER.getKey(), "org.h2.Driver");
    properties.put(JdbcConfig.JDBC_URL.getKey(), "jdbc:postgresql://localhost:5432/test");
    properties.put(JdbcConfig.USERNAME.getKey(), "test");
    properties.put(JdbcConfig.PASSWORD.getKey(), "test");

    GravitinoRuntimeException gre =
        Assertions.assertThrows(
            GravitinoRuntimeException.class, () -> DataSourceUtils.createDataSource(properties));
    Assertions.assertEquals(
        "H2 JDBC driver is not allowed in catalog configuration", gre.getMessage());
  }

  @Test
  public void testRejectH2DriverCaseInsensitive() {
    HashMap<String, String> properties = Maps.newHashMap();
    properties.put(JdbcConfig.JDBC_DRIVER.getKey(), "ORG.H2.DRIVER");
    properties.put(JdbcConfig.JDBC_URL.getKey(), "jdbc:postgresql://localhost:5432/test");
    properties.put(JdbcConfig.USERNAME.getKey(), "test");
    properties.put(JdbcConfig.PASSWORD.getKey(), "test");

    GravitinoRuntimeException gre =
        Assertions.assertThrows(
            GravitinoRuntimeException.class, () -> DataSourceUtils.createDataSource(properties));
    Assertions.assertEquals(
        "H2 JDBC driver is not allowed in catalog configuration", gre.getMessage());
  }

  @Test
  public void testRejectConnectionFactoryClassName() {
    // DBCP2's BasicDataSourceFactory loads "connectionFactoryClassName" via Class.forName and
    // instantiates it at pool init, which is a remote-code-execution vector. A user can reach this
    // key through the "gravitino.bypass." prefix (stripped before the config map is handed here),
    // so it must be rejected.
    HashMap<String, String> properties = Maps.newHashMap();
    properties.put(JdbcConfig.JDBC_DRIVER.getKey(), "com.mysql.cj.jdbc.Driver");
    properties.put(JdbcConfig.JDBC_URL.getKey(), "jdbc:mysql://localhost:3306/test");
    properties.put(JdbcConfig.USERNAME.getKey(), "test");
    properties.put(JdbcConfig.PASSWORD.getKey(), "test");
    properties.put("connectionFactoryClassName", "com.example.Evil");

    GravitinoRuntimeException gre =
        Assertions.assertThrows(
            GravitinoRuntimeException.class, () -> DataSourceUtils.createDataSource(properties));
    Assertions.assertEquals(
        "Unsafe JDBC connection pool property 'connectionFactoryClassName' is not allowed in"
            + " catalog configuration",
        gre.getMessage());
  }

  @Test
  public void testRejectEvictionPolicyClassName() {
    HashMap<String, String> properties = Maps.newHashMap();
    properties.put(JdbcConfig.JDBC_DRIVER.getKey(), "com.mysql.cj.jdbc.Driver");
    properties.put(JdbcConfig.JDBC_URL.getKey(), "jdbc:mysql://localhost:3306/test");
    properties.put(JdbcConfig.USERNAME.getKey(), "test");
    properties.put(JdbcConfig.PASSWORD.getKey(), "test");
    properties.put("evictionPolicyClassName", "com.example.Evil");

    GravitinoRuntimeException gre =
        Assertions.assertThrows(
            GravitinoRuntimeException.class, () -> DataSourceUtils.createDataSource(properties));
    Assertions.assertEquals(
        "Unsafe JDBC connection pool property 'evictionPolicyClassName' is not allowed in catalog"
            + " configuration",
        gre.getMessage());
  }

  @Test
  public void testRejectConnectionFactoryClassNameCaseInsensitive() {
    // The guard matches case-insensitively (broader than DBCP2's own case-sensitive key lookup),
    // so a mis-cased variant is rejected outright rather than silently ignored. The error echoes
    // the user-supplied key casing.
    HashMap<String, String> properties = Maps.newHashMap();
    properties.put(JdbcConfig.JDBC_DRIVER.getKey(), "com.mysql.cj.jdbc.Driver");
    properties.put(JdbcConfig.JDBC_URL.getKey(), "jdbc:mysql://localhost:3306/test");
    properties.put(JdbcConfig.USERNAME.getKey(), "test");
    properties.put(JdbcConfig.PASSWORD.getKey(), "test");
    properties.put("ConnectionFactoryClassName", "com.example.Evil");

    GravitinoRuntimeException gre =
        Assertions.assertThrows(
            GravitinoRuntimeException.class, () -> DataSourceUtils.createDataSource(properties));
    Assertions.assertEquals(
        "Unsafe JDBC connection pool property 'ConnectionFactoryClassName' is not allowed in"
            + " catalog configuration",
        gre.getMessage());
  }

  @Test
  public void testRejectEvictionPolicyClassNameCaseInsensitive() {
    HashMap<String, String> properties = Maps.newHashMap();
    properties.put(JdbcConfig.JDBC_DRIVER.getKey(), "com.mysql.cj.jdbc.Driver");
    properties.put(JdbcConfig.JDBC_URL.getKey(), "jdbc:mysql://localhost:3306/test");
    properties.put(JdbcConfig.USERNAME.getKey(), "test");
    properties.put(JdbcConfig.PASSWORD.getKey(), "test");
    properties.put("EVICTIONPOLICYCLASSNAME", "com.example.Evil");

    GravitinoRuntimeException gre =
        Assertions.assertThrows(
            GravitinoRuntimeException.class, () -> DataSourceUtils.createDataSource(properties));
    Assertions.assertEquals(
        "Unsafe JDBC connection pool property 'EVICTIONPOLICYCLASSNAME' is not allowed in catalog"
            + " configuration",
        gre.getMessage());
  }

  @Test
  public void testRejectDriverClassName() {
    // DBCP2's BasicDataSourceFactory loads the "driverClassName" value via Class.forName and
    // instantiates it (an RCE vector) when the pool creates a connection. The legitimate driver is
    // set separately from the "jdbc-driver" property, so the raw key must be rejected.
    HashMap<String, String> properties = Maps.newHashMap();
    properties.put(JdbcConfig.JDBC_DRIVER.getKey(), "com.mysql.cj.jdbc.Driver");
    properties.put(JdbcConfig.JDBC_URL.getKey(), "jdbc:mysql://localhost:3306/test");
    properties.put(JdbcConfig.USERNAME.getKey(), "test");
    properties.put(JdbcConfig.PASSWORD.getKey(), "test");
    properties.put("driverClassName", "com.example.Evil");

    GravitinoRuntimeException gre =
        Assertions.assertThrows(
            GravitinoRuntimeException.class, () -> DataSourceUtils.createDataSource(properties));
    Assertions.assertEquals(
        "Unsafe JDBC connection pool property 'driverClassName' is not allowed in catalog"
            + " configuration",
        gre.getMessage());
  }

  @Test
  public void testRejectInitialSize() {
    // initialSize > 0 makes the factory eagerly open a connection during createDataSource; reject
    // it (defense in depth) so pool creation stays lazy and our explicit url/driver setters control
    // how connections are created.
    HashMap<String, String> properties = Maps.newHashMap();
    properties.put(JdbcConfig.JDBC_DRIVER.getKey(), "com.mysql.cj.jdbc.Driver");
    properties.put(JdbcConfig.JDBC_URL.getKey(), "jdbc:mysql://localhost:3306/test");
    properties.put(JdbcConfig.USERNAME.getKey(), "test");
    properties.put(JdbcConfig.PASSWORD.getKey(), "test");
    properties.put("initialSize", "1");

    GravitinoRuntimeException gre =
        Assertions.assertThrows(
            GravitinoRuntimeException.class, () -> DataSourceUtils.createDataSource(properties));
    Assertions.assertEquals(
        "Unsafe JDBC connection pool property 'initialSize' is not allowed in catalog"
            + " configuration",
        gre.getMessage());
  }

  @Test
  public void testRejectRawUrlProperty() {
    // A raw DBCP "url" key would reach the factory unvalidated and, once the pool initializes, take
    // effect before the canonical setUrl override — smuggling a denied URL (e.g. H2) past the
    // guards. The canonical URL is supplied via "jdbc-url", so the raw key must be rejected.
    HashMap<String, String> properties = Maps.newHashMap();
    properties.put(JdbcConfig.JDBC_DRIVER.getKey(), "com.mysql.cj.jdbc.Driver");
    properties.put(JdbcConfig.JDBC_URL.getKey(), "jdbc:mysql://localhost:3306/test");
    properties.put(JdbcConfig.USERNAME.getKey(), "test");
    properties.put(JdbcConfig.PASSWORD.getKey(), "test");
    properties.put("url", "jdbc:h2:mem:evil");

    GravitinoRuntimeException gre =
        Assertions.assertThrows(
            GravitinoRuntimeException.class, () -> DataSourceUtils.createDataSource(properties));
    Assertions.assertEquals(
        "Unsafe JDBC connection pool property 'url' is not allowed in catalog configuration",
        gre.getMessage());
  }

  @Test
  public void testRejectRawUsernameProperty() {
    HashMap<String, String> properties = Maps.newHashMap();
    properties.put(JdbcConfig.JDBC_DRIVER.getKey(), "com.mysql.cj.jdbc.Driver");
    properties.put(JdbcConfig.JDBC_URL.getKey(), "jdbc:mysql://localhost:3306/test");
    properties.put(JdbcConfig.USERNAME.getKey(), "test");
    properties.put(JdbcConfig.PASSWORD.getKey(), "test");
    properties.put("username", "root");

    GravitinoRuntimeException gre =
        Assertions.assertThrows(
            GravitinoRuntimeException.class, () -> DataSourceUtils.createDataSource(properties));
    Assertions.assertEquals(
        "Unsafe JDBC connection pool property 'username' is not allowed in catalog configuration",
        gre.getMessage());
  }

  @Test
  public void testRejectRawPasswordProperty() {
    HashMap<String, String> properties = Maps.newHashMap();
    properties.put(JdbcConfig.JDBC_DRIVER.getKey(), "com.mysql.cj.jdbc.Driver");
    properties.put(JdbcConfig.JDBC_URL.getKey(), "jdbc:mysql://localhost:3306/test");
    properties.put(JdbcConfig.USERNAME.getKey(), "test");
    properties.put(JdbcConfig.PASSWORD.getKey(), "test");
    properties.put("password", "secret");

    GravitinoRuntimeException gre =
        Assertions.assertThrows(
            GravitinoRuntimeException.class, () -> DataSourceUtils.createDataSource(properties));
    Assertions.assertEquals(
        "Unsafe JDBC connection pool property 'password' is not allowed in catalog configuration",
        gre.getMessage());
  }

  @Test
  public void testRawUrlBypassCannotSmuggleH2ViaEagerInit() {
    // Regression for the factory-init ordering issue: even with initialSize > 0 forcing eager pool
    // init AND a raw "url" pointing at H2, the config must be rejected before the factory runs, so
    // the H2 URL never becomes the live connection.
    HashMap<String, String> properties = Maps.newHashMap();
    properties.put(JdbcConfig.JDBC_DRIVER.getKey(), "org.sqlite.JDBC");
    properties.put(JdbcConfig.JDBC_URL.getKey(), "jdbc:sqlite::memory:");
    properties.put(JdbcConfig.USERNAME.getKey(), "test");
    properties.put(JdbcConfig.PASSWORD.getKey(), "test");
    properties.put("url", "jdbc:h2:mem:evil");
    properties.put("initialSize", "1");

    // Rejected on the first unsafe key encountered (url or initialSize); the point is that
    // creation fails before BasicDataSourceFactory initializes the pool with the raw url.
    Assertions.assertThrows(
        GravitinoRuntimeException.class, () -> DataSourceUtils.createDataSource(properties));
  }

  @Test
  public void testValidConnectionUsesCanonicalUrl() throws SQLException {
    // Positive control: with only canonical keys, the live connection uses the canonical URL, not
    // any bootstrap value — proving the ordering fix holds for legitimate configs.
    HashMap<String, String> properties = Maps.newHashMap();
    properties.put(JdbcConfig.JDBC_DRIVER.getKey(), "org.sqlite.JDBC");
    properties.put(JdbcConfig.JDBC_URL.getKey(), "jdbc:sqlite::memory:");
    properties.put(JdbcConfig.USERNAME.getKey(), "test");
    properties.put(JdbcConfig.PASSWORD.getKey(), "test");

    BasicDataSource dataSource =
        (BasicDataSource)
            Assertions.assertDoesNotThrow(() -> DataSourceUtils.createDataSource(properties));
    Assertions.assertEquals("jdbc:sqlite::memory:", dataSource.getUrl());
    try (java.sql.Connection connection = dataSource.getConnection()) {
      Assertions.assertEquals("jdbc:sqlite::memory:", connection.getMetaData().getURL());
    } finally {
      dataSource.close();
    }
  }
}
