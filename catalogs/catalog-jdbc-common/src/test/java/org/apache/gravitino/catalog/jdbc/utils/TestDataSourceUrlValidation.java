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
}
