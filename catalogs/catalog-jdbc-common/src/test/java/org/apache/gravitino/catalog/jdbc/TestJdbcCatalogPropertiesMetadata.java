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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.Maps;
import java.util.Map;
import org.apache.gravitino.catalog.jdbc.config.JdbcConfig;
import org.apache.gravitino.connector.PropertyEntry;
import org.junit.jupiter.api.Test;

public class TestJdbcCatalogPropertiesMetadata {

  @Test
  public void testUsernamePropertyIsSensitive() {
    JdbcCatalogPropertiesMetadata metadata = new JdbcCatalogPropertiesMetadata();
    Map<String, PropertyEntry<?>> properties = metadata.specificPropertyEntries();

    PropertyEntry<?> usernameEntry = properties.get(JdbcConfig.USERNAME.getKey());
    assertNotNull(usernameEntry, "Username property should be defined");
    assertTrue(usernameEntry.isSensitive(), "Username should be marked as sensitive");
    assertTrue(usernameEntry.isRequired(), "Username should be required");
    assertFalse(usernameEntry.isImmutable(), "Username should not be immutable");
    assertFalse(usernameEntry.isHidden(), "Username should not be hidden");
    assertFalse(usernameEntry.isReserved(), "Username should not be reserved");
  }

  @Test
  public void testPasswordPropertyIsSensitive() {
    JdbcCatalogPropertiesMetadata metadata = new JdbcCatalogPropertiesMetadata();
    Map<String, PropertyEntry<?>> properties = metadata.specificPropertyEntries();

    PropertyEntry<?> passwordEntry = properties.get(JdbcConfig.PASSWORD.getKey());
    assertNotNull(passwordEntry, "Password property should be defined");
    assertTrue(passwordEntry.isSensitive(), "Password should be marked as sensitive");
    assertTrue(passwordEntry.isRequired(), "Password should be required");
    assertFalse(passwordEntry.isImmutable(), "Password should not be immutable");
    assertFalse(passwordEntry.isHidden(), "Password should not be hidden");
    assertFalse(passwordEntry.isReserved(), "Password should not be reserved");
  }

  @Test
  public void testJdbcUrlPropertyIsNotSensitive() {
    JdbcCatalogPropertiesMetadata metadata = new JdbcCatalogPropertiesMetadata();
    Map<String, PropertyEntry<?>> properties = metadata.specificPropertyEntries();

    PropertyEntry<?> urlEntry = properties.get(JdbcConfig.JDBC_URL.getKey());
    assertNotNull(urlEntry, "JDBC URL property should be defined");
    assertFalse(urlEntry.isSensitive(), "JDBC URL should not be marked as sensitive");
    assertTrue(urlEntry.isRequired(), "JDBC URL should be required");
  }

  @Test
  public void testJdbcDriverPropertyIsNotSensitive() {
    JdbcCatalogPropertiesMetadata metadata = new JdbcCatalogPropertiesMetadata();
    Map<String, PropertyEntry<?>> properties = metadata.specificPropertyEntries();

    PropertyEntry<?> driverEntry = properties.get(JdbcConfig.JDBC_DRIVER.getKey());
    assertNotNull(driverEntry, "JDBC driver property should be defined");
    assertFalse(driverEntry.isSensitive(), "JDBC driver should not be marked as sensitive");
    assertTrue(driverEntry.isRequired(), "JDBC driver should be required");
  }

  @Test
  public void testTransformPropertiesIncludesCredentials() {
    JdbcCatalogPropertiesMetadata metadata = new JdbcCatalogPropertiesMetadata();
    Map<String, String> inputProperties = Maps.newHashMap();
    inputProperties.put(JdbcConfig.JDBC_URL.getKey(), "jdbc:mysql://localhost:3306/test");
    inputProperties.put(JdbcConfig.JDBC_DRIVER.getKey(), "com.mysql.cj.jdbc.Driver");
    inputProperties.put(JdbcConfig.USERNAME.getKey(), "testuser");
    inputProperties.put(JdbcConfig.PASSWORD.getKey(), "testpass");
    inputProperties.put("non-jdbc-property", "value");

    Map<String, String> result = metadata.transformProperties(inputProperties);

    assertTrue(result.containsKey(JdbcConfig.USERNAME.getKey()), "Username should be included");
    assertTrue(result.containsKey(JdbcConfig.PASSWORD.getKey()), "Password should be included");
    assertEquals("testuser", result.get(JdbcConfig.USERNAME.getKey()));
    assertEquals("testpass", result.get(JdbcConfig.PASSWORD.getKey()));
    assertFalse(
        result.containsKey("non-jdbc-property"), "Non-JDBC properties should not be included");
  }

  @Test
  public void testAllRequiredPropertiesAreDefined() {
    JdbcCatalogPropertiesMetadata metadata = new JdbcCatalogPropertiesMetadata();
    Map<String, PropertyEntry<?>> properties = metadata.specificPropertyEntries();

    // Required properties
    assertNotNull(properties.get(JdbcConfig.JDBC_URL.getKey()));
    assertNotNull(properties.get(JdbcConfig.JDBC_DRIVER.getKey()));
    assertNotNull(properties.get(JdbcConfig.USERNAME.getKey()));
    assertNotNull(properties.get(JdbcConfig.PASSWORD.getKey()));

    // Optional properties
    assertNotNull(properties.get(JdbcConfig.JDBC_DATABASE.getKey()));
    assertNotNull(properties.get(JdbcConfig.POOL_MIN_SIZE.getKey()));
    assertNotNull(properties.get(JdbcConfig.POOL_MAX_SIZE.getKey()));
    assertNotNull(properties.get(JdbcConfig.TEST_ON_BORROW.getKey()));
  }

  @Test
  public void testPoolPropertiesAreHidden() {
    JdbcCatalogPropertiesMetadata metadata = new JdbcCatalogPropertiesMetadata();
    Map<String, PropertyEntry<?>> properties = metadata.specificPropertyEntries();

    PropertyEntry<?> poolMinSize = properties.get(JdbcConfig.POOL_MIN_SIZE.getKey());
    PropertyEntry<?> poolMaxSize = properties.get(JdbcConfig.POOL_MAX_SIZE.getKey());
    PropertyEntry<?> testOnBorrow = properties.get(JdbcConfig.TEST_ON_BORROW.getKey());

    assertTrue(poolMinSize.isHidden(), "Pool min size should be hidden");
    assertTrue(poolMaxSize.isHidden(), "Pool max size should be hidden");
    assertTrue(testOnBorrow.isHidden(), "Test on borrow should be hidden");
  }

  @Test
  public void testSensitivePropertiesDescription() {
    JdbcCatalogPropertiesMetadata metadata = new JdbcCatalogPropertiesMetadata();
    Map<String, PropertyEntry<?>> properties = metadata.specificPropertyEntries();

    PropertyEntry<?> usernameEntry = properties.get(JdbcConfig.USERNAME.getKey());
    PropertyEntry<?> passwordEntry = properties.get(JdbcConfig.PASSWORD.getKey());

    assertNotNull(usernameEntry.getDescription(), "Username should have a description");
    assertNotNull(passwordEntry.getDescription(), "Password should have a description");
    assertEquals(JdbcConfig.USERNAME.getDoc(), usernameEntry.getDescription());
    assertEquals(JdbcConfig.PASSWORD.getDoc(), passwordEntry.getDescription());
  }
}
