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
package org.apache.gravitino.stats.storage;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link MysqlPartitionStatisticStorageFactory}. */
public class TestMysqlPartitionStatisticStorageFactory {

  @Test
  public void testCreateWithValidConfiguration() {
    MysqlPartitionStatisticStorageFactory factory = new MysqlPartitionStatisticStorageFactory();

    Map<String, String> properties = new HashMap<>();
    properties.put("jdbc-url", "jdbc:mysql://localhost:3306/test_db");
    properties.put("jdbc-user", "test_user");
    properties.put("jdbc-password", "test_password");
    properties.put("jdbc-driver", "com.mysql.cj.jdbc.Driver");

    PartitionStatisticStorage storage = factory.create(properties);

    assertNotNull(storage);
    assertTrue(storage instanceof MysqlPartitionStatisticStorage);
  }

  @Test
  public void testCreateWithDefaultDriver() {
    MysqlPartitionStatisticStorageFactory factory = new MysqlPartitionStatisticStorageFactory();

    Map<String, String> properties = new HashMap<>();
    properties.put("jdbc-url", "jdbc:mysql://localhost:3306/test_db");
    properties.put("jdbc-user", "test_user");
    properties.put("jdbc-password", "test_password");
    // jdbc-driver not provided, should use default

    PartitionStatisticStorage storage = factory.create(properties);

    assertNotNull(storage);
    assertTrue(storage instanceof MysqlPartitionStatisticStorage);
  }

  @Test
  public void testCreateWithMissingJdbcUrl() {
    MysqlPartitionStatisticStorageFactory factory = new MysqlPartitionStatisticStorageFactory();

    Map<String, String> properties = new HashMap<>();
    properties.put("jdbc-user", "test_user");
    properties.put("jdbc-password", "test_password");
    // jdbc-url is missing

    Exception exception =
        assertThrows(IllegalArgumentException.class, () -> factory.create(properties));

    assertTrue(exception.getMessage().contains("jdbc-url"));
  }

  @Test
  public void testCreateWithMissingJdbcUser() {
    MysqlPartitionStatisticStorageFactory factory = new MysqlPartitionStatisticStorageFactory();

    Map<String, String> properties = new HashMap<>();
    properties.put("jdbc-url", "jdbc:mysql://localhost:3306/test_db");
    properties.put("jdbc-password", "test_password");
    // jdbc-user is missing

    Exception exception =
        assertThrows(IllegalArgumentException.class, () -> factory.create(properties));

    assertTrue(exception.getMessage().contains("jdbc-user"));
  }

  @Test
  public void testCreateWithMissingJdbcPassword() {
    MysqlPartitionStatisticStorageFactory factory = new MysqlPartitionStatisticStorageFactory();

    Map<String, String> properties = new HashMap<>();
    properties.put("jdbc-url", "jdbc:mysql://localhost:3306/test_db");
    properties.put("jdbc-user", "test_user");
    // jdbc-password is missing

    Exception exception =
        assertThrows(IllegalArgumentException.class, () -> factory.create(properties));

    assertTrue(exception.getMessage().contains("jdbc-password"));
  }

  @Test
  public void testCreateWithEmptyProperties() {
    MysqlPartitionStatisticStorageFactory factory = new MysqlPartitionStatisticStorageFactory();

    Map<String, String> properties = new HashMap<>();

    Exception exception =
        assertThrows(IllegalArgumentException.class, () -> factory.create(properties));

    assertNotNull(exception.getMessage());
  }

  @Test
  public void testCreateWithNullProperties() {
    MysqlPartitionStatisticStorageFactory factory = new MysqlPartitionStatisticStorageFactory();

    assertThrows(NullPointerException.class, () -> factory.create(null));
  }

  @Test
  public void testCreateWithAllProperties() {
    MysqlPartitionStatisticStorageFactory factory = new MysqlPartitionStatisticStorageFactory();

    Map<String, String> properties = new HashMap<>();
    properties.put("jdbc-url", "jdbc:mysql://localhost:3306/test_db?useSSL=false");
    properties.put("jdbc-user", "test_user");
    properties.put("jdbc-password", "test_password");
    properties.put("jdbc-driver", "com.mysql.cj.jdbc.Driver");
    properties.put("extra-property", "extra-value"); // Extra properties should be ignored

    PartitionStatisticStorage storage = factory.create(properties);

    assertNotNull(storage);
    assertTrue(storage instanceof MysqlPartitionStatisticStorage);
  }

  @Test
  public void testMultipleFactoryInstances() {
    // Test that multiple factory instances can be created independently
    MysqlPartitionStatisticStorageFactory factory1 = new MysqlPartitionStatisticStorageFactory();
    MysqlPartitionStatisticStorageFactory factory2 = new MysqlPartitionStatisticStorageFactory();

    Map<String, String> properties1 = new HashMap<>();
    properties1.put("jdbc-url", "jdbc:mysql://localhost:3306/db1");
    properties1.put("jdbc-user", "user1");
    properties1.put("jdbc-password", "pass1");

    Map<String, String> properties2 = new HashMap<>();
    properties2.put("jdbc-url", "jdbc:mysql://localhost:3306/db2");
    properties2.put("jdbc-user", "user2");
    properties2.put("jdbc-password", "pass2");

    PartitionStatisticStorage storage1 = factory1.create(properties1);
    PartitionStatisticStorage storage2 = factory2.create(properties2);

    assertNotNull(storage1);
    assertNotNull(storage2);
    assertTrue(storage1 instanceof MysqlPartitionStatisticStorage);
    assertTrue(storage2 instanceof MysqlPartitionStatisticStorage);
  }

  @Test
  public void testJdbcUrlWithParameters() {
    MysqlPartitionStatisticStorageFactory factory = new MysqlPartitionStatisticStorageFactory();

    Map<String, String> properties = new HashMap<>();
    properties.put(
        "jdbc-url",
        "jdbc:mysql://localhost:3306/test_db?useSSL=false&serverTimezone=UTC&allowPublicKeyRetrieval=true");
    properties.put("jdbc-user", "test_user");
    properties.put("jdbc-password", "test_password");

    PartitionStatisticStorage storage = factory.create(properties);

    assertNotNull(storage);
    assertTrue(storage instanceof MysqlPartitionStatisticStorage);
  }

  @Test
  public void testJdbcUrlWithIPv6() {
    MysqlPartitionStatisticStorageFactory factory = new MysqlPartitionStatisticStorageFactory();

    Map<String, String> properties = new HashMap<>();
    properties.put("jdbc-url", "jdbc:mysql://[::1]:3306/test_db");
    properties.put("jdbc-user", "test_user");
    properties.put("jdbc-password", "test_password");

    PartitionStatisticStorage storage = factory.create(properties);

    assertNotNull(storage);
    assertTrue(storage instanceof MysqlPartitionStatisticStorage);
  }

  @Test
  public void testEmptyPassword() {
    MysqlPartitionStatisticStorageFactory factory = new MysqlPartitionStatisticStorageFactory();

    Map<String, String> properties = new HashMap<>();
    properties.put("jdbc-url", "jdbc:mysql://localhost:3306/test_db");
    properties.put("jdbc-user", "test_user");
    properties.put("jdbc-password", ""); // Empty password

    PartitionStatisticStorage storage = factory.create(properties);

    assertNotNull(storage);
    assertTrue(storage instanceof MysqlPartitionStatisticStorage);
  }

  @Test
  public void testSpecialCharactersInCredentials() {
    MysqlPartitionStatisticStorageFactory factory = new MysqlPartitionStatisticStorageFactory();

    Map<String, String> properties = new HashMap<>();
    properties.put("jdbc-url", "jdbc:mysql://localhost:3306/test_db");
    properties.put("jdbc-user", "user@domain.com");
    properties.put("jdbc-password", "p@ssw0rd!#$%"); // Special characters

    PartitionStatisticStorage storage = factory.create(properties);

    assertNotNull(storage);
    assertTrue(storage instanceof MysqlPartitionStatisticStorage);
  }
}
