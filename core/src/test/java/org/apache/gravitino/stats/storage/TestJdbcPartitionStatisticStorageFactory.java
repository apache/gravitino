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

/** Unit tests for {@link JdbcPartitionStatisticStorageFactory}. */
public class TestJdbcPartitionStatisticStorageFactory {

  @Test
  public void testCreateWithValidConfiguration() {
    JdbcPartitionStatisticStorageFactory factory = new JdbcPartitionStatisticStorageFactory();

    Map<String, String> properties = new HashMap<>();
    properties.put("jdbcUrl", "jdbc:mysql://localhost:3306/test_db");
    properties.put("jdbcUser", "test_user");
    properties.put("jdbcPassword", "test_password");
    properties.put("jdbcDriver", "com.mysql.cj.jdbc.Driver");

    // This test only validates that the properties pass validation
    // Actual storage creation requires GravitinoEnv and is tested in integration tests
    try {
      PartitionStatisticStorage storage = factory.create(properties);
      assertNotNull(storage);
      assertTrue(storage instanceof JdbcPartitionStatisticStorage);
    } catch (Exception e) {
      // Expected if GravitinoEnv is not initialized
      if (e.getMessage() == null || !e.getMessage().contains("GravitinoEnv")) {
        Throwable cause = e.getCause();
        if (cause == null || !cause.getMessage().contains("GravitinoEnv")) {
          throw e;
        }
      }
    }
  }

  @Test
  public void testCreateWithDefaultDriver() {
    JdbcPartitionStatisticStorageFactory factory = new JdbcPartitionStatisticStorageFactory();

    Map<String, String> properties = new HashMap<>();
    properties.put("jdbcUrl", "jdbc:mysql://localhost:3306/test_db");
    properties.put("jdbcUser", "test_user");
    properties.put("jdbcPassword", "test_password");
    // jdbcDriver not provided, should use default

    // This test only validates that the properties pass validation
    try {
      PartitionStatisticStorage storage = factory.create(properties);
      assertNotNull(storage);
      assertTrue(storage instanceof JdbcPartitionStatisticStorage);
    } catch (Exception e) {
      if (e.getMessage() == null || !e.getMessage().contains("GravitinoEnv")) {
        Throwable cause = e.getCause();
        if (cause == null || !cause.getMessage().contains("GravitinoEnv")) {
          throw e;
        }
      }
    }
  }

  @Test
  public void testCreateWithMissingJdbcUrl() {
    JdbcPartitionStatisticStorageFactory factory = new JdbcPartitionStatisticStorageFactory();

    Map<String, String> properties = new HashMap<>();
    properties.put("jdbcUser", "test_user");
    properties.put("jdbcPassword", "test_password");
    // jdbcUrl is missing

    Exception exception =
        assertThrows(IllegalArgumentException.class, () -> factory.create(properties));

    assertTrue(exception.getMessage().contains("jdbcUrl"));
  }

  @Test
  public void testCreateWithMissingJdbcUser() {
    JdbcPartitionStatisticStorageFactory factory = new JdbcPartitionStatisticStorageFactory();

    Map<String, String> properties = new HashMap<>();
    properties.put("jdbcUrl", "jdbc:mysql://localhost:3306/test_db");
    properties.put("jdbcPassword", "test_password");
    // jdbcUser is missing

    Exception exception =
        assertThrows(IllegalArgumentException.class, () -> factory.create(properties));

    assertTrue(exception.getMessage().contains("jdbcUser"));
  }

  @Test
  public void testCreateWithMissingJdbcPassword() {
    JdbcPartitionStatisticStorageFactory factory = new JdbcPartitionStatisticStorageFactory();

    Map<String, String> properties = new HashMap<>();
    properties.put("jdbcUrl", "jdbc:mysql://localhost:3306/test_db");
    properties.put("jdbcUser", "test_user");
    // jdbcPassword is missing

    Exception exception =
        assertThrows(IllegalArgumentException.class, () -> factory.create(properties));

    assertTrue(exception.getMessage().contains("jdbcPassword"));
  }

  @Test
  public void testCreateWithEmptyProperties() {
    JdbcPartitionStatisticStorageFactory factory = new JdbcPartitionStatisticStorageFactory();

    Map<String, String> properties = new HashMap<>();

    Exception exception =
        assertThrows(IllegalArgumentException.class, () -> factory.create(properties));

    assertNotNull(exception.getMessage());
  }

  @Test
  public void testCreateWithNullProperties() {
    JdbcPartitionStatisticStorageFactory factory = new JdbcPartitionStatisticStorageFactory();

    assertThrows(NullPointerException.class, () -> factory.create(null));
  }

  @Test
  public void testCreateWithAllProperties() {
    JdbcPartitionStatisticStorageFactory factory = new JdbcPartitionStatisticStorageFactory();

    Map<String, String> properties = new HashMap<>();
    properties.put("jdbcUrl", "jdbc:mysql://localhost:3306/test_db?useSSL=false");
    properties.put("jdbcUser", "test_user");
    properties.put("jdbcPassword", "test_password");
    properties.put("jdbcDriver", "com.mysql.cj.jdbc.Driver");
    properties.put("extra-property", "extra-value"); // Extra properties should be ignored

    try {
      PartitionStatisticStorage storage = factory.create(properties);
      assertNotNull(storage);
      assertTrue(storage instanceof JdbcPartitionStatisticStorage);
    } catch (Exception e) {
      if (e.getMessage() == null || !e.getMessage().contains("GravitinoEnv")) {
        Throwable cause = e.getCause();
        if (cause == null || !cause.getMessage().contains("GravitinoEnv")) {
          throw e;
        }
      }
    }
  }

  @Test
  public void testMultipleFactoryInstances() {
    // Test that multiple factory instances can be created independently
    JdbcPartitionStatisticStorageFactory factory1 = new JdbcPartitionStatisticStorageFactory();
    JdbcPartitionStatisticStorageFactory factory2 = new JdbcPartitionStatisticStorageFactory();

    Map<String, String> properties1 = new HashMap<>();
    properties1.put("jdbcUrl", "jdbc:mysql://localhost:3306/db1");
    properties1.put("jdbcUser", "user1");
    properties1.put("jdbcPassword", "pass1");

    Map<String, String> properties2 = new HashMap<>();
    properties2.put("jdbcUrl", "jdbc:mysql://localhost:3306/db2");
    properties2.put("jdbcUser", "user2");
    properties2.put("jdbcPassword", "pass2");

    try {
      PartitionStatisticStorage storage1 = factory1.create(properties1);
      PartitionStatisticStorage storage2 = factory2.create(properties2);
      assertNotNull(storage1);
      assertNotNull(storage2);
      assertTrue(storage1 instanceof JdbcPartitionStatisticStorage);
      assertTrue(storage2 instanceof JdbcPartitionStatisticStorage);
    } catch (Exception e) {
      if (e.getMessage() == null || !e.getMessage().contains("GravitinoEnv")) {
        Throwable cause = e.getCause();
        if (cause == null || !cause.getMessage().contains("GravitinoEnv")) {
          throw e;
        }
      }
    }
  }

  @Test
  public void testJdbcUrlWithParameters() {
    JdbcPartitionStatisticStorageFactory factory = new JdbcPartitionStatisticStorageFactory();

    Map<String, String> properties = new HashMap<>();
    properties.put(
        "jdbcUrl",
        "jdbc:mysql://localhost:3306/test_db?useSSL=false&serverTimezone=UTC&allowPublicKeyRetrieval=true");
    properties.put("jdbcUser", "test_user");
    properties.put("jdbcPassword", "test_password");

    try {
      PartitionStatisticStorage storage = factory.create(properties);
      assertNotNull(storage);
      assertTrue(storage instanceof JdbcPartitionStatisticStorage);
    } catch (Exception e) {
      if (e.getMessage() == null || !e.getMessage().contains("GravitinoEnv")) {
        Throwable cause = e.getCause();
        if (cause == null || !cause.getMessage().contains("GravitinoEnv")) {
          throw e;
        }
      }
    }
  }

  @Test
  public void testJdbcUrlWithIPv6() {
    JdbcPartitionStatisticStorageFactory factory = new JdbcPartitionStatisticStorageFactory();

    Map<String, String> properties = new HashMap<>();
    properties.put("jdbcUrl", "jdbc:mysql://[::1]:3306/test_db");
    properties.put("jdbcUser", "test_user");
    properties.put("jdbcPassword", "test_password");

    try {
      PartitionStatisticStorage storage = factory.create(properties);
      assertNotNull(storage);
      assertTrue(storage instanceof JdbcPartitionStatisticStorage);
    } catch (Exception e) {
      if (e.getMessage() == null || !e.getMessage().contains("GravitinoEnv")) {
        Throwable cause = e.getCause();
        if (cause == null || !cause.getMessage().contains("GravitinoEnv")) {
          throw e;
        }
      }
    }
  }

  @Test
  public void testEmptyPassword() {
    JdbcPartitionStatisticStorageFactory factory = new JdbcPartitionStatisticStorageFactory();

    Map<String, String> properties = new HashMap<>();
    properties.put("jdbcUrl", "jdbc:mysql://localhost:3306/test_db");
    properties.put("jdbcUser", "test_user");
    properties.put("jdbcPassword", ""); // Empty password

    Exception exception =
        assertThrows(IllegalArgumentException.class, () -> factory.create(properties));

    assertTrue(exception.getMessage().contains("jdbcPassword"));
  }

  @Test
  public void testSpecialCharactersInCredentials() {
    JdbcPartitionStatisticStorageFactory factory = new JdbcPartitionStatisticStorageFactory();

    Map<String, String> properties = new HashMap<>();
    properties.put("jdbcUrl", "jdbc:mysql://localhost:3306/test_db");
    properties.put("jdbcUser", "user@domain.com");
    properties.put("jdbcPassword", "p@ssw0rd!#$%"); // Special characters

    try {
      PartitionStatisticStorage storage = factory.create(properties);
      assertNotNull(storage);
      assertTrue(storage instanceof JdbcPartitionStatisticStorage);
    } catch (Exception e) {
      if (e.getMessage() == null || !e.getMessage().contains("GravitinoEnv")) {
        Throwable cause = e.getCause();
        if (cause == null || !cause.getMessage().contains("GravitinoEnv")) {
          throw e;
        }
      }
    }
  }
}
