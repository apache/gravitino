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

package org.apache.gravitino.idp.storage.mapper;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

public class TestIdpBaseSQLProviderFactory {

  @Test
  void testProviderAccessors() {
    TestFactory factory = new TestFactory();

    assertSame("mysql", factory.mysqlProviderValue());
    assertSame("h2", factory.h2ProviderValue());
    assertSame("postgresql", factory.postgresqlProviderValue());
  }

  @Test
  void testGetProvider() {
    TestFactory factory = new TestFactory();

    assertEquals("mysql", factory.provider("mysql"));
    assertEquals("h2", factory.provider("h2"));
    assertEquals("postgresql", factory.provider("postgresql"));
  }

  @Test
  void testGetProviderWithNullDatabaseId() {
    TestFactory factory = new TestFactory();

    IllegalStateException exception =
        assertThrows(IllegalStateException.class, () -> factory.provider(null));
    assertEquals(
        "MyBatis databaseId is not configured for test SQL provider.", exception.getMessage());
  }

  @Test
  void testGetProviderWithUnsupportedDatabaseId() {
    TestFactory factory = new TestFactory();

    IllegalStateException exception =
        assertThrows(IllegalStateException.class, () -> factory.provider("sqlite"));
    assertEquals(
        "Unsupported test SQL provider databaseId: sqlite, supported backends: [MYSQL, H2,"
            + " POSTGRESQL]",
        exception.getMessage());
  }

  private static class TestFactory extends IdpBaseSQLProviderFactory<String> {
    private TestFactory() {
      super("test SQL provider", "mysql", "h2", "postgresql");
    }

    private String provider(String databaseId) {
      return resolveProvider(databaseId);
    }

    private String mysqlProviderValue() {
      return mysqlProviderInstance();
    }

    private String h2ProviderValue() {
      return h2ProviderInstance();
    }

    private String postgresqlProviderValue() {
      return postgresqlProviderInstance();
    }
  }
}
