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
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.gravitino.storage.relational.JDBCBackend.JDBCBackendType;
import org.junit.jupiter.api.Test;

public class TestSQLProviderFactoryHelper {
  private static final Map<JDBCBackendType, String> PROVIDER_MAP =
      ImmutableMap.of(
          JDBCBackendType.MYSQL, "mysql",
          JDBCBackendType.H2, "h2",
          JDBCBackendType.POSTGRESQL, "postgresql");

  @Test
  void testGetProvider() {
    assertEquals("mysql", provider("mysql"));
    assertEquals("h2", provider("h2"));
    assertEquals("postgresql", provider("postgresql"));
  }

  @Test
  void testGetProviderWithNullDatabaseId() {
    IllegalStateException exception =
        assertThrows(IllegalStateException.class, () -> provider(null));
    assertEquals(
        "MyBatis databaseId is not configured for TestSQLProviderFactoryHelper.",
        exception.getMessage());
  }

  @Test
  void testGetProviderWithUnsupportedDatabaseId() {
    IllegalStateException exception =
        assertThrows(IllegalStateException.class, () -> provider("sqlite"));
    assertEquals(
        "Unsupported TestSQLProviderFactoryHelper databaseId: sqlite, supported backends: [MYSQL,"
            + " H2,"
            + " POSTGRESQL]",
        exception.getMessage());
  }

  private String provider(String databaseId) {
    return SQLProviderFactoryHelper.getProvider(
        databaseId, PROVIDER_MAP, TestSQLProviderFactoryHelper.class);
  }
}
