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
package org.apache.gravitino.catalog.lakehouse.paimon.utils;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestPaimonJdbcTablePropertiesWorkaround {

  private static final String MYSQL_URI = "jdbc:mysql://127.0.0.1:3306/paimon_meta";

  @Test
  void testNeededOnlyForMySqlJdbcBackendWithSync() {
    Assertions.assertTrue(
        PaimonJdbcTablePropertiesWorkaround.isNeeded(
            ImmutableMap.of("metastore", "jdbc", "uri", MYSQL_URI)));
    Assertions.assertTrue(
        PaimonJdbcTablePropertiesWorkaround.isNeeded(
            ImmutableMap.of(
                "metastore", "JDBC", "uri", "JDBC:MYSQL://h/db", "sync-all-properties", "true")));

    // Sync disabled: Paimon does not use the table.
    Assertions.assertFalse(
        PaimonJdbcTablePropertiesWorkaround.isNeeded(
            ImmutableMap.of(
                "metastore", "jdbc", "uri", MYSQL_URI, "sync-all-properties", "false")));
    // Other databases do not have the key length limit.
    Assertions.assertFalse(
        PaimonJdbcTablePropertiesWorkaround.isNeeded(
            ImmutableMap.of("metastore", "jdbc", "uri", "jdbc:postgresql://127.0.0.1:5432/db")));
    // Other backends do not use the table.
    Assertions.assertFalse(
        PaimonJdbcTablePropertiesWorkaround.isNeeded(
            ImmutableMap.of("metastore", "hive", "uri", "thrift://127.0.0.1:9083")));
    Assertions.assertFalse(
        PaimonJdbcTablePropertiesWorkaround.isNeeded(ImmutableMap.of("metastore", "filesystem")));
    Map<String, String> noUri = ImmutableMap.of("metastore", "jdbc");
    Assertions.assertFalse(PaimonJdbcTablePropertiesWorkaround.isNeeded(noUri));
  }

  @Test
  void testKeyFitsMySqlLimit() {
    // utf8mb4 takes up to 4 bytes per character; InnoDB allows 3072 bytes per key.
    Assertions.assertEquals(3072, (64 + 255 + 255 + 194) * 4);
    String ddl = PaimonJdbcTablePropertiesWorkaround.CREATE_TABLE_PROPERTIES_TABLE;
    Assertions.assertTrue(ddl.contains("catalog_key VARCHAR(64) NOT NULL"));
    Assertions.assertTrue(ddl.contains("database_name VARCHAR(255) NOT NULL"));
    Assertions.assertTrue(ddl.contains("table_name VARCHAR(255) NOT NULL"));
    Assertions.assertTrue(ddl.contains("property_key VARCHAR(194) NOT NULL"));
  }
}
