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

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.trino.connector.metadata.GravitinoCatalog;
import org.apache.gravitino.trino.connector.metadata.TestGravitinoCatalog;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestMySQLCatalogPropertyConverter {

  @Test
  @SuppressWarnings("unchecked")
  public void testBuildMySqlConnectorProperties() throws Exception {
    String name = "test_catalog";
    // trino.bypass properties will be skipped when the catalog properties is defined by Gravitino
    Map<String, String> properties =
        ImmutableMap.<String, String>builder()
            .put("trino.bypass.connection-user", "skip_value")
            .put("jdbc-url", "jdbc:mysql://localhost:5432/test")
            .put("jdbc-user", "test")
            .put("jdbc-password", "test")
            .put("trino.bypass.join-pushdown.strategy", "EAGER")
            .put("unknown-key", "1")
            .put("trino.bypass.mysql.unknown-key", "1")
            .put("trino.bypass.connection-url", "skip_value")
            .build();
    Catalog mockCatalog =
        TestGravitinoCatalog.mockCatalog(
            name, "jdbc-postgresql", "test catalog", Catalog.Type.RELATIONAL, properties);
    MySQLConnectorAdapter adapter = new MySQLConnectorAdapter();

    Map<String, String> config =
        adapter.buildInternalConnectorConfig(new GravitinoCatalog("test", mockCatalog));

    // test converted properties
    Assertions.assertEquals(config.get("connection-url"), "jdbc:mysql://localhost:5432/test");
    Assertions.assertEquals(config.get("connection-user"), "test");
    Assertions.assertEquals(config.get("connection-password"), "test");

    // test trino passing properties
    Assertions.assertEquals(config.get("join-pushdown.strategy"), "EAGER");

    // test unknown properties
    Assertions.assertNull(config.get("unknown-key"));
    Assertions.assertEquals(config.get("mysql.unknown-key"), "1");
  }
}
