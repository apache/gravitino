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
package org.apache.gravitino.catalog.postgresql;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.connector.CatalogOperations;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.CatalogEntity;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

/** Tests database resolution before catalog operations are initialized. */
class TestPostgreSqlCatalogConfiguration {

  @ParameterizedTest
  @CsvSource(
      value = {
        "jdbc:postgresql://localhost:5432/demo|demo",
        "jdbc:postgresql:demo|demo",
        "jdbc:postgresql://host1:5432,host2:5432/demo?ssl=true|demo",
        "jdbc:postgresql://localhost/my%20db|my db",
        "jdbc:postgresql://localhost/demo?currentSchema=other|demo",
        "jdbc:postgresql://localhost/demo?PGDBNAME=other|other"
      },
      delimiter = '|')
  void testDatabaseFromUrl(String url, String database) {
    Map<String, String> config = Map.of("jdbc-url", url);
    CapturingCatalog catalog = new CapturingCatalog();
    catalog.withCatalogConf(config);
    catalog.withCatalogEntity(
        CatalogEntity.builder()
            .withId(1L)
            .withName("test")
            .withNamespace(Namespace.of("metalake"))
            .withType(Catalog.Type.RELATIONAL)
            .withProvider(catalog.shortName())
            .withAuditInfo(
                AuditInfo.builder().withCreator("test").withCreateTime(Instant.EPOCH).build())
            .build());
    Assertions.assertThrows(UnsupportedOperationException.class, catalog::ops);
    Assertions.assertEquals(database, catalog.config.get("jdbc-database"));
    Assertions.assertEquals(url, catalog.config.get("jdbc-url"));
    Assertions.assertFalse(config.containsKey("jdbc-database"));
  }

  @ParameterizedTest
  @CsvSource(
      value = {
        "jdbc:postgresql://localhost:5432/demo|demo",
        "jdbc:postgresql://localhost/my%20db|my db",
        "jdbc:postgresql://localhost/|explicit"
      },
      delimiter = '|')
  void testExplicitDatabase(String url, String database) {
    CapturingCatalog catalog = new CapturingCatalog();
    catalog.withCatalogConf(Map.of("jdbc-url", url, "jdbc-database", database));
    catalog.withCatalogEntity(
        CatalogEntity.builder()
            .withId(1L)
            .withName("test")
            .withNamespace(Namespace.of("metalake"))
            .withType(Catalog.Type.RELATIONAL)
            .withProvider(catalog.shortName())
            .withAuditInfo(
                AuditInfo.builder().withCreator("test").withCreateTime(Instant.EPOCH).build())
            .build());
    Assertions.assertThrows(UnsupportedOperationException.class, catalog::ops);
    Assertions.assertEquals(database, catalog.config.get("jdbc-database"));
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "jdbc:postgresql://localhost/demo",
        "jdbc:postgresql://localhost/demo?PGDBNAME=other"
      })
  void testConflictingDatabaseRejected(String url) {
    IllegalArgumentException error =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                new PostgreSqlCatalog()
                    .withCatalogConf(Map.of("jdbc-url", url, "jdbc-database", "conflicting")));
    Assertions.assertEquals(
        "jdbc-database must match the database specified in jdbc-url", error.getMessage());
  }

  @ParameterizedTest
  @NullAndEmptySource
  @ValueSource(strings = {" ", "\t"})
  void testBlankExplicitDatabaseRejected(String database) {
    Map<String, String> config = new HashMap<>();
    config.put("jdbc-url", "jdbc:postgresql://localhost:5432/demo");
    config.put("jdbc-database", database);
    IllegalArgumentException error =
        Assertions.assertThrows(
            IllegalArgumentException.class, () -> new PostgreSqlCatalog().withCatalogConf(config));
    Assertions.assertTrue(error.getMessage().contains("jdbc-database"));
  }

  @Test
  void testMissingDatabaseRejectedBeforeFirstOperation() {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            new PostgreSqlCatalog()
                .withCatalogConf(Map.of("jdbc-url", "jdbc:postgresql://localhost/")));
  }

  @Test
  void testInvalidUrlRejected() {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> new PostgreSqlCatalog().withCatalogConf(Map.of("jdbc-url", "invalid")));
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> new PostgreSqlCatalog().withCatalogConf(Map.of()));
  }

  private static class CapturingCatalog extends PostgreSqlCatalog {
    private Map<String, String> config;

    /** {@inheritDoc} */
    @Override
    protected CatalogOperations newOps(Map<String, String> conf) {
      config = conf;
      throw new UnsupportedOperationException("Configuration captured without connecting");
    }
  }
}
