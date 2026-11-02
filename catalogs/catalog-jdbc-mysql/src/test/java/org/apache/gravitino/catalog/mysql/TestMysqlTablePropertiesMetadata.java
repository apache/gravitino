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
package org.apache.gravitino.catalog.mysql;

import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.StringIdentifier;
import org.apache.gravitino.catalog.PropertiesMetadataHelpers;
import org.apache.gravitino.catalog.jdbc.JdbcCatalogOperations;
import org.apache.gravitino.catalog.jdbc.JdbcColumn;
import org.apache.gravitino.catalog.jdbc.JdbcTable;
import org.apache.gravitino.catalog.jdbc.converter.JdbcExceptionConverter;
import org.apache.gravitino.catalog.mysql.converter.MysqlColumnDefaultValueConverter;
import org.apache.gravitino.catalog.mysql.converter.MysqlTypeConverter;
import org.apache.gravitino.catalog.mysql.operation.MysqlDatabaseOperations;
import org.apache.gravitino.catalog.mysql.operation.MysqlTableOperations;
import org.apache.gravitino.connector.CatalogInfo;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.TableChange;
import org.apache.gravitino.rel.expressions.distributions.Distribution;
import org.apache.gravitino.rel.expressions.distributions.Distributions;
import org.apache.gravitino.rel.expressions.sorts.SortOrder;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.expressions.transforms.Transforms;
import org.apache.gravitino.rel.indexes.Index;
import org.apache.gravitino.rel.indexes.Indexes;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

class TestMysqlTablePropertiesMetadata {
  private static final NameIdentifier IDENT =
      NameIdentifier.of("metalake", "catalog", "database", "table");

  @ParameterizedTest
  @ValueSource(strings = {"unknown-key", "ENGINE", "ROW_FORMAT", "engine; DROP TABLE t"})
  void testCatalogRejectsUnknownPropertiesBeforeCreateOrAlter(String key) {
    RecordingTableOperations table = new RecordingTableOperations();
    try (JdbcCatalogOperations operations = operations(table)) {
      IllegalArgumentException create =
          Assertions.assertThrows(
              IllegalArgumentException.class, () -> create(operations, Map.of(key, "MEMORY")));
      Assertions.assertTrue(create.getMessage().contains(key));
      Assertions.assertTrue(
          create.getMessage().contains("Supported properties: auto-increment-offset, engine"),
          create.getMessage());
      IllegalArgumentException alter =
          Assertions.assertThrows(
              IllegalArgumentException.class,
              () -> operations.alterTable(IDENT, TableChange.setProperty(key, "MEMORY")));
      Assertions.assertTrue(alter.getMessage().contains(key));
      IllegalArgumentException remove =
          Assertions.assertThrows(
              IllegalArgumentException.class,
              () -> operations.alterTable(IDENT, TableChange.removeProperty(key)));
      Assertions.assertTrue(remove.getMessage().contains(key));
      Assertions.assertEquals(0, table.writes);
    }
  }

  @ParameterizedTest
  @CsvSource({"engine,InnoDB; DROP TABLE t", "auto-increment-offset,42; DROP TABLE t"})
  void testInvalidDeclaredValuesAreRejectedBeforeCreate(String key, String value) {
    RecordingTableOperations table = new RecordingTableOperations();
    try (JdbcCatalogOperations operations = operations(table)) {
      IllegalArgumentException error =
          Assertions.assertThrows(
              IllegalArgumentException.class, () -> create(operations, Map.of(key, value)));
      Assertions.assertTrue(error.getMessage().contains(key));
      Assertions.assertEquals(0, table.writes);
    }
  }

  @Test
  void testValidPropertiesRoundTripAndMetadataDoesNotBecomeSql() {
    RecordingTableOperations table = new RecordingTableOperations();
    try (JdbcCatalogOperations operations = operations(table)) {
      Table created =
          create(
              operations,
              Map.of(
                  "engine",
                  "InNoDB",
                  "auto-increment-offset",
                  "+0042",
                  "credential-providers",
                  "test-provider"));
      Assertions.assertEquals(Map.of("ENGINE", "InnoDB", "AUTO_INCREMENT", "42"), table.properties);
      Assertions.assertEquals(
          Map.of("engine", "InnoDB", "auto-increment-offset", "42"), created.properties());
      Table loaded = operations.loadTable(IDENT);
      Assertions.assertEquals("InnoDB", loaded.properties().get("engine"));
      Assertions.assertEquals("42", loaded.properties().get("auto-increment-offset"));
      Assertions.assertEquals(1, table.writes);
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"engine", "auto-increment-offset"})
  void testDeclaredPropertiesRemainImmutable(String key) {
    IllegalArgumentException error =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                PropertiesMetadataHelpers.validatePropertyForAlter(
                    new MysqlTablePropertiesMetadata(), Map.of(key, "42"), Map.of()));
    Assertions.assertTrue(error.getMessage().contains("immutable"));
    Map<String, String> removal = new HashMap<>();
    removal.put(key, null);
    Assertions.assertTrue(
        new MysqlTablePropertiesMetadata().transformToJdbcProperties(removal).containsValue(null));
  }

  private static Table create(JdbcCatalogOperations operations, Map<String, String> properties) {
    return operations.createTable(
        IDENT,
        new Column[0],
        "comment",
        StringIdentifier.newPropertiesWithId(StringIdentifier.fromId(1L), properties),
        Transforms.EMPTY_TRANSFORM,
        Distributions.NONE,
        new SortOrder[0],
        Indexes.EMPTY_INDEXES);
  }

  private static JdbcCatalogOperations operations(RecordingTableOperations table) {
    JdbcCatalogOperations operations =
        new JdbcCatalogOperations(
            new JdbcExceptionConverter(),
            new MysqlTypeConverter(),
            new MysqlDatabaseOperations(),
            table,
            new MysqlColumnDefaultValueConverter());
    Map<String, String> conf =
        Map.of(
            "jdbc-url",
            "jdbc:mysql://127.0.0.1:1/unused",
            "jdbc-driver",
            "com.mysql.cj.jdbc.Driver",
            "jdbc-user",
            "test",
            "jdbc-password",
            "test");
    CatalogInfo info =
        new CatalogInfo(
            1L,
            "catalog",
            Catalog.Type.RELATIONAL,
            "jdbc-mysql",
            "test",
            conf,
            AuditInfo.builder().withCreator("test").withCreateTime(Instant.EPOCH).build(),
            Namespace.of("metalake"));
    operations.initialize(conf, info, new MysqlCatalog());
    return operations;
  }

  private static class RecordingTableOperations extends MysqlTableOperations {
    private int writes;
    private Map<String, String> properties = Collections.emptyMap();
    private String comment;

    /** Records a create after catalog property validation and translation. */
    @Override
    public void create(
        String databaseName,
        String tableName,
        JdbcColumn[] columns,
        String comment,
        Map<String, String> properties,
        Transform[] partitioning,
        Distribution distribution,
        Index[] indexes,
        SortOrder[] sortOrders) {
      writes++;
      this.properties = new HashMap<>(properties);
      this.comment = comment;
    }

    /** Records alterations reaching the table operation boundary. */
    @Override
    public void alterTable(String databaseName, String tableName, TableChange... changes) {
      writes++;
    }

    /** Returns the state recorded by create. */
    @Override
    public JdbcTable load(String databaseName, String tableName) {
      return JdbcTable.builder()
          .withName(tableName)
          .withColumns(new JdbcColumn[0])
          .withProperties(properties)
          .withComment(comment)
          .build();
    }
  }
}
