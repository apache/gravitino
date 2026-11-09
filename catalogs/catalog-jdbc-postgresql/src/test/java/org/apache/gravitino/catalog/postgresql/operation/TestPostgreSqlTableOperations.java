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
package org.apache.gravitino.catalog.postgresql.operation;

import com.google.common.collect.Maps;
import com.google.common.io.BaseEncoding;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.sql.DataSource;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.catalog.jdbc.JdbcColumn;
import org.apache.gravitino.catalog.jdbc.JdbcTable;
import org.apache.gravitino.catalog.jdbc.config.JdbcConfig;
import org.apache.gravitino.catalog.jdbc.utils.DataSourceUtils;
import org.apache.gravitino.catalog.jdbc.utils.JdbcConnectorUtils;
import org.apache.gravitino.catalog.postgresql.converter.PostgreSqlColumnDefaultValueConverter;
import org.apache.gravitino.catalog.postgresql.converter.PostgreSqlExceptionConverter;
import org.apache.gravitino.catalog.postgresql.converter.PostgreSqlTypeConverter;
import org.apache.gravitino.dto.rel.expressions.LiteralDTO;
import org.apache.gravitino.dto.requests.TableUpdateRequest;
import org.apache.gravitino.dto.util.DTOConverters;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.exceptions.GravitinoRuntimeException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.NoSuchTableException;
import org.apache.gravitino.exceptions.TableAlreadyExistsException;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.TableChange;
import org.apache.gravitino.rel.expressions.FunctionExpression;
import org.apache.gravitino.rel.expressions.UnparsedExpression;
import org.apache.gravitino.rel.expressions.distributions.Distributions;
import org.apache.gravitino.rel.expressions.literals.Literals;
import org.apache.gravitino.rel.expressions.transforms.Transforms;
import org.apache.gravitino.rel.indexes.Index;
import org.apache.gravitino.rel.indexes.Index.IndexType;
import org.apache.gravitino.rel.indexes.Indexes;
import org.apache.gravitino.rel.types.Type;
import org.apache.gravitino.rel.types.Types;
import org.apache.gravitino.utils.RandomNameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.AdditionalAnswers;
import org.mockito.Mockito;
import org.postgresql.PGConnection;

@Tag("gravitino-docker-test")
public class TestPostgreSqlTableOperations extends TestPostgreSql {

  private static Type VARCHAR = Types.VarCharType.of(255);
  private static Type INT = Types.IntegerType.get();

  @ParameterizedTest
  @ValueSource(strings = {"on", "off"})
  void testBackslashLiteralRoundTripsUnderReadSetting(String setting) throws Exception {
    DataSource source = Mockito.mock(DataSource.class);
    Mockito.when(source.getConnection())
        .thenAnswer(
            invocation -> {
              Connection c =
                  DriverManager.getConnection(
                      CONTAINER.getJdbcUrl(), CONTAINER.getUsername(), CONTAINER.getPassword());
              try (Statement s = c.createStatement()) {
                s.execute("SET standard_conforming_strings = " + setting);
              }
              return c;
            });
    PostgreSqlTableOperations ops = new PostgreSqlTableOperations();
    ops.initialize(
        source,
        new PostgreSqlExceptionConverter(),
        new PostgreSqlTypeConverter(),
        new PostgreSqlColumnDefaultValueConverter(),
        Map.of(JdbcConfig.JDBC_DATABASE.getKey(), TEST_DB_NAME));
    String table = RandomNameUtils.genRandomName("backslash_default_");
    String[] values = {"a\\b", "a\\\\b", "quote' \\path\nnext", "NULL::text"};
    for (String value : values) {
      try {
        ops.create(
            TEST_DB_NAME,
            table,
            new JdbcColumn[] {
              JdbcColumn.builder().withName("first").withType(VARCHAR).withNullable(true).build(),
              JdbcColumn.builder()
                  .withName("s")
                  .withType(Types.StringType.get())
                  .withNullable(true)
                  .withDefaultValue(Literals.stringLiteral(value))
                  .build(),
              JdbcColumn.builder()
                  .withName("another")
                  .withType(Types.StringType.get())
                  .withNullable(true)
                  .withDefaultValue(Literals.stringLiteral(value))
                  .build()
            },
            null,
            Map.of(),
            null,
            Distributions.NONE,
            Indexes.EMPTY_INDEXES);
        try (Connection c = source.getConnection();
            Statement s = c.createStatement()) {
          s.executeUpdate("INSERT INTO " + TEST_DB_NAME + "." + table + " DEFAULT VALUES");
          try (ResultSet row = s.executeQuery("SELECT s FROM " + TEST_DB_NAME + "." + table)) {
            Assertions.assertTrue(row.next());
            Assertions.assertEquals(value, row.getString(1));
          }
          try (ResultSet metadata =
              c.getMetaData().getColumns(c.getCatalog(), TEST_DB_NAME, table, "s")) {
            Assertions.assertTrue(metadata.next());
            String raw = metadata.getString("COLUMN_DEF");
            Assertions.assertFalse(raw.startsWith("E'"));
          }
        }
        JdbcTable loaded = ops.load(TEST_DB_NAME, table);
        Assertions.assertEquals(Literals.stringLiteral(value), loaded.columns()[1].defaultValue());
        Assertions.assertEquals(Literals.stringLiteral(value), loaded.columns()[2].defaultValue());
        try (Connection c = source.getConnection()) {
          PGConnection pg =
              Mockito.mock(
                  PGConnection.class, AdditionalAnswers.delegatesTo(c.unwrap(PGConnection.class)));
          Connection wrapped = Mockito.mock(Connection.class, AdditionalAnswers.delegatesTo(c));
          Mockito.doReturn(pg).when(wrapped).unwrap(PGConnection.class);
          try (ResultSet columns = ops.getColumns(wrapped, TEST_DB_NAME, table)) {
            Assertions.assertTrue(columns.next());
          }
          Mockito.verify(pg, Mockito.times(value.indexOf('\\') >= 0 ? 1 : 0)).escapeLiteral("\\");
          try (Statement statement = c.createStatement();
              ResultSet result = statement.executeQuery("SHOW standard_conforming_strings")) {
            Assertions.assertTrue(result.next());
            Assertions.assertEquals(setting, result.getString(1));
          }
        }
      } finally {
        ops.drop(TEST_DB_NAME, table);
      }
    }
  }

  @Test
  void testFunctionDefaultsRemainExpressionsAfterCreateAndAlter() throws SQLException {
    String table = RandomNameUtils.genRandomName("function_read_");
    try {
      TABLE_OPERATIONS.create(
          TEST_DB_NAME,
          table,
          new JdbcColumn[] {
            JdbcColumn.builder()
                .withName("s")
                .withType(Types.StringType.get())
                .withNullable(true)
                .withDefaultValue(FunctionExpression.of("upper", Literals.stringLiteral("x")))
                .build()
          },
          null,
          Map.of(),
          null,
          Distributions.NONE,
          Indexes.EMPTY_INDEXES);
      Assertions.assertEquals(
          UnparsedExpression.of("upper('x'::text)"),
          TABLE_OPERATIONS.load(TEST_DB_NAME, table).columns()[0].defaultValue());
      TABLE_OPERATIONS.alterTable(
          TEST_DB_NAME,
          table,
          TableChange.addColumn(
              new String[] {"added"},
              Types.StringType.get(),
              null,
              TableChange.ColumnPosition.defaultPos(),
              true,
              false,
              FunctionExpression.of("lower", Literals.stringLiteral("Y"))));
      Assertions.assertEquals(
          UnparsedExpression.of("lower('Y'::text)"),
          TABLE_OPERATIONS.load(TEST_DB_NAME, table).columns()[1].defaultValue());
      TABLE_OPERATIONS.alterTable(
          TEST_DB_NAME,
          table,
          TableChange.updateColumnDefaultValue(
              new String[] {"added"}, FunctionExpression.of("upper", Literals.stringLiteral("z"))));
      Assertions.assertEquals(
          UnparsedExpression.of("upper('z'::text)"),
          TABLE_OPERATIONS.load(TEST_DB_NAME, table).columns()[1].defaultValue());
      try (Connection connection = DATA_SOURCE.getConnection();
          Statement statement = connection.createStatement()) {
        statement.executeUpdate("INSERT INTO " + TEST_DB_NAME + "." + table + " DEFAULT VALUES");
        try (ResultSet result =
            statement.executeQuery("SELECT s,added FROM " + TEST_DB_NAME + "." + table)) {
          Assertions.assertTrue(result.next());
          Assertions.assertEquals("X", result.getString(1));
          Assertions.assertEquals("Z", result.getString(2));
        }
      }
    } finally {
      TABLE_OPERATIONS.drop(TEST_DB_NAME, table);
    }
  }

  @Test
  void testStandaloneUniqueIndexDeletionIsExplicitlyRejected() throws SQLException {
    String table = RandomNameUtils.genRandomName("standalone_index_");
    String index = "native\" unique; --";
    TABLE_OPERATIONS.create(
        TEST_DB_NAME,
        table,
        new JdbcColumn[] {
          JdbcColumn.builder().withName("id").withType(INT).withNullable(true).build()
        },
        "before",
        Map.of(),
        null,
        Distributions.NONE,
        Indexes.EMPTY_INDEXES);
    try (Connection connection = DATA_SOURCE.getConnection();
        Statement statement = connection.createStatement()) {
      statement.executeUpdate(
          "CREATE UNIQUE INDEX \""
              + index.replace("\"", "\"\"")
              + "\" ON "
              + TEST_DB_NAME
              + "."
              + table
              + "(id)");
      for (boolean ifExists : new boolean[] {false, true}) {
        IllegalArgumentException error =
            Assertions.assertThrows(
                IllegalArgumentException.class,
                () ->
                    TABLE_OPERATIONS.alterTable(
                        TEST_DB_NAME,
                        table,
                        TableChange.updateComment("must not be applied"),
                        TableChange.deleteIndex(index, ifExists)));
        Assertions.assertTrue(error.getMessage().contains("standalone"));
        JdbcTable loaded = TABLE_OPERATIONS.load(TEST_DB_NAME, table);
        Assertions.assertEquals("before", loaded.comment());
        Assertions.assertTrue(Arrays.stream(loaded.index()).anyMatch(i -> index.equals(i.name())));
      }
    } finally {
      TABLE_OPERATIONS.drop(TEST_DB_NAME, table);
    }
  }

  @Test
  void testRestNullDefaultsAreSqlNullOnAlter() throws SQLException {
    String table = RandomNameUtils.genRandomName("null_default_");
    TABLE_OPERATIONS.create(
        TEST_DB_NAME,
        table,
        new JdbcColumn[] {
          JdbcColumn.builder()
              .withName("s")
              .withType(VARCHAR)
              .withNullable(true)
              .withDefaultValue(Literals.stringLiteral("before"))
              .build(),
          JdbcColumn.builder()
              .withName("i")
              .withType(INT)
              .withNullable(true)
              .withDefaultValue(Literals.integerLiteral(42))
              .build()
        },
        null,
        Map.of(),
        null,
        Distributions.NONE,
        Indexes.EMPTY_INDEXES);
    try (Connection connection = DATA_SOURCE.getConnection();
        Statement statement = connection.createStatement()) {
      TableUpdateRequest update =
          new TableUpdateRequest.UpdateTableColumnDefaultValueRequest(
              new String[] {"s"}, LiteralDTO.NULL);
      TableUpdateRequest typedNull =
          new TableUpdateRequest.UpdateTableColumnDefaultValueRequest(
              new String[] {"i"}, LiteralDTO.builder().withDataType(INT).withValue(null).build());
      TableUpdateRequest add =
          new TableUpdateRequest.AddTableColumnRequest(
              new String[] {"added"}, VARCHAR, null, null, true, false, LiteralDTO.NULL);
      TABLE_OPERATIONS.alterTable(
          TEST_DB_NAME, table, update.tableChange(), typedNull.tableChange(), add.tableChange());
      statement.executeUpdate("INSERT INTO " + TEST_DB_NAME + "." + table + " DEFAULT VALUES");
      try (ResultSet result =
          statement.executeQuery("SELECT s,i,added FROM " + TEST_DB_NAME + "." + table)) {
        Assertions.assertTrue(result.next());
        Assertions.assertNull(result.getObject(1));
        Assertions.assertNull(result.getObject(2));
        Assertions.assertNull(result.getObject(3));
      }
      for (Column column : TABLE_OPERATIONS.load(TEST_DB_NAME, table).columns())
        Assertions.assertEquals(Literals.NULL, column.defaultValue());
    } finally {
      TABLE_OPERATIONS.drop(TEST_DB_NAME, table);
    }
  }

  @Test
  void testDdlFragmentsAreRejectedOnCreateAndAlter() {
    String table = RandomNameUtils.genRandomName("ddl_guard_");
    JdbcColumn id = JdbcColumn.builder().withName("id").withType(INT).withNullable(true).build();
    TABLE_OPERATIONS.create(
        TEST_DB_NAME,
        table,
        new JdbcColumn[] {id},
        null,
        Map.of(),
        null,
        Distributions.NONE,
        Indexes.EMPTY_INDEXES);
    try {
      for (String fragment :
          new String[] {
            "integer, injected_col integer",
            "integer DEFAULT 42",
            "integer); CREATE TABLE injected_marker(id int); CREATE TABLE continuation(id int"
          }) {
        JdbcColumn bad =
            JdbcColumn.builder()
                .withName("id")
                .withType(Types.ExternalType.of(fragment))
                .withNullable(true)
                .build();
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                TABLE_OPERATIONS.create(
                    TEST_DB_NAME,
                    table + "_bad",
                    new JdbcColumn[] {bad},
                    null,
                    Map.of(),
                    null,
                    Distributions.NONE,
                    Indexes.EMPTY_INDEXES));
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                TABLE_OPERATIONS.alterTable(
                    TEST_DB_NAME,
                    table,
                    TableChange.addColumn(new String[] {"bad"}, bad.dataType())));
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                TABLE_OPERATIONS.alterTable(
                    TEST_DB_NAME,
                    table,
                    TableChange.updateColumnType(new String[] {"id"}, bad.dataType())));
      }
      FunctionExpression badDefault =
          FunctionExpression.of(
              "abs(1))); CREATE TABLE injected_marker(id int); CREATE TABLE continuation(id timestamptz DEFAULT (now");
      JdbcColumn bad =
          JdbcColumn.builder()
              .withName("id")
              .withType(INT)
              .withDefaultValue(badDefault)
              .withNullable(true)
              .build();
      Assertions.assertThrows(
          IllegalArgumentException.class,
          () ->
              TABLE_OPERATIONS.create(
                  TEST_DB_NAME,
                  table + "_bad",
                  new JdbcColumn[] {bad},
                  null,
                  Map.of(),
                  null,
                  Distributions.NONE,
                  Indexes.EMPTY_INDEXES));
      Assertions.assertThrows(
          IllegalArgumentException.class,
          () ->
              TABLE_OPERATIONS.alterTable(
                  TEST_DB_NAME,
                  table,
                  TableChange.updateColumnDefaultValue(new String[] {"id"}, badDefault)));
      Assertions.assertThrows(
          IllegalArgumentException.class,
          () ->
              TABLE_OPERATIONS.alterTable(
                  TEST_DB_NAME,
                  table,
                  TableChange.addColumn(
                      new String[] {"bad"},
                      INT,
                      null,
                      TableChange.ColumnPosition.defaultPos(),
                      true,
                      false,
                      badDefault)));
      List<String> names = TABLE_OPERATIONS.listTables(TEST_DB_NAME);
      Assertions.assertFalse(names.contains("injected_marker"));
      Assertions.assertFalse(names.contains("continuation"));
      Assertions.assertFalse(names.contains(table + "_bad"));
      Assertions.assertEquals(1, TABLE_OPERATIONS.load(TEST_DB_NAME, table).columns().length);
    } finally {
      TABLE_OPERATIONS.drop(TEST_DB_NAME, table);
      TABLE_OPERATIONS.drop(TEST_DB_NAME, table + "_bad");
      TABLE_OPERATIONS.drop(TEST_DB_NAME, "injected_marker");
      TABLE_OPERATIONS.drop(TEST_DB_NAME, "continuation");
    }
  }

  @Test
  void testQuotedNativeTypesAndModifiersStillExecute() throws SQLException {
    String table = RandomNameUtils.genRandomName("native_type_");
    String domain = RandomNameUtils.genRandomName("domain_") + ";quoted";
    String type = "\"" + TEST_DB_NAME + "\".\"" + domain + "\"";
    try (Connection connection = DATA_SOURCE.getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DOMAIN " + type + " AS integer");
      try {
        TABLE_OPERATIONS.create(
            TEST_DB_NAME,
            table,
            new JdbcColumn[] {
              JdbcColumn.builder()
                  .withName("id")
                  .withType(Types.ExternalType.of(type))
                  .withNullable(true)
                  .build(),
              JdbcColumn.builder()
                  .withName("n")
                  .withType(Types.ExternalType.of("numeric(39,0) ARRAY"))
                  .withNullable(true)
                  .build(),
              JdbcColumn.builder()
                  .withName("iv")
                  .withType(Types.ExternalType.of("interval day to second(6)"))
                  .withNullable(true)
                  .build(),
              JdbcColumn.builder()
                  .withName("national")
                  .withType(Types.ExternalType.of("national character(3)"))
                  .withNullable(true)
                  .build()
            },
            null,
            Map.of(),
            null,
            Distributions.NONE,
            Indexes.EMPTY_INDEXES);
        statement.executeUpdate(
            "INSERT INTO "
                + TEST_DB_NAME
                + "."
                + table
                + " VALUES(7, ARRAY[123456789012345678901234567890123456789], INTERVAL '1 day', 'abc')");
        try (ResultSet result =
            statement.executeQuery("SELECT id, n[1]::text FROM " + TEST_DB_NAME + "." + table)) {
          Assertions.assertTrue(result.next());
          Assertions.assertEquals(7, result.getInt(1));
          Assertions.assertEquals("123456789012345678901234567890123456789", result.getString(2));
        }
        Assertions.assertEquals(4, TABLE_OPERATIONS.load(TEST_DB_NAME, table).columns().length);
      } finally {
        TABLE_OPERATIONS.drop(TEST_DB_NAME, table);
        statement.execute("DROP DOMAIN " + type);
      }
    }
  }

  @Test
  void testFunctionDefaultsWithArgumentsOnCreateAndAlter() throws SQLException {
    String table = RandomNameUtils.genRandomName("function_default_");
    String value = "O'Brien; CREATE TABLE injected_marker(id int); --";
    JdbcColumn[] columns = {
      JdbcColumn.builder()
          .withName("n")
          .withType(INT)
          .withNullable(true)
          .withDefaultValue(FunctionExpression.of("pg_catalog.abs", Literals.integerLiteral(-3)))
          .build(),
      JdbcColumn.builder()
          .withName("s")
          .withType(VARCHAR)
          .withNullable(true)
          .withDefaultValue(
              FunctionExpression.of(
                  "concat", Literals.stringLiteral(value), Literals.stringLiteral("!")))
          .build(),
      JdbcColumn.builder()
          .withName("ts")
          .withType(Types.TimestampType.withoutTimeZone())
          .withNullable(true)
          .withDefaultValue(
              FunctionExpression.of(
                  "date_trunc", Literals.stringLiteral("day"), FunctionExpression.of("now")))
          .build()
    };
    TABLE_OPERATIONS.create(
        TEST_DB_NAME,
        table,
        columns,
        null,
        Map.of(),
        null,
        Distributions.NONE,
        Indexes.EMPTY_INDEXES);
    try (Connection connection = DATA_SOURCE.getConnection();
        Statement statement = connection.createStatement()) {
      statement.executeUpdate("INSERT INTO " + TEST_DB_NAME + "." + table + " DEFAULT VALUES");
      try (ResultSet result =
          statement.executeQuery("SELECT n,s,ts FROM " + TEST_DB_NAME + "." + table)) {
        Assertions.assertTrue(result.next());
        Assertions.assertEquals(3, result.getInt(1));
        Assertions.assertEquals(value + "!", result.getString(2));
        Assertions.assertEquals(
            LocalTime.MIDNIGHT, result.getTimestamp(3).toLocalDateTime().toLocalTime());
      }
      Assertions.assertInstanceOf(
          UnparsedExpression.class,
          TABLE_OPERATIONS.load(TEST_DB_NAME, table).columns()[1].defaultValue());
      TABLE_OPERATIONS.alterTable(
          TEST_DB_NAME,
          table,
          TableChange.addColumn(
              new String[] {"added"},
              INT,
              null,
              TableChange.ColumnPosition.defaultPos(),
              true,
              false,
              FunctionExpression.of("abs", Literals.integerLiteral(-5))));
      TABLE_OPERATIONS.alterTable(
          TEST_DB_NAME,
          table,
          TableChange.updateColumnDefaultValue(
              new String[] {"added"}, FunctionExpression.of("abs", Literals.integerLiteral(-9))));
      statement.executeUpdate("INSERT INTO " + TEST_DB_NAME + "." + table + " DEFAULT VALUES");
      try (ResultSet result =
          statement.executeQuery(
              "SELECT added FROM " + TEST_DB_NAME + "." + table + " ORDER BY added")) {
        Assertions.assertTrue(result.next());
        Assertions.assertEquals(5, result.getInt(1));
        Assertions.assertTrue(result.next());
        Assertions.assertEquals(9, result.getInt(1));
      }
      Assertions.assertFalse(TABLE_OPERATIONS.listTables(TEST_DB_NAME).contains("injected_marker"));
    } finally {
      TABLE_OPERATIONS.drop(TEST_DB_NAME, table);
    }
  }

  @Test
  void testSqlTokensInIndexNamesStayIdentifiers() throws SQLException {
    String table = RandomNameUtils.genRandomName("index_tokens_");
    String primary = "pk\"; CREATE TABLE marker(id int);--";
    String unique = "uq\"; CREATE TABLE marker(id int);--";
    TABLE_OPERATIONS.create(
        TEST_DB_NAME,
        table,
        new JdbcColumn[] {
          JdbcColumn.builder().withName("id").withType(INT).withNullable(false).build()
        },
        null,
        Map.of(),
        null,
        Distributions.NONE,
        new Index[] {Indexes.primary(primary, new String[][] {{"id"}})});
    try {
      Assertions.assertTrue(
          Arrays.stream(TABLE_OPERATIONS.load(TEST_DB_NAME, table).index())
              .anyMatch(i -> primary.equals(i.name())));
      TABLE_OPERATIONS.alterTable(
          TEST_DB_NAME,
          table,
          TableChange.addIndex(IndexType.UNIQUE_KEY, unique, new String[][] {{"id"}}));
      Assertions.assertTrue(
          Arrays.stream(TABLE_OPERATIONS.load(TEST_DB_NAME, table).index())
              .anyMatch(i -> unique.equals(i.name())));
      TABLE_OPERATIONS.alterTable(
          TEST_DB_NAME,
          table,
          TableChange.deleteIndex(unique, false),
          TableChange.deleteIndex(primary, false));
      Assertions.assertEquals(0, TABLE_OPERATIONS.load(TEST_DB_NAME, table).index().length);
      Assertions.assertFalse(TABLE_OPERATIONS.listTables(TEST_DB_NAME).contains("marker"));
    } finally {
      TABLE_OPERATIONS.drop(TEST_DB_NAME, table);
      TABLE_OPERATIONS.drop(TEST_DB_NAME, "marker");
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"on", "off"})
  void testStringFunctionArgumentsAreEscapedForAnyStringSetting(String setting)
      throws SQLException {
    PostgreSqlColumnDefaultValueConverter converter = new PostgreSqlColumnDefaultValueConverter();
    String[] values = {
      "back\\slash O'Brien; --",
      "\\'); CREATE TABLE " + TEST_DB_NAME + ".function_argument_marker(id int); SELECT ('safe"
    };
    try (Connection connection = DATA_SOURCE.getConnection();
        Statement statement = connection.createStatement()) {
      try {
        statement.execute("SET standard_conforming_strings = " + setting);
        for (String value : values) {
          String expression =
              converter.fromGravitino(
                  FunctionExpression.of("concat", Literals.stringLiteral(value)));
          try (ResultSet result = statement.executeQuery("SELECT " + expression)) {
            Assertions.assertTrue(result.next());
            Assertions.assertEquals(value, result.getString(1));
          }
        }
        Assertions.assertFalse(
            TABLE_OPERATIONS.listTables(TEST_DB_NAME).contains("function_argument_marker"));
      } finally {
        statement.execute("RESET standard_conforming_strings");
      }
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"4142", "0080ff", ""})
  void testBinaryDefaultsPreserveBytesOnCreateAndAlter(String hex) throws SQLException {
    String table = RandomNameUtils.genRandomName("binary_defaults");
    byte[] bytes = BaseEncoding.base16().lowerCase().decode(hex);
    TABLE_OPERATIONS.create(
        TEST_DB_NAME,
        table,
        new JdbcColumn[] {
          JdbcColumn.builder()
              .withName("payload")
              .withType(Types.BinaryType.get())
              .withNullable(true)
              .withDefaultValue(Literals.of(bytes, Types.BinaryType.get()))
              .build()
        },
        null,
        Collections.emptyMap(),
        null,
        Distributions.NONE,
        Indexes.EMPTY_INDEXES);
    try (Connection connection = DATA_SOURCE.getConnection();
        Statement statement = connection.createStatement()) {
      String qualified = TEST_DB_NAME + "." + table;
      statement.executeUpdate("INSERT INTO " + qualified + " DEFAULT VALUES");
      try (ResultSet result = statement.executeQuery("SELECT payload FROM " + qualified)) {
        Assertions.assertTrue(result.next());
        Assertions.assertArrayEquals(bytes, result.getBytes(1));
      }
      statement.executeUpdate("DELETE FROM " + qualified);
      byte[] altered = BaseEncoding.base16().lowerCase().decode(hex + "ff");
      TABLE_OPERATIONS.alterTable(
          TEST_DB_NAME,
          table,
          TableChange.updateColumnDefaultValue(
              new String[] {"payload"},
              DTOConverters.toFunctionArg(Literals.of(altered, Types.BinaryType.get()))));
      statement.executeUpdate("INSERT INTO " + qualified + " DEFAULT VALUES");
      try (ResultSet result = statement.executeQuery("SELECT payload FROM " + qualified)) {
        Assertions.assertTrue(result.next());
        Assertions.assertArrayEquals(altered, result.getBytes(1));
      }
    } finally {
      TABLE_OPERATIONS.drop(TEST_DB_NAME, table);
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"quoted\";CREATE TABLE injected_marker (id int);--"})
  void testQuotedTableNamesStayIdentifiersDuringAlter(String name) {
    TABLE_OPERATIONS.create(
        TEST_DB_NAME,
        name,
        new JdbcColumn[] {
          JdbcColumn.builder().withName("id").withType(INT).withNullable(true).build()
        },
        "comment",
        Map.of(),
        null,
        Distributions.NONE,
        Indexes.EMPTY_INDEXES);
    try {
      TABLE_OPERATIONS.alterTable(
          TEST_DB_NAME, name, TableChange.renameColumn(new String[] {"id"}, "id\"quoted"));
      Assertions.assertEquals(
          "id\"quoted", TABLE_OPERATIONS.load(TEST_DB_NAME, name).columns()[0].name());
      TABLE_OPERATIONS.alterTable(TEST_DB_NAME, name, TableChange.updateComment("updated"));
      Assertions.assertEquals("updated", TABLE_OPERATIONS.load(TEST_DB_NAME, name).comment());
      Assertions.assertFalse(TABLE_OPERATIONS.listTables(TEST_DB_NAME).contains("injected_marker"));
    } finally {
      TABLE_OPERATIONS.drop(TEST_DB_NAME, name);
    }
  }

  @Test
  void testIndexNameConflictDoesNotReportExistingTable() throws SQLException {
    String table = RandomNameUtils.genRandomName("index_conflict");
    String index = RandomNameUtils.genRandomName("index_name");
    try (Connection connection = DATA_SOURCE.getConnection()) {
      JdbcConnectorUtils.executeUpdate(
          connection, "CREATE TABLE " + TEST_DB_NAME + "." + table + "(id int, other int)");
      try {
        TABLE_OPERATIONS.alterTable(
            TEST_DB_NAME,
            table,
            TableChange.addIndex(IndexType.UNIQUE_KEY, index, new String[][] {{"id"}}));
        IllegalArgumentException error =
            Assertions.assertThrows(
                IllegalArgumentException.class,
                () ->
                    TABLE_OPERATIONS.alterTable(
                        TEST_DB_NAME,
                        table,
                        TableChange.addIndex(
                            IndexType.UNIQUE_KEY, index, new String[][] {{"other"}})));
        Assertions.assertTrue(error.getMessage().contains(index));
        Assertions.assertTrue(error.getMessage().contains("index or constraint"));
        Assertions.assertTrue(error.getCause() instanceof TableAlreadyExistsException);
        Assertions.assertEquals(1, TABLE_OPERATIONS.load(TEST_DB_NAME, table).index().length);
      } finally {
        TABLE_OPERATIONS.drop(TEST_DB_NAME, table);
      }
    }
  }

  @Test
  void testUnnamedUniqueIndexesDoNotCollideAcrossTables() {
    String first = "quoted\"" + RandomNameUtils.genRandomName("unnamed_first");
    String second = RandomNameUtils.genRandomName("unnamed_second");
    for (String name : new String[] {first, second}) {
      TABLE_OPERATIONS.create(
          TEST_DB_NAME,
          name,
          new JdbcColumn[] {
            JdbcColumn.builder().withName("id").withType(INT).withNullable(true).build()
          },
          null,
          Map.of(),
          null,
          Distributions.NONE,
          Indexes.EMPTY_INDEXES);
    }
    try {
      for (String name : new String[] {first, second}) {
        TABLE_OPERATIONS.alterTable(
            TEST_DB_NAME,
            name,
            TableChange.addIndex(IndexType.UNIQUE_KEY, null, new String[][] {{"id"}}));
        JdbcTable table = TABLE_OPERATIONS.load(TEST_DB_NAME, name);
        Assertions.assertEquals(1, table.index().length);
        Assertions.assertNotEquals("null", table.index()[0].name());
      }
    } finally {
      TABLE_OPERATIONS.drop(TEST_DB_NAME, first);
      TABLE_OPERATIONS.drop(TEST_DB_NAME, second);
    }
  }

  @Test
  void testListTablesEscapesSchemaPattern() throws SQLException {
    String schema = "list\\schema_%";
    String neighbor = "listXschema_neighbor";
    try (Connection connection = DATA_SOURCE.getConnection()) {
      JdbcConnectorUtils.executeUpdate(connection, "CREATE SCHEMA \"" + schema + "\"");
      JdbcConnectorUtils.executeUpdate(connection, "CREATE SCHEMA " + neighbor);
      try {
        JdbcConnectorUtils.executeUpdate(
            connection, "CREATE TABLE \"" + schema + "\".actual_table(id int)");
        JdbcConnectorUtils.executeUpdate(
            connection, "CREATE TABLE " + neighbor + ".wrong_table(id int)");
        Assertions.assertEquals(List.of("actual_table"), TABLE_OPERATIONS.listTables(schema));
      } finally {
        JdbcConnectorUtils.executeUpdate(connection, "DROP SCHEMA \"" + schema + "\" CASCADE");
        JdbcConnectorUtils.executeUpdate(connection, "DROP SCHEMA " + neighbor + " CASCADE");
      }
    }
  }

  @Test
  void testBackslashTableNameLoadsAndAlters() {
    String name = "back\\slash_%";
    TABLE_OPERATIONS.create(
        TEST_DB_NAME,
        name,
        new JdbcColumn[] {
          JdbcColumn.builder().withName("id").withType(INT).withNullable(true).build()
        },
        "comment",
        Map.of(),
        null,
        Distributions.NONE,
        Indexes.EMPTY_INDEXES);
    try {
      Assertions.assertEquals("id", TABLE_OPERATIONS.load(TEST_DB_NAME, name).columns()[0].name());
      TABLE_OPERATIONS.alterTable(TEST_DB_NAME, name, TableChange.updateComment("updated"));
      Assertions.assertEquals("updated", TABLE_OPERATIONS.load(TEST_DB_NAME, name).comment());
    } finally {
      TABLE_OPERATIONS.drop(TEST_DB_NAME, name);
    }
  }

  @Test
  void testLoadMetadataWithoutSchemaUsage() throws SQLException {
    String role = RandomNameUtils.genRandomName("metadata_reader");
    String schema = RandomNameUtils.genRandomName("metadata_schema");
    String table = RandomNameUtils.genRandomName("metadata_table");
    try (Connection admin = DATA_SOURCE.getConnection()) {
      JdbcConnectorUtils.executeUpdate(
          admin, "CREATE ROLE " + role + " LOGIN PASSWORD 'test-only-password'");
      try {
        JdbcConnectorUtils.executeUpdate(admin, "CREATE SCHEMA " + schema);
        JdbcConnectorUtils.executeUpdate(admin, "REVOKE ALL ON SCHEMA " + schema + " FROM PUBLIC");
        JdbcConnectorUtils.executeUpdate(
            admin,
            "CREATE TABLE "
                + schema
                + "."
                + table
                + " (id serial, label varchar, tags varchar(12)[])");
        JdbcConnectorUtils.executeUpdate(
            admin, "GRANT SELECT ON " + schema + "." + table + " TO " + role);
        Map<String, String> properties = new HashMap<>();
        properties.put("jdbc-driver", CONTAINER.getDriverClassName());
        properties.put("jdbc-url", CONTAINER.getJdbcUrl());
        properties.put("jdbc-database", admin.getCatalog());
        properties.put("jdbc-user", role);
        properties.put("jdbc-password", "test-only-password");
        properties.put("jdbc.pool.max-size", "1");
        properties.put("jdbc.pool.min-size", "1");
        try (BasicDataSource limitedSource =
            (BasicDataSource) DataSourceUtils.createDataSource(properties)) {
          PostgreSqlTableOperations limited = new PostgreSqlTableOperations();
          limited.initialize(
              limitedSource,
              JDBC_EXCEPTION_CONVERTER,
              new PostgreSqlTypeConverter(),
              new PostgreSqlColumnDefaultValueConverter(),
              properties);
          try (Connection connection = limitedSource.getConnection();
              Statement statement = connection.createStatement()) {
            try (ResultSet privileges =
                statement.executeQuery(
                    "SELECT pg_catalog.has_schema_privilege(current_user, '"
                        + schema
                        + "', 'USAGE')")) {
              Assertions.assertTrue(privileges.next());
              Assertions.assertFalse(privileges.getBoolean(1));
            }
            try (ResultSet columns =
                connection.getMetaData().getColumns(connection.getCatalog(), schema, table, null)) {
              int count = 0;
              while (columns.next()) {
                count++;
              }
              Assertions.assertEquals(3, count);
            }
            SQLException denied =
                Assertions.assertThrows(
                    SQLException.class,
                    () -> statement.executeQuery("SELECT * FROM " + schema + "." + table));
            Assertions.assertEquals("42501", denied.getSQLState());
          }
          JdbcTable loaded = limited.load(schema, table);
          Assertions.assertEquals(3, loaded.columns().length);
          Assertions.assertEquals(Types.IntegerType.get(), loaded.columns()[0].dataType());
          Assertions.assertTrue(loaded.columns()[0].autoIncrement());
          Assertions.assertEquals(Column.DEFAULT_VALUE_NOT_SET, loaded.columns()[0].defaultValue());
          Assertions.assertEquals(Types.StringType.get(), loaded.columns()[1].dataType());
          Assertions.assertEquals(
              Types.ListType.nullable(Types.VarCharType.of(12)), loaded.columns()[2].dataType());
        }
      } finally {
        JdbcConnectorUtils.executeUpdate(admin, "DROP SCHEMA IF EXISTS " + schema + " CASCADE");
        JdbcConnectorUtils.executeUpdate(admin, "DROP ROLE " + role);
      }
    }
  }

  @Test
  void testDropWithoutSchemaUsageIsForbiddenAndKeepsTable() throws SQLException {
    String role = RandomNameUtils.genRandomName("drop_role");
    String schema = RandomNameUtils.genRandomName("drop_schema");
    String table = RandomNameUtils.genRandomName("drop_table");
    try (Connection admin = DATA_SOURCE.getConnection()) {
      JdbcConnectorUtils.executeUpdate(
          admin, "CREATE ROLE " + role + " LOGIN PASSWORD 'test-only-password'");
      JdbcConnectorUtils.executeUpdate(admin, "CREATE SCHEMA " + schema);
      try {
        JdbcConnectorUtils.executeUpdate(admin, "REVOKE ALL ON SCHEMA " + schema + " FROM PUBLIC");
        JdbcConnectorUtils.executeUpdate(
            admin, "CREATE TABLE " + schema + "." + table + " (id int)");
        Map<String, String> properties = new HashMap<>();
        properties.put("jdbc-driver", CONTAINER.getDriverClassName());
        properties.put("jdbc-url", CONTAINER.getJdbcUrl());
        properties.put("jdbc-database", admin.getCatalog());
        properties.put("jdbc-user", role);
        properties.put("jdbc-password", "test-only-password");
        properties.put("jdbc.pool.max-size", "1");
        properties.put("jdbc.pool.min-size", "1");
        try (BasicDataSource limitedSource =
            (BasicDataSource) DataSourceUtils.createDataSource(properties)) {
          PostgreSqlTableOperations limited = new PostgreSqlTableOperations();
          limited.initialize(
              limitedSource,
              JDBC_EXCEPTION_CONVERTER,
              new PostgreSqlTypeConverter(),
              new PostgreSqlColumnDefaultValueConverter(),
              properties);
          Assertions.assertThrows(ForbiddenException.class, () -> limited.drop(schema, table));
          Assertions.assertEquals(table, TABLE_OPERATIONS.load(schema, table).name());
        }
        Assertions.assertTrue(TABLE_OPERATIONS.drop(schema, table));
        Assertions.assertFalse(TABLE_OPERATIONS.drop(schema, table));
      } finally {
        JdbcConnectorUtils.executeUpdate(admin, "DROP SCHEMA IF EXISTS " + schema + " CASCADE");
        JdbcConnectorUtils.executeUpdate(admin, "DROP ROLE IF EXISTS " + role);
      }
    }
  }

  @Test
  void testLeastPrivilegeCreateDistinguishesMissingSchemaFromForbidden() throws SQLException {
    String role = RandomNameUtils.genRandomName("limited_role");
    String schema = RandomNameUtils.genRandomName("restricted_schema");
    String table = RandomNameUtils.genRandomName("permission_table");
    try (Connection admin = DATA_SOURCE.getConnection()) {
      JdbcConnectorUtils.executeUpdate(
          admin, "CREATE ROLE " + role + " LOGIN PASSWORD 'test-only-password'");
      JdbcConnectorUtils.executeUpdate(admin, "CREATE SCHEMA " + schema);
      try {
        JdbcConnectorUtils.executeUpdate(admin, "REVOKE ALL ON SCHEMA " + schema + " FROM PUBLIC");
        Map<String, String> properties = new HashMap<>();
        properties.put("jdbc-driver", CONTAINER.getDriverClassName());
        properties.put("jdbc-url", CONTAINER.getJdbcUrl());
        properties.put("jdbc-database", admin.getCatalog());
        properties.put("jdbc-user", role);
        properties.put("jdbc-password", "test-only-password");
        properties.put("jdbc.pool.max-size", "1");
        properties.put("jdbc.pool.min-size", "1");
        try (BasicDataSource limitedSource =
            (BasicDataSource) DataSourceUtils.createDataSource(properties)) {
          PostgreSqlTableOperations limited = new PostgreSqlTableOperations();
          limited.initialize(
              limitedSource,
              JDBC_EXCEPTION_CONVERTER,
              new PostgreSqlTypeConverter(),
              new PostgreSqlColumnDefaultValueConverter(),
              properties);
          JdbcColumn[] columns = {
            JdbcColumn.builder().withName("id").withType(INT).withNullable(true).build()
          };
          Assertions.assertThrows(
              ForbiddenException.class,
              () ->
                  limited.create(
                      schema,
                      table,
                      columns,
                      null,
                      Map.of(),
                      null,
                      Distributions.NONE,
                      Indexes.EMPTY_INDEXES));
          JdbcConnectorUtils.executeUpdate(
              admin, "GRANT USAGE ON SCHEMA " + schema + " TO " + role);
          Assertions.assertThrows(
              ForbiddenException.class,
              () ->
                  limited.create(
                      schema,
                      table,
                      columns,
                      null,
                      Map.of(),
                      null,
                      Distributions.NONE,
                      Indexes.EMPTY_INDEXES));
          JdbcConnectorUtils.executeUpdate(
              admin, "GRANT CREATE ON SCHEMA " + schema + " TO " + role);
          Assertions.assertDoesNotThrow(
              () ->
                  limited.create(
                      schema,
                      table,
                      columns,
                      null,
                      Map.of(),
                      null,
                      Distributions.NONE,
                      Indexes.EMPTY_INDEXES));
          Assertions.assertEquals(table, TABLE_OPERATIONS.load(schema, table).name());
          String absent = RandomNameUtils.genRandomName("missing_schema");
          Assertions.assertThrows(
              NoSuchSchemaException.class,
              () ->
                  limited.create(
                      absent,
                      table,
                      columns,
                      null,
                      Map.of(),
                      null,
                      Distributions.NONE,
                      Indexes.EMPTY_INDEXES));
        }
      } finally {
        JdbcConnectorUtils.executeUpdate(admin, "DROP SCHEMA IF EXISTS " + schema + " CASCADE");
        JdbcConnectorUtils.executeUpdate(admin, "DROP ROLE IF EXISTS " + role);
      }
    }
  }

  @Test
  void testNulValidationPreventsCreateAndPartialAlterDdl() {
    String table = RandomNameUtils.genRandomName("nul_validation");
    String invalidTable = RandomNameUtils.genRandomName("nul_create");
    JdbcColumn[] columns = {
      JdbcColumn.builder().withName("col1").withType(VARCHAR).withNullable(true).build()
    };
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            TABLE_OPERATIONS.create(
                TEST_DB_NAME,
                invalidTable,
                columns,
                "bad\0comment",
                Map.of(),
                null,
                Distributions.NONE,
                Indexes.EMPTY_INDEXES));
    Assertions.assertFalse(TABLE_OPERATIONS.listTables(TEST_DB_NAME).contains(invalidTable));
    TABLE_OPERATIONS.create(
        TEST_DB_NAME,
        table,
        columns,
        "original",
        Map.of(),
        null,
        Distributions.NONE,
        Indexes.EMPTY_INDEXES);
    try {
      Assertions.assertThrows(
          IllegalArgumentException.class,
          () ->
              TABLE_OPERATIONS.alterTable(
                  TEST_DB_NAME,
                  table,
                  TableChange.addColumn(new String[] {"must_not_exist"}, INT),
                  TableChange.updateComment("bad\0comment")));
      JdbcTable loaded = TABLE_OPERATIONS.load(TEST_DB_NAME, table);
      Assertions.assertEquals(1, loaded.columns().length);
      Assertions.assertEquals("original", loaded.comment());
      Assertions.assertThrows(
          IllegalArgumentException.class,
          () ->
              TABLE_OPERATIONS.alterTable(
                  TEST_DB_NAME,
                  table,
                  TableChange.updateColumnDefaultValue(
                      new String[] {"col1"}, Literals.stringLiteral("bad\0default"))));
      Assertions.assertEquals(
          Literals.NULL, TABLE_OPERATIONS.load(TEST_DB_NAME, table).columns()[0].defaultValue());
    } finally {
      TABLE_OPERATIONS.drop(TEST_DB_NAME, table);
    }
  }

  @Test
  void testNativeSerialColumnsRetainIntegerTypesAndAutoIncrement() throws SQLException {
    String name = RandomNameUtils.genRandomName("serial_types");
    try (Connection connection = DATA_SOURCE.getConnection()) {
      JdbcConnectorUtils.executeUpdate(
          connection,
          "CREATE TABLE "
              + TEST_DB_NAME
              + "."
              + name
              + " (small_id smallserial, id serial, big_id bigserial)");
      try {
        JdbcTable table = TABLE_OPERATIONS.load(TEST_DB_NAME, name);
        Assertions.assertEquals(Types.ShortType.get(), table.columns()[0].dataType());
        Assertions.assertEquals(Types.IntegerType.get(), table.columns()[1].dataType());
        Assertions.assertEquals(Types.LongType.get(), table.columns()[2].dataType());
        for (Column column : table.columns()) {
          Assertions.assertTrue(column.autoIncrement());
          Assertions.assertEquals(Column.DEFAULT_VALUE_NOT_SET, column.defaultValue());
        }
      } finally {
        TABLE_OPERATIONS.drop(TEST_DB_NAME, name);
      }
    }
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "smallserial",
        "serial",
        "bigserial",
        "smallint GENERATED BY DEFAULT AS IDENTITY",
        "integer GENERATED ALWAYS AS IDENTITY",
        "bigint GENERATED BY DEFAULT AS IDENTITY"
      })
  void testNativeAutoIncrementColumnsCanBeRecreated(String declaration) throws SQLException {
    String source = RandomNameUtils.genRandomName("serial_source") + "_'\"\\%_";
    String target = RandomNameUtils.genRandomName("serial_copy");
    String qualifiedSource = TEST_DB_NAME + ".\"" + source.replace("\"", "\"\"") + "\"";
    try (Connection connection = DATA_SOURCE.getConnection();
        Statement statement = connection.createStatement()) {
      statement.executeUpdate(
          "CREATE TABLE "
              + qualifiedSource
              + " (id "
              + declaration
              + ", ordinary int DEFAULT 42, label varchar DEFAULT 'value')");
      try {
        statement.executeUpdate("INSERT INTO " + qualifiedSource + " DEFAULT VALUES");
        if (declaration.contains("GENERATED ALWAYS")) {
          SQLException error =
              Assertions.assertThrows(
                  SQLException.class,
                  () ->
                      statement.executeUpdate(
                          "INSERT INTO " + qualifiedSource + " (id) VALUES (1000)"));
          Assertions.assertEquals("428C9", error.getSQLState());
        }
        JdbcTable loaded = TABLE_OPERATIONS.load(TEST_DB_NAME, source);
        Column id = loaded.columns()[0];
        Assertions.assertTrue(id.autoIncrement());
        Assertions.assertEquals(Column.DEFAULT_VALUE_NOT_SET, id.defaultValue());
        Assertions.assertFalse(id.nullable());
        TABLE_OPERATIONS.create(
            TEST_DB_NAME,
            target,
            Arrays.stream(loaded.columns()).map(JdbcColumn.class::cast).toArray(JdbcColumn[]::new),
            null,
            Map.of(),
            null,
            Distributions.NONE,
            Indexes.EMPTY_INDEXES);
        TABLE_OPERATIONS.alterTable(
            TEST_DB_NAME,
            target,
            TableChange.addColumn(
                new String[] {"added_id"},
                id.dataType(),
                null,
                TableChange.ColumnPosition.defaultPos(),
                false,
                true,
                id.defaultValue()));
        // Recreation preserves basic generation through BY DEFAULT, including for an ALWAYS
        // source. Explicit values remain accepted by both the copied and added identity columns.
        try (ResultSet rows =
            statement.executeQuery(
                "INSERT INTO "
                    + TEST_DB_NAME
                    + "."
                    + target
                    + " (id, added_id) VALUES (1000, 2000) RETURNING id, added_id")) {
          Assertions.assertTrue(rows.next());
          Assertions.assertEquals(1000, rows.getLong("id"));
          Assertions.assertEquals(2000, rows.getLong("added_id"));
          Assertions.assertFalse(rows.next());
        }
        for (int expected = 1; expected <= 3; expected++) {
          if (expected == 3) {
            // Dropping the original table must not remove either generator on the new table.
            TABLE_OPERATIONS.drop(TEST_DB_NAME, source);
          }
          try (ResultSet rows =
              statement.executeQuery(
                  "INSERT INTO "
                      + TEST_DB_NAME
                      + "."
                      + target
                      + " DEFAULT VALUES RETURNING id, added_id, ordinary, label")) {
            Assertions.assertTrue(rows.next());
            Assertions.assertEquals(expected, rows.getLong("id"));
            Assertions.assertEquals(expected, rows.getLong("added_id"));
            Assertions.assertEquals(42, rows.getInt("ordinary"));
            Assertions.assertEquals("value", rows.getString("label"));
          }
        }
        JdbcTable copy = TABLE_OPERATIONS.load(TEST_DB_NAME, target);
        for (int index : new int[] {0, 3}) {
          Assertions.assertEquals(id.dataType(), copy.columns()[index].dataType());
          Assertions.assertTrue(copy.columns()[index].autoIncrement());
          Assertions.assertEquals(
              Column.DEFAULT_VALUE_NOT_SET, copy.columns()[index].defaultValue());
        }
      } finally {
        TABLE_OPERATIONS.drop(TEST_DB_NAME, target);
        TABLE_OPERATIONS.drop(TEST_DB_NAME, source);
      }
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"shared", "owned_custom", "nullable_owned"})
  void testNonSerialSequenceDefaultsRemainUnparsed(String scenario) throws SQLException {
    boolean ownedCustomExpression = scenario.equals("owned_custom");
    boolean ownedSequence = !scenario.equals("shared");
    String source = RandomNameUtils.genRandomName("sequence_default");
    String target = source + "_copy";
    String sequence = source + "_seq";
    try (Connection connection = DATA_SOURCE.getConnection();
        Statement statement = connection.createStatement()) {
      statement.executeUpdate("CREATE SEQUENCE " + TEST_DB_NAME + "." + sequence);
      try {
        statement.executeUpdate(
            "CREATE TABLE "
                + TEST_DB_NAME
                + "."
                + source
                + " (id integer "
                + (scenario.equals("nullable_owned") ? "" : "NOT NULL ")
                + "DEFAULT nextval('"
                + TEST_DB_NAME
                + "."
                + sequence
                + "'::regclass)"
                + (ownedCustomExpression ? " + 1" : "")
                + ")");
        if (ownedSequence) {
          statement.executeUpdate(
              "ALTER SEQUENCE "
                  + TEST_DB_NAME
                  + "."
                  + sequence
                  + " OWNED BY "
                  + TEST_DB_NAME
                  + "."
                  + source
                  + ".id");
        }
        JdbcColumn id = (JdbcColumn) TABLE_OPERATIONS.load(TEST_DB_NAME, source).columns()[0];
        Assertions.assertTrue(id.autoIncrement());
        Assertions.assertInstanceOf(UnparsedExpression.class, id.defaultValue());
        IllegalArgumentException error =
            Assertions.assertThrows(
                IllegalArgumentException.class,
                () ->
                    TABLE_OPERATIONS.create(
                        TEST_DB_NAME,
                        target,
                        new JdbcColumn[] {id},
                        null,
                        Map.of(),
                        null,
                        Distributions.NONE,
                        Indexes.EMPTY_INDEXES));
        Assertions.assertTrue(error.getMessage().contains("Not a supported column default value"));
        Assertions.assertFalse(TABLE_OPERATIONS.listTables(TEST_DB_NAME).contains(target));
        TABLE_OPERATIONS.drop(TEST_DB_NAME, source);
        if (!ownedSequence) {
          try (ResultSet rows =
              statement.executeQuery(
                  "SELECT nextval('" + TEST_DB_NAME + "." + sequence + "'::regclass)")) {
            Assertions.assertTrue(rows.next());
            Assertions.assertEquals(1, rows.getLong(1));
          }
        }
      } finally {
        TABLE_OPERATIONS.drop(TEST_DB_NAME, target);
        TABLE_OPERATIONS.drop(TEST_DB_NAME, source);
        statement.executeUpdate("DROP SEQUENCE IF EXISTS " + TEST_DB_NAME + "." + sequence);
      }
    }
  }

  @Test
  void testArrayElementMetadataFromSource() throws SQLException {
    String name = RandomNameUtils.genRandomName("array_metadata");
    try (Connection connection = DATA_SOURCE.getConnection()) {
      JdbcConnectorUtils.executeUpdate(
          connection,
          "CREATE TABLE "
              + TEST_DB_NAME
              + "."
              + name
              + "(c char(3)[], v varchar(12)[], n numeric(10,2)[], ts timestamp(3)[], wide numeric(39,0)[], "
              + "unbounded varchar[], unconstrained numeric[], t time(0)[], tz timestamptz(6)[], "
              + "max_precision numeric(1000,0)[])");
      try {
        JdbcTable table = TABLE_OPERATIONS.load(TEST_DB_NAME, name);
        Assertions.assertEquals(
            Types.ListType.nullable(Types.FixedCharType.of(3)), table.columns()[0].dataType());
        Assertions.assertEquals(
            Types.ListType.nullable(Types.VarCharType.of(12)), table.columns()[1].dataType());
        Assertions.assertEquals(
            Types.ListType.nullable(Types.DecimalType.of(10, 2)), table.columns()[2].dataType());
        Assertions.assertEquals(
            Types.ListType.nullable(Types.TimestampType.withoutTimeZone(3)),
            table.columns()[3].dataType());
        Assertions.assertEquals(
            Types.ListType.nullable(Types.ExternalType.of("numeric(39,0)")),
            table.columns()[4].dataType());
        Assertions.assertEquals(
            Types.ListType.nullable(Types.StringType.get()), table.columns()[5].dataType());
        Assertions.assertEquals(
            Types.ListType.nullable(Types.ExternalType.of("numeric")),
            table.columns()[6].dataType());
        Assertions.assertEquals(
            Types.ListType.nullable(Types.TimeType.of(0)), table.columns()[7].dataType());
        Assertions.assertEquals(
            Types.ListType.nullable(Types.TimestampType.withTimeZone(6)),
            table.columns()[8].dataType());
        Assertions.assertEquals(
            Types.ListType.nullable(Types.ExternalType.of("numeric(1000,0)")),
            table.columns()[9].dataType());
      } finally {
        TABLE_OPERATIONS.drop(TEST_DB_NAME, name);
      }
    }
  }

  @Test
  void testNativeUnboundedBpcharPreservesSemanticsOnRoundTrip() throws SQLException {
    String name = RandomNameUtils.genRandomName("unbounded_bpchar");
    String copy = RandomNameUtils.genRandomName("bpchar_copy");
    try (Connection connection = DATA_SOURCE.getConnection();
        Statement statement = connection.createStatement()) {
      statement.executeUpdate(
          "CREATE TABLE "
              + TEST_DB_NAME
              + "."
              + name
              + " (label bpchar, labels bpchar[], bounded char(3))");
      try {
        JdbcTable table = TABLE_OPERATIONS.load(TEST_DB_NAME, name);
        Assertions.assertEquals(Types.ExternalType.of("bpchar"), table.columns()[0].dataType());
        Assertions.assertEquals(
            Types.ListType.nullable(Types.ExternalType.of("bpchar")),
            table.columns()[1].dataType());
        Assertions.assertEquals(Types.FixedCharType.of(3), table.columns()[2].dataType());
        TABLE_OPERATIONS.create(
            TEST_DB_NAME,
            copy,
            Arrays.stream(table.columns()).map(JdbcColumn.class::cast).toArray(JdbcColumn[]::new),
            null,
            Collections.emptyMap(),
            Transforms.EMPTY_TRANSFORM,
            Distributions.NONE,
            Indexes.EMPTY_INDEXES);
        statement.executeUpdate(
            "INSERT INTO "
                + TEST_DB_NAME
                + "."
                + copy
                + " (label, labels) VALUES ('a', '{a}'), ('a ', '{\"a \"}')");
        try (ResultSet result =
            statement.executeQuery(
                "SELECT count(DISTINCT label), count(DISTINCT labels) FROM "
                    + TEST_DB_NAME
                    + "."
                    + copy)) {
          Assertions.assertTrue(result.next());
          Assertions.assertEquals(1, result.getInt(1));
          Assertions.assertEquals(1, result.getInt(2));
        }
      } finally {
        TABLE_OPERATIONS.drop(TEST_DB_NAME, copy);
        TABLE_OPERATIONS.drop(TEST_DB_NAME, name);
      }
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 12, 255, Integer.MAX_VALUE})
  void testCharacterDeclarationsDoNotDependOnDriverUnknownLength(int unknownLength)
      throws SQLException {
    Map<String, String> properties = new HashMap<>();
    properties.put(JdbcConfig.JDBC_DRIVER.getKey(), CONTAINER.getDriverClassName());
    String jdbcUrl = CONTAINER.getJdbcUrl();
    properties.put(
        JdbcConfig.JDBC_URL.getKey(),
        jdbcUrl + (jdbcUrl.contains("?") ? "&" : "?") + "unknownLength=" + unknownLength);
    properties.put(JdbcConfig.USERNAME.getKey(), CONTAINER.getUsername());
    properties.put(JdbcConfig.PASSWORD.getKey(), CONTAINER.getPassword());
    DataSource dataSource = DataSourceUtils.createDataSource(properties);
    PostgreSqlTableOperations operations = new PostgreSqlTableOperations();
    operations.initialize(
        dataSource,
        JDBC_EXCEPTION_CONVERTER,
        new PostgreSqlTypeConverter(),
        new PostgreSqlColumnDefaultValueConverter(),
        Map.of(JdbcConfig.JDBC_DATABASE.getKey(), CONTAINER.getDatabaseName()));
    operations.setDatabaseOperation(DATABASE_OPERATIONS);
    String name = RandomNameUtils.genRandomName("character_lengths");
    String copy = RandomNameUtils.genRandomName("character_copy");
    try (Connection connection = dataSource.getConnection();
        Statement statement = connection.createStatement()) {
      statement.executeUpdate(
          "CREATE TABLE "
              + TEST_DB_NAME
              + "."
              + name
              + " (label varchar DEFAULT 'value', labels varchar[], "
              + "bounded varchar(12) DEFAULT 'bound', bounded_array varchar(12)[], "
              + "fixed char(12), fixed_array char(12)[], "
              + "bounded_255 varchar(255), bounded_255_array varchar(255)[])");
      try {
        JdbcTable table = operations.load(TEST_DB_NAME, name);
        Type[] types = {
          Types.StringType.get(), Types.ListType.nullable(Types.StringType.get()),
          Types.VarCharType.of(12), Types.ListType.nullable(Types.VarCharType.of(12)),
          Types.FixedCharType.of(12), Types.ListType.nullable(Types.FixedCharType.of(12)),
          Types.VarCharType.of(255), Types.ListType.nullable(Types.VarCharType.of(255))
        };
        for (int i = 0; i < types.length; i++) {
          Assertions.assertEquals(types[i], table.columns()[i].dataType());
        }
        Assertions.assertEquals(Literals.stringLiteral("value"), table.columns()[0].defaultValue());
        Assertions.assertEquals(
            Literals.varcharLiteral(12, "bound"), table.columns()[2].defaultValue());
        operations.create(
            TEST_DB_NAME,
            copy,
            Arrays.stream(table.columns()).map(JdbcColumn.class::cast).toArray(JdbcColumn[]::new),
            null,
            Collections.emptyMap(),
            Transforms.EMPTY_TRANSFORM,
            Distributions.NONE,
            Indexes.EMPTY_INDEXES);
        statement.executeUpdate(
            "INSERT INTO "
                + TEST_DB_NAME
                + "."
                + name
                + " (label, labels) VALUES (repeat('x', 300), ARRAY[repeat('x', 300)])");
        statement.executeUpdate(
            "INSERT INTO "
                + TEST_DB_NAME
                + "."
                + copy
                + " SELECT * FROM "
                + TEST_DB_NAME
                + "."
                + name);
        try (ResultSet result =
            statement.executeQuery(
                "SELECT char_length(label), char_length(labels[1]) FROM "
                    + TEST_DB_NAME
                    + "."
                    + copy)) {
          Assertions.assertTrue(result.next());
          Assertions.assertEquals(300, result.getInt(1));
          Assertions.assertEquals(300, result.getInt(2));
        }
      } finally {
        operations.drop(TEST_DB_NAME, copy);
        operations.drop(TEST_DB_NAME, name);
      }
    } finally {
      DataSourceUtils.closeDataSource(dataSource);
    }
  }

  @Test
  void testCharacterMetadataEscapesSchemaAndTablePatterns() throws SQLException {
    String schema = RandomNameUtils.genRandomName("character_schema") + "\\_%";
    String table = "character\"\\_%";
    String qualified =
        "\"" + schema.replace("\"", "\"\"") + "\".\"" + table.replace("\"", "\"\"") + "\"";
    DATABASE_OPERATIONS.create(schema, null, Map.of());
    try (Connection connection = DATA_SOURCE.getConnection()) {
      JdbcConnectorUtils.executeUpdate(
          connection,
          "CREATE TABLE "
              + qualified
              + " (v varchar, vs varchar[], b bpchar, bs bpchar[], bounded varchar(12))");
      try {
        JdbcTable loaded = TABLE_OPERATIONS.load(schema, table);
        Type[] expected = {
          Types.StringType.get(),
          Types.ListType.nullable(Types.StringType.get()),
          Types.ExternalType.of("bpchar"),
          Types.ListType.nullable(Types.ExternalType.of("bpchar")),
          Types.VarCharType.of(12)
        };
        Assertions.assertEquals(expected.length, loaded.columns().length);
        for (int i = 0; i < expected.length; i++) {
          Assertions.assertEquals(expected[i], loaded.columns()[i].dataType());
        }
      } finally {
        TABLE_OPERATIONS.drop(schema, table);
      }
    } finally {
      DATABASE_OPERATIONS.delete(schema, false);
    }
  }

  @Test
  void testNativeUnboundedVarcharLoadsAsString() throws SQLException {
    String name = RandomNameUtils.genRandomName("unbounded_varchar");
    try (Connection connection = DATA_SOURCE.getConnection()) {
      JdbcConnectorUtils.executeUpdate(
          connection,
          "CREATE TABLE " + TEST_DB_NAME + "." + name + " (label varchar, bounded varchar(255))");
    }
    try {
      JdbcTable table = TABLE_OPERATIONS.load(TEST_DB_NAME, name);
      Assertions.assertEquals(Types.StringType.get(), table.columns()[0].dataType());
      Assertions.assertEquals(Types.VarCharType.of(255), table.columns()[1].dataType());
    } finally {
      TABLE_OPERATIONS.drop(TEST_DB_NAME, name);
    }
  }

  @Test
  public void testOperationTable() {
    String tableName = RandomNameUtils.genRandomName("op_table_");
    String tableComment = "test_comment";
    List<JdbcColumn> columns = new ArrayList<>();
    columns.add(
        JdbcColumn.builder()
            .withName("col_1")
            .withType(Types.LongType.get())
            .withComment("increment key")
            .withNullable(false)
            .withAutoIncrement(true)
            .build());
    columns.add(
        JdbcColumn.builder()
            .withName("col_2")
            .withType(INT)
            .withNullable(false)
            .withComment("set test key")
            .build());
    columns.add(JdbcColumn.builder().withName("col_3").withType(INT).withNullable(true).build());
    columns.add(
        JdbcColumn.builder()
            .withName("col_4")
            .withType(VARCHAR)
            .withDefaultValue(Literals.of("hello world", VARCHAR))
            .withNullable(false)
            .build());
    Map<String, String> properties = new HashMap<>();
    // create table
    TABLE_OPERATIONS.create(
        TEST_DB_NAME,
        tableName,
        columns.toArray(new JdbcColumn[0]),
        tableComment,
        properties,
        null,
        Distributions.NONE,
        Indexes.EMPTY_INDEXES);

    // list table
    List<String> tables = TABLE_OPERATIONS.listTables(TEST_DB_NAME);
    Assertions.assertTrue(tables.contains(tableName));

    // load table
    JdbcTable load = TABLE_OPERATIONS.load(TEST_DB_NAME, tableName);
    assertionsTableInfo(
        tableName, tableComment, columns, properties, null, Transforms.EMPTY_TRANSFORM, load);

    // rename table
    String newName = "new_table";
    Assertions.assertDoesNotThrow(() -> TABLE_OPERATIONS.rename(TEST_DB_NAME, tableName, newName));
    Assertions.assertDoesNotThrow(() -> TABLE_OPERATIONS.load(TEST_DB_NAME, newName));

    // alter table
    JdbcColumn newColumn =
        JdbcColumn.builder()
            .withName("col_5")
            .withType(VARCHAR)
            .withComment("new_add")
            .withNullable(true)
            .build();
    JdbcColumn newColumn1 =
        JdbcColumn.builder()
            .withName("col_6")
            .withType(Types.BooleanType.get())
            .withComment("new_add")
            .withDefaultValue(Literals.of("true", Types.BooleanType.get()))
            .build();
    TABLE_OPERATIONS.alterTable(
        TEST_DB_NAME,
        newName,
        TableChange.addColumn(
            new String[] {newColumn.name()}, newColumn.dataType(), newColumn.comment()),
        TableChange.addColumn(
            new String[] {newColumn1.name()},
            newColumn1.dataType(),
            newColumn1.comment(),
            newColumn1.defaultValue()),
        TableChange.updateColumnComment(new String[] {columns.get(0).name()}, "test_new_comment"),
        TableChange.updateColumnType(
            new String[] {columns.get(1).name()}, Types.DecimalType.of(10, 2)),
        TableChange.updateColumnDefaultValue(
            new String[] {columns.get(3).name()}, Literals.of("new hello world", VARCHAR)),
        TableChange.deleteColumn(new String[] {columns.get(2).name()}, true));
    load = TABLE_OPERATIONS.load(TEST_DB_NAME, newName);
    List<JdbcColumn> alterColumns = new ArrayList<JdbcColumn>();
    alterColumns.add(
        JdbcColumn.builder()
            .withName("col_1")
            .withType(Types.LongType.get())
            .withComment("test_new_comment")
            .withNullable(false)
            .withAutoIncrement(true)
            .build());
    alterColumns.add(
        JdbcColumn.builder()
            .withName("col_2")
            .withType(Types.DecimalType.of(10, 2))
            .withNullable(false)
            .withComment("set test key")
            .build());
    alterColumns.add(
        JdbcColumn.builder()
            .withName("col_4")
            .withType(VARCHAR)
            .withDefaultValue(Literals.of("new hello world", VARCHAR))
            .withNullable(false)
            .build());
    alterColumns.add(newColumn);
    alterColumns.add(newColumn1);
    assertionsTableInfo(
        newName, tableComment, alterColumns, properties, null, Transforms.EMPTY_TRANSFORM, load);

    TABLE_OPERATIONS.alterTable(
        TEST_DB_NAME,
        newName,
        TableChange.renameColumn(new String[] {"col_1"}, "col_1_new"),
        TableChange.updateColumnType(new String[] {"col_2"}, Types.DoubleType.get()));
    load = TABLE_OPERATIONS.load(TEST_DB_NAME, newName);
    alterColumns.clear();
    alterColumns.add(
        JdbcColumn.builder()
            .withName("col_1_new")
            .withType(Types.LongType.get())
            .withComment("test_new_comment")
            .withNullable(false)
            .withAutoIncrement(true)
            .build());
    alterColumns.add(
        JdbcColumn.builder()
            .withName("col_2")
            .withType(Types.DoubleType.get())
            .withNullable(false)
            .withComment("set test key")
            .build());
    alterColumns.add(
        JdbcColumn.builder()
            .withName("col_4")
            .withType(VARCHAR)
            .withDefaultValue(Literals.of("new hello world", VARCHAR))
            .withNullable(false)
            .build());
    alterColumns.add(newColumn);
    alterColumns.add(newColumn1);
    assertionsTableInfo(
        newName, tableComment, alterColumns, properties, null, Transforms.EMPTY_TRANSFORM, load);

    // alter column Nullability
    TABLE_OPERATIONS.alterTable(
        TEST_DB_NAME, newName, TableChange.updateColumnNullability(new String[] {"col_2"}, true));
    load = TABLE_OPERATIONS.load(TEST_DB_NAME, newName);
    alterColumns.clear();
    alterColumns.add(
        JdbcColumn.builder()
            .withName("col_1_new")
            .withType(Types.LongType.get())
            .withComment("test_new_comment")
            .withNullable(false)
            .withAutoIncrement(true)
            .build());
    alterColumns.add(
        JdbcColumn.builder()
            .withName("col_2")
            .withType(Types.DoubleType.get())
            .withNullable(true)
            .withComment("set test key")
            .build());
    alterColumns.add(
        JdbcColumn.builder()
            .withName("col_4")
            .withType(VARCHAR)
            .withDefaultValue(Literals.of("new hello world", VARCHAR))
            .withNullable(false)
            .build());
    alterColumns.add(newColumn);
    alterColumns.add(newColumn1);
    assertionsTableInfo(
        newName, tableComment, alterColumns, properties, null, Transforms.EMPTY_TRANSFORM, load);

    // a nullable column with a NULL default can be changed back to not null, and the NULL
    // default is dropped
    TABLE_OPERATIONS.alterTable(
        TEST_DB_NAME,
        newName,
        TableChange.updateColumnDefaultValue(new String[] {"col_2"}, Literals.NULL));
    TABLE_OPERATIONS.alterTable(
        TEST_DB_NAME, newName, TableChange.updateColumnNullability(new String[] {"col_2"}, false));
    load = TABLE_OPERATIONS.load(TEST_DB_NAME, newName);
    alterColumns.set(
        1,
        JdbcColumn.builder()
            .withName("col_2")
            .withType(Types.DoubleType.get())
            .withNullable(false)
            .withComment("set test key")
            .build());
    assertionsTableInfo(
        newName, tableComment, alterColumns, properties, null, Transforms.EMPTY_TRANSFORM, load);

    // delete column
    TABLE_OPERATIONS.alterTable(
        TEST_DB_NAME,
        newName,
        TableChange.deleteColumn(new String[] {newColumn.name()}, true),
        TableChange.deleteColumn(new String[] {newColumn1.name()}, true));

    load = TABLE_OPERATIONS.load(TEST_DB_NAME, newName);
    alterColumns.remove(newColumn);
    alterColumns.remove(newColumn1);
    assertionsTableInfo(
        newName, tableComment, alterColumns, properties, null, Transforms.EMPTY_TRANSFORM, load);

    TableChange deleteColumn = TableChange.deleteColumn(new String[] {newColumn.name()}, false);
    IllegalArgumentException illegalArgumentException =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> TABLE_OPERATIONS.alterTable(TEST_DB_NAME, newName, deleteColumn));
    Assertions.assertEquals(
        "Delete column does not exist: " + newColumn.name(), illegalArgumentException.getMessage());

    Assertions.assertDoesNotThrow(
        () ->
            TABLE_OPERATIONS.alterTable(
                TEST_DB_NAME,
                newName,
                TableChange.deleteColumn(new String[] {newColumn.name()}, true)));
    Assertions.assertDoesNotThrow(() -> TABLE_OPERATIONS.drop(TEST_DB_NAME, newName));
    Assertions.assertFalse(TABLE_OPERATIONS.drop(TEST_DB_NAME, newName));
  }

  @Test
  public void testUpdateColumnDefaultValueAndNullabilityTogether() {
    String tableName = RandomNameUtils.genRandomName("default_not_null_table_");
    JdbcColumn[] columns = {
      JdbcColumn.builder()
          .withName("col_1")
          .withType(Types.IntegerType.get())
          .withNullable(true)
          .withComment("set default first")
          .build(),
      JdbcColumn.builder()
          .withName("col_2")
          .withType(Types.IntegerType.get())
          .withNullable(true)
          .withComment("set not null first")
          .build()
    };
    TABLE_OPERATIONS.create(
        TEST_DB_NAME,
        tableName,
        columns,
        "test_comment",
        new HashMap<>(),
        null,
        Distributions.NONE,
        Indexes.EMPTY_INDEXES);

    TABLE_OPERATIONS.alterTable(
        TEST_DB_NAME,
        tableName,
        TableChange.updateColumnDefaultValue(new String[] {"col_1"}, Literals.integerLiteral(5)),
        TableChange.updateColumnNullability(new String[] {"col_1"}, false),
        TableChange.updateColumnNullability(new String[] {"col_2"}, false),
        TableChange.updateColumnDefaultValue(new String[] {"col_2"}, Literals.integerLiteral(7)));

    JdbcTable load = TABLE_OPERATIONS.load(TEST_DB_NAME, tableName);
    Map<String, Column> loadedColumns =
        Arrays.stream(load.columns()).collect(Collectors.toMap(Column::name, c -> c));
    Assertions.assertFalse(loadedColumns.get("col_1").nullable());
    Assertions.assertEquals(Literals.integerLiteral(5), loadedColumns.get("col_1").defaultValue());
    Assertions.assertFalse(loadedColumns.get("col_2").nullable());
    Assertions.assertEquals(Literals.integerLiteral(7), loadedColumns.get("col_2").defaultValue());
  }

  @Test
  public void testClearCommentsWithNull() {
    String tableName = RandomNameUtils.genRandomName("clear_comment_");
    JdbcColumn column =
        JdbcColumn.builder().withName("col_1").withType(INT).withComment("col comment").build();
    try {
      TABLE_OPERATIONS.create(
          TEST_DB_NAME,
          tableName,
          new JdbcColumn[] {column},
          "table comment",
          new HashMap<>(),
          null,
          Distributions.NONE,
          Indexes.EMPTY_INDEXES);

      TABLE_OPERATIONS.alterTable(
          TEST_DB_NAME,
          tableName,
          TableChange.updateColumnComment(new String[] {"col_1"}, null),
          TableChange.updateComment(null));

      JdbcTable load = TABLE_OPERATIONS.load(TEST_DB_NAME, tableName);
      Assertions.assertTrue(StringUtils.isEmpty(load.comment()), load.comment());
      Assertions.assertTrue(
          StringUtils.isEmpty(load.columns()[0].comment()), load.columns()[0].comment());
    } finally {
      TABLE_OPERATIONS.drop(TEST_DB_NAME, tableName);
    }
  }

  @Test
  public void testCreateAllTypeTable() {
    String tableName = RandomNameUtils.genRandomName("type_table_");
    String tableComment = "test_comment";
    List<JdbcColumn> columns = new ArrayList<>();
    columns.add(
        JdbcColumn.builder()
            .withName("col_1")
            .withType(Types.BooleanType.get())
            .withNullable(true)
            .build());
    columns.add(
        JdbcColumn.builder()
            .withName("col_2")
            .withType(Types.ShortType.get())
            .withNullable(false)
            .build());
    columns.add(JdbcColumn.builder().withName("col_3").withType(INT).withNullable(true).build());
    columns.add(
        JdbcColumn.builder()
            .withName("col_4")
            .withType(Types.LongType.get())
            .withNullable(false)
            .build());
    columns.add(
        JdbcColumn.builder()
            .withName("col_5")
            .withType(Types.FloatType.get())
            .withNullable(false)
            .build());
    columns.add(
        JdbcColumn.builder()
            .withName("col_6")
            .withType(Types.DoubleType.get())
            .withNullable(false)
            .build());
    columns.add(
        JdbcColumn.builder()
            .withName("col_7")
            .withType(Types.DateType.get())
            .withNullable(false)
            .build());
    columns.add(
        JdbcColumn.builder()
            .withName("col_8")
            .withType(Types.TimeType.of(6))
            .withNullable(false)
            .build());
    columns.add(
        JdbcColumn.builder()
            .withName("col_9")
            .withType(Types.TimestampType.withoutTimeZone(6))
            .withNullable(false)
            .build());
    columns.add(
        JdbcColumn.builder()
            .withName("col_10")
            .withType(Types.TimestampType.withTimeZone(6))
            .withNullable(false)
            .build());
    columns.add(
        JdbcColumn.builder()
            .withName("col_11")
            .withType(Types.DecimalType.of(10, 2))
            .withNullable(false)
            .build());
    columns.add(
        JdbcColumn.builder().withName("col_12").withType(VARCHAR).withNullable(false).build());
    columns.add(
        JdbcColumn.builder()
            .withName("col_13")
            .withType(Types.FixedCharType.of(10))
            .withNullable(false)
            .build());
    columns.add(
        JdbcColumn.builder()
            .withName("col_14")
            .withType(Types.StringType.get())
            .withNullable(false)
            .build());
    columns.add(
        JdbcColumn.builder()
            .withName("col_15")
            .withType(Types.BinaryType.get())
            .withNullable(false)
            .build());
    columns.add(
        JdbcColumn.builder()
            .withName("col_16")
            .withType(Types.ListType.of(Types.IntegerType.get(), true))
            .withNullable(true)
            .build());

    // create table
    TABLE_OPERATIONS.create(
        TEST_DB_NAME,
        tableName,
        columns.toArray(new JdbcColumn[0]),
        tableComment,
        Collections.emptyMap(),
        null,
        Distributions.NONE,
        Indexes.EMPTY_INDEXES);

    JdbcTable load = TABLE_OPERATIONS.load(TEST_DB_NAME, tableName);
    assertionsTableInfo(
        tableName,
        tableComment,
        columns,
        Collections.emptyMap(),
        null,
        Transforms.EMPTY_TRANSFORM,
        load);
  }

  @Test
  public void testCreateMultipleTable() throws SQLException {
    String testDbName = RandomNameUtils.genRandomName("test_db_");
    try (Connection connection = DATA_SOURCE.getConnection()) {
      JdbcConnectorUtils.executeUpdate(connection, "CREATE DATABASE " + testDbName);
    }
    HashMap<String, String> properties = Maps.newHashMap();
    properties.put(JdbcConfig.JDBC_DRIVER.getKey(), CONTAINER.getDriverClassName());
    String jdbcUrl =
        StringUtils.substring(CONTAINER.getJdbcUrl(), 0, CONTAINER.getJdbcUrl().lastIndexOf("/"));
    properties.put(JdbcConfig.JDBC_URL.getKey(), jdbcUrl + "/" + testDbName);
    properties.put(JdbcConfig.USERNAME.getKey(), CONTAINER.getUsername());
    properties.put(JdbcConfig.PASSWORD.getKey(), CONTAINER.getPassword());
    DataSource dataSource = DataSourceUtils.createDataSource(properties);
    PostgreSqlSchemaOperations postgreSqlSchemaOperations = new PostgreSqlSchemaOperations();
    Map<String, String> config =
        new HashMap<String, String>() {
          {
            put(JdbcConfig.JDBC_DATABASE.getKey(), testDbName);
          }
        };
    postgreSqlSchemaOperations.initialize(dataSource, JDBC_EXCEPTION_CONVERTER, config);
    postgreSqlSchemaOperations.create(TEST_DB_NAME, null, null);

    PostgreSqlTableOperations postgreSqlTableOperations = new PostgreSqlTableOperations();

    postgreSqlTableOperations.initialize(
        dataSource,
        JDBC_EXCEPTION_CONVERTER,
        new PostgreSqlTypeConverter(),
        new PostgreSqlColumnDefaultValueConverter(),
        config);
    postgreSqlTableOperations.setDatabaseOperation(postgreSqlSchemaOperations);

    String table_1 = "table_multiple_1";
    postgreSqlTableOperations.create(
        TEST_DB_NAME,
        table_1,
        new JdbcColumn[] {
          JdbcColumn.builder()
              .withName("col_1")
              .withType(VARCHAR)
              .withComment("test_comment_col1")
              .withNullable(true)
              .build()
        },
        null,
        null,
        null,
        Distributions.NONE,
        Indexes.EMPTY_INDEXES);

    List<String> tableNames = TABLE_OPERATIONS.listTables(TEST_DB_NAME);
    Assertions.assertFalse(tableNames.contains(table_1));

    Assertions.assertThrows(
        NoSuchTableException.class, () -> TABLE_OPERATIONS.load(TEST_DB_NAME, table_1));

    Assertions.assertThrows(
        NoSuchTableException.class, () -> TABLE_OPERATIONS.load("other_schema", table_1));
    Assertions.assertThrows(
        NoSuchTableException.class, () -> postgreSqlTableOperations.load("other_schema", table_1));

    String table_2 = "table_multiple_2";
    TABLE_OPERATIONS.create(
        TEST_DB_NAME,
        table_2,
        new JdbcColumn[] {
          JdbcColumn.builder()
              .withName("col_1")
              .withType(VARCHAR)
              .withComment("test_comment_col1")
              .withNullable(true)
              .build()
        },
        null,
        null,
        null,
        Distributions.NONE,
        Indexes.EMPTY_INDEXES);
    tableNames = postgreSqlTableOperations.listTables(TEST_DB_NAME);
    Assertions.assertFalse(tableNames.contains(table_2));

    Assertions.assertThrows(
        NoSuchTableException.class, () -> postgreSqlTableOperations.load(TEST_DB_NAME, table_2));
    Assertions.assertThrows(
        NoSuchTableException.class, () -> postgreSqlTableOperations.load("other_schema", table_2));
    Assertions.assertThrows(
        NoSuchTableException.class, () -> TABLE_OPERATIONS.load("other_schema", table_2));
    postgreSqlTableOperations.drop(TEST_DB_NAME, table_1);

    Assertions.assertThrows(
        NoSuchTableException.class,
        () -> {
          postgreSqlTableOperations.load(TEST_DB_NAME, table_1);
        });

    JdbcTable load = TABLE_OPERATIONS.load(TEST_DB_NAME, table_2);
    Assertions.assertEquals(table_2, load.name());
  }

  @Test
  public void testCreateAutoIncrementTable() {
    String tableName = RandomNameUtils.genRandomName("increment_table_");
    String tableComment = "test_comment";
    List<JdbcColumn> columns = new ArrayList<>();
    columns.add(
        JdbcColumn.builder()
            .withName("col_1")
            .withType(Types.LongType.get())
            .withComment("increment key")
            .withNullable(false)
            .withAutoIncrement(true)
            .build());
    columns.add(
        JdbcColumn.builder()
            .withName("col_2")
            .withType(INT)
            .withNullable(false)
            .withComment("set test key")
            .build());
    Map<String, String> properties = new HashMap<>();
    // create table
    TABLE_OPERATIONS.create(
        TEST_DB_NAME,
        tableName,
        columns.toArray(new JdbcColumn[0]),
        tableComment,
        properties,
        null,
        Distributions.NONE,
        Indexes.EMPTY_INDEXES);

    // list table
    List<String> tables = TABLE_OPERATIONS.listTables(TEST_DB_NAME);
    Assertions.assertTrue(tables.contains(tableName));

    // load table
    JdbcTable load = TABLE_OPERATIONS.load(TEST_DB_NAME, tableName);
    assertionsTableInfo(
        tableName, tableComment, columns, properties, null, Transforms.EMPTY_TRANSFORM, load);

    columns.clear();
    columns.add(
        JdbcColumn.builder()
            .withName("col_1")
            .withType(Types.BooleanType.get())
            .withComment("increment key")
            .withNullable(false)
            .withAutoIncrement(true)
            .build());
    columns.add(
        JdbcColumn.builder()
            .withName("col_2")
            .withType(INT)
            .withNullable(false)
            .withComment("set test key")
            .build());

    // Boolean columns remain unsupported; smallint is covered by the native round-trip tests.
    String randomName = RandomNameUtils.genRandomName("increment_table_");
    JdbcColumn[] jdbcColumns = columns.toArray(new JdbcColumn[0]);
    IllegalArgumentException illegalArgumentException =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> {
              TABLE_OPERATIONS.create(
                  TEST_DB_NAME,
                  randomName,
                  jdbcColumns,
                  tableComment,
                  properties,
                  null,
                  Distributions.NONE,
                  Indexes.EMPTY_INDEXES);
            });

    Assertions.assertTrue(
        StringUtils.contains(illegalArgumentException.getMessage(), "Unsupported auto-increment"));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            TABLE_OPERATIONS.alterTable(
                TEST_DB_NAME,
                tableName,
                TableChange.addColumn(
                    new String[] {"invalid_identity"},
                    Types.BooleanType.get(),
                    null,
                    TableChange.ColumnPosition.defaultPos(),
                    false,
                    true,
                    Column.DEFAULT_VALUE_NOT_SET)));
    Assertions.assertEquals(2, TABLE_OPERATIONS.load(TEST_DB_NAME, tableName).columns().length);
  }

  @Test
  public void testCreateIndexTable() {
    String tableName = RandomNameUtils.genRandomName("index_table_");
    String tableComment = "test_comment";
    List<JdbcColumn> columns = new ArrayList<>();
    columns.add(
        JdbcColumn.builder()
            .withName("col_1")
            .withType(Types.LongType.get())
            .withComment("increment key")
            .withNullable(false)
            .withAutoIncrement(true)
            .build());
    columns.add(
        JdbcColumn.builder()
            .withName("col_2")
            .withType(INT)
            .withNullable(false)
            .withComment("id-1")
            .build());
    columns.add(
        JdbcColumn.builder()
            .withName("col_3")
            .withType(VARCHAR)
            .withNullable(false)
            .withComment("name")
            .build());
    columns.add(
        JdbcColumn.builder()
            .withName("col_4")
            .withType(VARCHAR)
            .withNullable(false)
            .withComment("city")
            .build());
    Map<String, String> properties = new HashMap<>();

    Index[] indexes =
        new Index[] {
          Indexes.primary("test_pk", new String[][] {{"col_1"}, {"col_2"}}),
          Indexes.unique("u1_key", new String[][] {{"col_2"}, {"col_3"}}),
          Indexes.unique("u2_key", new String[][] {{"col_3"}, {"col_4"}})
        };

    // Test create table index success.
    TABLE_OPERATIONS.create(
        TEST_DB_NAME,
        tableName,
        columns.toArray(new JdbcColumn[0]),
        tableComment,
        properties,
        null,
        Distributions.NONE,
        indexes);

    JdbcTable load = TABLE_OPERATIONS.load(TEST_DB_NAME, tableName);
    assertionsTableInfo(
        tableName, tableComment, columns, properties, indexes, Transforms.EMPTY_TRANSFORM, load);

    TABLE_OPERATIONS.drop(TEST_DB_NAME, tableName);

    // Test create table index failed.
    JdbcColumn[] jdbcColumns = columns.toArray(new JdbcColumn[0]);
    Index[] primaryIndex =
        new Index[] {
          Indexes.primary("no_exist_pk", new String[][] {{"no_exist_1"}}),
          Indexes.unique("no_exist_key", new String[][] {{"no_exist_2"}, {"no_exist_3"}})
        };
    GravitinoRuntimeException gravitinoRuntimeException =
        Assertions.assertThrows(
            GravitinoRuntimeException.class,
            () -> {
              TABLE_OPERATIONS.create(
                  TEST_DB_NAME,
                  tableName,
                  jdbcColumns,
                  tableComment,
                  properties,
                  null,
                  Distributions.NONE,
                  primaryIndex);
            });
    Assertions.assertTrue(
        StringUtils.contains(
            gravitinoRuntimeException.getMessage(),
            "column \"no_exist_1\" named in key does not exist"));
  }

  @Test
  public void testAppendIndexesSql() {
    // Test append index sql success.
    Index[] indexes =
        new Index[] {
          Indexes.primary("test_pk", new String[][] {{"col_1"}, {"col_2"}}),
          Indexes.unique("u1_key", new String[][] {{"col_2"}, {"col_3"}}),
          Indexes.unique("u2_key", new String[][] {{"col_3"}, {"col_4"}})
        };
    StringBuilder successBuilder = new StringBuilder();
    PostgreSqlTableOperations.appendIndexesSql(indexes, successBuilder);
    Assertions.assertEquals(
        ",\n"
            + "CONSTRAINT \"test_pk\" PRIMARY KEY (\"col_1\", \"col_2\"),\n"
            + "CONSTRAINT \"u1_key\" UNIQUE (\"col_2\", \"col_3\"),\n"
            + "CONSTRAINT \"u2_key\" UNIQUE (\"col_3\", \"col_4\")",
        successBuilder.toString());

    // Test append index sql not have name.
    indexes =
        new Index[] {
          Indexes.primary(null, new String[][] {{"col_1"}, {"col_2"}}),
          Indexes.unique(null, new String[][] {{"col_2"}, {"col_3"}}),
          Indexes.unique(null, new String[][] {{"col_3"}, {"col_4"}})
        };
    successBuilder = new StringBuilder();
    PostgreSqlTableOperations.appendIndexesSql(indexes, successBuilder);
    Assertions.assertEquals(
        ",\n"
            + " PRIMARY KEY (\"col_1\", \"col_2\"),\n"
            + " UNIQUE (\"col_2\", \"col_3\"),\n"
            + " UNIQUE (\"col_3\", \"col_4\")",
        successBuilder.toString());

    // Test append index sql failed.
    Index primary = Indexes.primary("test_pk", new String[][] {{"col_1", "col_2"}});
    Index unique1 = Indexes.unique("u1_key", new String[][] {{"col_2", "col_3"}});
    Index unique2 = Indexes.unique("u2_key", new String[][] {{"col_3", "col_4"}});
    StringBuilder stringBuilder = new StringBuilder();

    IllegalArgumentException illegalArgumentException =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                PostgreSqlTableOperations.appendIndexesSql(
                    new Index[] {primary, unique1, unique2}, stringBuilder));
    Assertions.assertTrue(
        StringUtils.contains(
            illegalArgumentException.getMessage(),
            "Index does not support complex fields in PostgreSQL"));
  }

  @Test
  public void testOperationIndexDefinition() {
    String tableName = "t1";
    // Test add index definition success.
    TableChange.AddIndex addIndex =
        new TableChange.AddIndex(
            Index.IndexType.PRIMARY_KEY, "test_pk", new String[][] {{"col_1"}});
    String result = PostgreSqlTableOperations.addIndexDefinition(tableName, addIndex);
    Assertions.assertEquals(
        "ALTER TABLE \"t1\" ADD CONSTRAINT \"test_pk\" PRIMARY KEY (\"col_1\");", result);

    addIndex =
        new TableChange.AddIndex(
            Index.IndexType.UNIQUE_KEY, "test_uk", new String[][] {{"col_1"}, {"col_2"}});
    result = PostgreSqlTableOperations.addIndexDefinition(tableName, addIndex);
    Assertions.assertEquals(
        "ALTER TABLE \"t1\" ADD CONSTRAINT \"test_uk\" UNIQUE (\"col_1\", \"col_2\");", result);

    // Test delete index definition.
    TableChange.DeleteIndex deleteIndex = new TableChange.DeleteIndex("test_pk", false);
    result = PostgreSqlTableOperations.deleteIndexDefinition(tableName, deleteIndex);
    Assertions.assertEquals("ALTER TABLE \"t1\" DROP CONSTRAINT \"test_pk\";", result);

    deleteIndex = new TableChange.DeleteIndex("test_2_pk", true);
    result = PostgreSqlTableOperations.deleteIndexDefinition(tableName, deleteIndex);
    Assertions.assertEquals("ALTER TABLE \"t1\" DROP CONSTRAINT IF EXISTS \"test_2_pk\";", result);
  }

  @Test
  void testUpdateColumnAutoIncrementDefinition() {
    TableChange.UpdateColumnAutoIncrement updateColumnAutoIncrement =
        new TableChange.UpdateColumnAutoIncrement(new String[] {"col_1"}, true);
    String sql =
        PostgreSqlTableOperations.updateColumnAutoIncrementDefinition(
            updateColumnAutoIncrement, "table_test");
    Assertions.assertEquals(
        "ALTER TABLE \"table_test\" ALTER COLUMN  \"col_1\" ADD GENERATED BY DEFAULT AS IDENTITY;",
        sql);

    updateColumnAutoIncrement =
        new TableChange.UpdateColumnAutoIncrement(new String[] {"col_2"}, false);
    sql =
        PostgreSqlTableOperations.updateColumnAutoIncrementDefinition(
            updateColumnAutoIncrement, "table_test");
    Assertions.assertEquals(
        "ALTER TABLE \"table_test\" ALTER COLUMN  \"col_2\" DROP IDENTITY;", sql);
  }

  @Test
  public void testCalculateDatetimePrecision() {
    Assertions.assertNull(
        TABLE_OPERATIONS.calculateDatetimePrecision("DATE", 10, -1),
        "DATE type should return 0 precision");

    Assertions.assertEquals(
        0,
        TABLE_OPERATIONS.calculateDatetimePrecision("TIME", 10, 0),
        "TIME type should return 0 precision");

    Assertions.assertEquals(
        3,
        TABLE_OPERATIONS.calculateDatetimePrecision("TIMETZ", 23, 3),
        "TIMESTAMP type should return 0 precision");

    Assertions.assertEquals(
        6,
        TABLE_OPERATIONS.calculateDatetimePrecision("TIMESTAMP", 26, 6),
        "TIMESTAMP type should return 0 precision");

    Assertions.assertEquals(
        1,
        TABLE_OPERATIONS.calculateDatetimePrecision("TIMESTAMPTZ", 19, 1),
        "Lower case type name should work");

    Assertions.assertNull(
        TABLE_OPERATIONS.calculateDatetimePrecision("VARCHAR", 50, 0),
        "Non-datetime type should return 0 precision");
  }

  @ParameterizedTest
  @ValueSource(strings = {"O'Brien", "quote\\' and \\new\\path; \"double\"", "ends with \\"})
  void testStringDefaultValueRoundTrips(String defaultValue) {
    String tableName = RandomNameUtils.genRandomName("pg_default_round_trip");
    JdbcColumn column =
        JdbcColumn.builder()
            .withName("col1")
            .withType(VARCHAR)
            .withNullable(true)
            .withDefaultValue(Literals.varcharLiteral(255, defaultValue))
            .build();
    TABLE_OPERATIONS.create(
        TEST_DB_NAME,
        tableName,
        new JdbcColumn[] {column},
        null,
        Collections.emptyMap(),
        Transforms.EMPTY_TRANSFORM,
        Distributions.NONE,
        Indexes.EMPTY_INDEXES);
    try {
      JdbcTable table = TABLE_OPERATIONS.load(TEST_DB_NAME, tableName);
      Assertions.assertEquals(
          Literals.varcharLiteral(255, defaultValue), table.columns()[0].defaultValue());

      TABLE_OPERATIONS.alterTable(
          TEST_DB_NAME,
          tableName,
          TableChange.addColumn(
              new String[] {"col2"},
              VARCHAR,
              null,
              TableChange.ColumnPosition.defaultPos(),
              true,
              false,
              Literals.varcharLiteral(255, defaultValue)));
      table = TABLE_OPERATIONS.load(TEST_DB_NAME, tableName);
      Assertions.assertEquals(
          Literals.varcharLiteral(255, defaultValue), table.columns()[1].defaultValue());

      String updated = defaultValue + " updated";
      TABLE_OPERATIONS.alterTable(
          TEST_DB_NAME,
          tableName,
          TableChange.updateColumnDefaultValue(
              new String[] {"col1"}, Literals.varcharLiteral(255, updated)));
      table = TABLE_OPERATIONS.load(TEST_DB_NAME, tableName);
      Assertions.assertEquals(
          Literals.varcharLiteral(255, updated), table.columns()[0].defaultValue());
    } finally {
      TABLE_OPERATIONS.drop(TEST_DB_NAME, tableName);
    }
  }
}
