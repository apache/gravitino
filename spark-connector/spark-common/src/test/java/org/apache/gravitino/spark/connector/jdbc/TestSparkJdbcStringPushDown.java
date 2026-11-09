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

package org.apache.gravitino.spark.connector.jdbc;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.types.Types;
import org.apache.gravitino.spark.connector.SparkTransformConverter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.expressions.Expression;
import org.apache.spark.sql.connector.expressions.Expressions;
import org.apache.spark.sql.connector.expressions.LiteralValue;
import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCTable;
import org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCTableCatalog;
import org.apache.spark.sql.jdbc.JdbcDialects;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

/** PostgreSQL and MySQL string prefilters keep Spark semantics and reduce source rows. */
@Tag("gravitino-docker-test")
public class TestSparkJdbcStringPushDown {

  /** A real JDBC reader with the production Gravitino table wrapper. */
  @SuppressWarnings("overrides")
  public static class StringCatalog extends JDBCTableCatalog {
    /** {@inheritDoc} */
    @Override
    public JDBCTable loadTable(Identifier identifier) {
      JDBCTable physical = (JDBCTable) super.loadTable(identifier);
      Table metadata = mock(Table.class);
      when(metadata.columns())
          .thenReturn(
              identifier.name().equals("composites")
                  ? new Column[] {
                    Column.of("id", Types.IntegerType.get()),
                    Column.of("p", Types.ExternalType.of("pair"))
                  }
                  : new Column[] {
                    Column.of("id", Types.IntegerType.get()), Column.of("s", Types.StringType.get())
                  });
      return new SparkJdbcTable(
          identifier,
          metadata,
          physical,
          this,
          JdbcPropertiesConverter.getInstance(),
          new SparkTransformConverter(false),
          new SparkJdbcTypeConverter());
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"postgres", "mysql"})
  void testStringPredicatesKeepResidualsAndSafePushdown(String backend) throws Exception {
    boolean postgres = backend.equals("postgres");
    int port = postgres ? 5432 : 3306;
    String user = postgres ? "postgres" : "root";
    try (GenericContainer<?> database =
        postgres
            ? new GenericContainer<>("postgres:16")
                .withEnv("POSTGRES_DB", "test")
                .withEnv("POSTGRES_PASSWORD", "test")
                .withExposedPorts(port)
                .waitingFor(
                    Wait.forLogMessage(".*database system is ready to accept connections.*\\n", 2))
            : new GenericContainer<>("mysql:8.0")
                .withEnv("MYSQL_DATABASE", "test")
                .withEnv("MYSQL_ROOT_PASSWORD", "test")
                .withEnv("MYSQL_ROOT_HOST", "%")
                .withExposedPorts(port)
                .waitingFor(Wait.forLogMessage(".*mysqld: ready for connections.*\\n", 2))) {
      database.start();
      String url =
          "jdbc:"
              + (postgres ? "postgresql" : "mysql")
              + "://"
              + database.getHost()
              + ":"
              + database.getMappedPort(port)
              + "/test"
              + (postgres ? "" : "?allowPublicKeyRetrieval=true&useSSL=false");
      try (Connection connection = DriverManager.getConnection(url, user, "test");
          Statement statement = connection.createStatement()) {
        statement.execute(
            "CREATE TABLE strings(id int, s "
                + (postgres
                    ? "text COLLATE \"en-US-x-icu\""
                    : "varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci")
                + ")");
        if (postgres) {
          statement.execute("CREATE TYPE pair AS (x int, y int)");
          statement.execute("CREATE TABLE composites(id int, p pair)");
          statement.execute(
              "INSERT INTO composites VALUES (1, ROW(1,NULL)), (2, ROW(NULL,NULL)), (3, ROW(1,2)), (4, NULL)");
        }
        try (PreparedStatement insert =
            connection.prepareStatement("INSERT INTO strings VALUES (?, ?)")) {
          String[] values = {"v500", "a", "A", "a ", null, "C:\\temp", "abc\\", "O'Reilly"};
          for (int i = 0; i < values.length; i++) {
            insert.setInt(1, i + 1);
            insert.setString(2, values[i]);
            insert.executeUpdate();
          }
        }
      }
      try (SparkSession spark =
          SparkSession.builder()
              .master("local[1]")
              .appName("string-pushdown-regression")
              .config("spark.ui.enabled", "false")
              .config("spark.sql.catalog.strings", StringCatalog.class.getName())
              .config("spark.sql.catalog.strings.url", url)
              .config("spark.sql.catalog.strings.user", user)
              .config("spark.sql.catalog.strings.password", "test")
              .config(
                  "spark.sql.catalog.strings.driver",
                  postgres ? "org.postgresql.Driver" : "com.mysql.cj.jdbc.Driver")
              .getOrCreate()) {
        if (spark.version().startsWith("4.")) {
          assertNonBinaryCollationIsNotPrefiltered(url);
        }
        String table = "strings." + (postgres ? "public" : "test") + ".strings";
        if (postgres) {
          assertCompositeNullChecksStayInSpark(spark);
        }
        assertPrefilter(spark, table, "s = 'V500'", List.of());
        assertPrefilter(spark, table, "s = 'a'", List.of(2));
        assertPrefilter(spark, table, "s IN ('a', 'v500')", List.of(1, 2));
        assertPrefilter(spark, table, "s = \"O'Reilly\"", List.of(8));
        assertKept(spark, table, "NOT (s = 'V500')", List.of(1, 2, 3, 4, 6, 7, 8));
        assertKept(spark, table, "s NOT IN ('a', 'v500')", List.of(3, 4, 6, 7, 8));
        assertKept(spark, table, "s = r'C:\\temp'", List.of(6));
        assertKept(spark, table, "s = concat('abc', chr(92))", List.of(7));
        String nullCount = "SELECT count(*) FROM " + table + " WHERE s IS NULL";
        String nullPlan = plan(spark, nullCount);
        Assertions.assertTrue(nullPlan.contains("IS NULL"), nullPlan);
        Assertions.assertTrue(nullPlan.contains("PushedAggregates: [COUNT(*)]"), nullPlan);
        Assertions.assertFalse(nullPlan.contains("Filter "), nullPlan);
        Assertions.assertEquals(1L, spark.sql(nullCount).head().getLong(0));
        Assertions.assertEquals(
            7L,
            spark.sql("SELECT count(*) FROM " + table + " WHERE s IS NOT NULL").head().getLong(0));
        String columnCount = "SELECT count(s) FROM " + table;
        String columnCountPlan = plan(spark, columnCount);
        Assertions.assertTrue(
            columnCountPlan.contains("PushedAggregates: [COUNT(s)]"), columnCountPlan);
        Assertions.assertEquals(7L, spark.sql(columnCount).head().getLong(0));
        String distinctCount = "SELECT count(DISTINCT s) FROM " + table;
        String distinctPlan = plan(spark, distinctCount);
        Assertions.assertFalse(distinctPlan.contains("PushedAggregates: [COUNT"), distinctPlan);
        Assertions.assertEquals(7L, spark.sql(distinctCount).head().getLong(0));
        String limited = "SELECT id FROM " + table + " WHERE s = 'a' LIMIT 1";
        Assertions.assertFalse(plan(spark, limited).contains("PushedLimit"));
        Assertions.assertEquals(2, spark.sql(limited).head().getInt(0));
        String count = "SELECT count(*) FROM " + table + " WHERE s = 'a'";
        Assertions.assertFalse(plan(spark, count).contains("PushedAggregates: [COUNT"));
        Assertions.assertEquals(1L, spark.sql(count).head().getLong(0));
      }
    }
  }

  private static void assertCompositeNullChecksStayInSpark(SparkSession spark) {
    String table = "strings.public.composites";
    for (String predicate : new String[] {"p IS NULL", "p IS NOT NULL", "p = '(1,)'"}) {
      String sql = "SELECT id FROM " + table + " WHERE " + predicate + " ORDER BY id";
      String actualPlan = plan(spark, sql);
      Assertions.assertTrue(actualPlan.contains("Filter "), actualPlan);
      String pushed = actualPlan.substring(actualPlan.indexOf("PushedFilters:"));
      Assertions.assertFalse(
          pushed.contains("p IS NULL") || pushed.contains("p IS NOT NULL"), pushed);
      List<Integer> expected =
          predicate.equals("p IS NULL")
              ? List.of(4)
              : predicate.equals("p IS NOT NULL") ? List.of(1, 2, 3) : List.of(1);
      Assertions.assertEquals(
          expected,
          spark.sql(sql).collectAsList().stream()
              .map(row -> row.getInt(0))
              .collect(Collectors.toList()));
    }
    String count = "SELECT count(*) FROM " + table + " WHERE p IS NOT NULL";
    String countPlan = plan(spark, count);
    Assertions.assertFalse(countPlan.contains("PushedAggregates: [COUNT"), countPlan);
    Assertions.assertEquals(3L, spark.sql(count).head().getLong(0));
    String columnCount = "SELECT count(p) FROM " + table;
    String columnCountPlan = plan(spark, columnCount);
    Assertions.assertFalse(columnCountPlan.contains("PushedAggregates: [COUNT"), columnCountPlan);
    Assertions.assertEquals(3L, spark.sql(columnCount).head().getLong(0));
  }

  private static void assertNonBinaryCollationIsNotPrefiltered(String url) {
    DataType collated = DataType.fromDDL("STRING COLLATE UTF8_LCASE");
    Column[] columns = {Column.of("s", Types.StringType.get())};
    StructType binarySchema = new StructType().add("s", DataTypes.StringType);
    StructType collatedSchema = new StructType().add("s", collated);
    SparkJdbcPushDownPolicy collatedColumn =
        new SparkJdbcPushDownPolicy(
            columns, binarySchema, collatedSchema, JdbcDialects.get(url), url);
    Predicate equality =
        new Predicate(
            "=",
            new Expression[] {
              Expressions.column("s"),
              new LiteralValue<>(UTF8String.fromString("A"), DataTypes.StringType)
            });
    Assertions.assertFalse(collatedColumn.canPrefilter(equality));
    SparkJdbcPushDownPolicy binaryColumn =
        new SparkJdbcPushDownPolicy(
            columns, binarySchema, binarySchema, JdbcDialects.get(url), url);
    Predicate collatedLiteral =
        new Predicate(
            "=",
            new Expression[] {
              Expressions.column("s"), new LiteralValue<>(UTF8String.fromString("A"), collated)
            });
    Assertions.assertFalse(binaryColumn.canPrefilter(collatedLiteral));
  }

  private static void assertPrefilter(
      SparkSession spark, String table, String predicate, List<Integer> expected) {
    String sql = "SELECT id FROM " + table + " WHERE " + predicate + " ORDER BY id";
    String plan = plan(spark, sql);
    Assertions.assertTrue(plan.contains("Filter "), plan);
    String pushed = plan.substring(plan.indexOf("PushedFilters:"));
    Assertions.assertTrue(pushed.contains("s =") || pushed.contains("s IN"), pushed);
    Assertions.assertEquals(
        expected,
        spark.sql(sql).collectAsList().stream()
            .map(row -> row.getInt(0))
            .collect(Collectors.toList()));
  }

  private static void assertKept(
      SparkSession spark, String table, String predicate, List<Integer> expected) {
    String sql = "SELECT id FROM " + table + " WHERE " + predicate + " ORDER BY id";
    String plan = plan(spark, sql);
    Assertions.assertTrue(plan.contains("Filter "), plan);
    String pushed = plan.substring(plan.indexOf("PushedFilters:"));
    Assertions.assertFalse(
        pushed.contains("s =") || pushed.contains("s IN") || pushed.contains("NOT ("), pushed);
    Assertions.assertEquals(
        expected,
        spark.sql(sql).collectAsList().stream()
            .map(row -> row.getInt(0))
            .collect(Collectors.toList()));
  }

  private static String plan(SparkSession spark, String sql) {
    Dataset<Row> result = spark.sql(sql);
    return result.queryExecution().executedPlan().toString();
  }
}
