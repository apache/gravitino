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
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.types.Types;
import org.apache.gravitino.spark.connector.SparkTransformConverter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCScanBuilder;
import org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCTable;
import org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCTableCatalog;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

/**
 * Live Spark scans with the NTZ option on keep string, array, binary, and timestamp semantics,
 * while safe expressions are still pushed down.
 */
@Tag("gravitino-docker-test")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestSparkJdbcReadCorrectness {

  private static final String[] ZONES = {"UTC", "America/Los_Angeles", "Asia/Shanghai"};

  private GenericContainer<?> pg;
  private SparkSession spark;

  /** Minimal catalog that installs the production JDBC table wrapper. */
  @SuppressWarnings("overrides")
  public static class SafeCatalog extends JDBCTableCatalog {
    private boolean useTimestampNtz;

    /** {@inheritDoc} */
    @Override
    public void initialize(String name, CaseInsensitiveStringMap options) {
      super.initialize(name, options);
      useTimestampNtz = Boolean.parseBoolean(options.get("preferTimestampNTZ"));
    }

    /** {@inheritDoc} */
    @Override
    public JDBCTable loadTable(Identifier identifier) {
      JDBCTable physical = (JDBCTable) super.loadTable(identifier);
      Table metadata = mock(Table.class);
      when(metadata.columns()).thenReturn(columns(identifier.name()));
      return new SparkJdbcTable(
          identifier,
          metadata,
          physical,
          this,
          JdbcPropertiesConverter.getInstance(),
          new SparkTransformConverter(false),
          new SparkJdbcTypeConverter(useTimestampNtz));
    }

    private static Column[] columns(String table) {
      switch (table) {
        case "timestamp_only":
          return new Column[] {
            Column.of("id", Types.IntegerType.get()),
            Column.of("ts", Types.TimestampType.withoutTimeZone())
          };
        case "zoned_only":
          return new Column[] {
            Column.of("id", Types.IntegerType.get()),
            Column.of("tz", Types.TimestampType.withTimeZone())
          };
        default:
          return new Column[] {
            Column.of("id", Types.IntegerType.get()),
            Column.of("s", Types.StringType.get()),
            Column.of("b", Types.BinaryType.get()),
            Column.of("ts", Types.TimestampType.withoutTimeZone()),
            Column.of("tz", Types.TimestampType.withTimeZone()),
            Column.of("tags", Types.ListType.nullable(Types.StringType.get())),
            Column.of("tss", Types.ListType.nullable(Types.TimestampType.withoutTimeZone()))
          };
      }
    }
  }

  @BeforeAll
  void startPostgreSql() throws Exception {
    pg =
        new GenericContainer<>("postgres:16")
            .withEnv("POSTGRES_DB", "test")
            .withEnv("POSTGRES_PASSWORD", "test")
            .withExposedPorts(5432)
            .waitingFor(
                Wait.forLogMessage(".*database system is ready to accept connections.*\\n", 2));
    pg.start();
    String url = "jdbc:postgresql://" + pg.getHost() + ":" + pg.getMappedPort(5432) + "/test";
    try (Connection connection = DriverManager.getConnection(url, "postgres", "test");
        Statement statement = connection.createStatement()) {
      statement.execute(
          "CREATE TABLE samples(id int, s text COLLATE \"en-US-x-icu\", b bytea, ts timestamp,"
              + " tz timestamptz, tags text[] COLLATE \"en-US-x-icu\", tss timestamp[])");
      statement.execute(
          "INSERT INTO samples VALUES"
              + " (1, 'a', decode('01','hex'), '2026-10-31 12:34:56.123456',"
              + " '2026-10-31 12:34:56.123456+00', '{a}', '{\"2024-01-02 03:04:05\"}'),"
              + " (2, 'B', decode('02','hex'), '0001-01-01 00:00:00',"
              + " '2026-10-31 12:34:56.123456+00', '{B}', null),"
              + " (3, '_x', decode('01','hex'), null, null, '{_x}', null)");
      statement.execute("CREATE TABLE timestamp_only(id int, ts timestamp)");
      statement.execute(
          "INSERT INTO timestamp_only VALUES (1, '2024-01-01 03:04:05'), (2, '2024-01-03 03:04:05')");
      statement.execute("CREATE TABLE zoned_only(id int, tz timestamptz)");
      statement.execute(
          "INSERT INTO zoned_only VALUES (1, '2024-01-01 00:00:00+00'), (2, '2024-01-02 00:00:00+00')");
    }
    spark =
        SparkSession.builder()
            .master("local[1]")
            .appName("jdbc-read-regression")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.catalog.legacy", SafeCatalog.class.getName())
            .config("spark.sql.catalog.legacy.url", url)
            .config("spark.sql.catalog.legacy.user", "postgres")
            .config("spark.sql.catalog.legacy.password", "test")
            .config("spark.sql.catalog.legacy.driver", "org.postgresql.Driver")
            .config("spark.sql.catalog.legacy.preferTimestampNTZ", "false")
            .config("spark.sql.catalog.safe", SafeCatalog.class.getName())
            .config("spark.sql.catalog.safe.url", url)
            .config("spark.sql.catalog.safe.user", "postgres")
            .config("spark.sql.catalog.safe.password", "test")
            .config("spark.sql.catalog.safe.driver", "org.postgresql.Driver")
            .config("spark.sql.catalog.safe.preferTimestampNTZ", "true")
            .config("spark.sql.catalog.safe.pushDownPredicate", "true")
            .config("spark.sql.catalog.safe.pushDownAggregate", "true")
            .config("spark.sql.catalog.safe.pushDownLimit", "true")
            .config("spark.sql.catalog.safe.pushDownOffset", "true")
            .getOrCreate();
  }

  @AfterAll
  void stop() {
    if (spark != null) {
      spark.stop();
    }
    if (pg != null) {
      pg.stop();
    }
  }

  @Test
  void testSensitiveExpressionsKeepSparkSemantics() {
    for (String zone : ZONES) {
      spark.conf().set("spark.sql.session.timeZone", zone);
      Assertions.assertEquals(
          1793450096123456L,
          spark
              .sql("SELECT unix_micros(tz) FROM safe.public.samples WHERE id=1")
              .head()
              .getLong(0));
      Row aggregate = spark.sql("SELECT min(s),max(s) FROM safe.public.samples").head();
      Assertions.assertEquals("B", aggregate.getString(0));
      Assertions.assertEquals("a", aggregate.getString(1));
      Assertions.assertEquals(
          "B",
          spark.sql("SELECT s FROM safe.public.samples ORDER BY s LIMIT 1").head().getString(0));
      Assertions.assertEquals(
          List.of(1, 3), ids("SELECT id FROM safe.public.samples WHERE b = X'01' ORDER BY id"));
      // Arrays compare with the element collation in PostgreSQL, so they also stay in Spark.
      Assertions.assertEquals(
          List.of(2), ids("SELECT id FROM safe.public.samples ORDER BY tags LIMIT 1"));
      Assertions.assertEquals(
          "[a]",
          spark.sql("SELECT cast(max(tags) AS string) FROM safe.public.samples").head().get(0));
      Assertions.assertEquals(
          "2026-10-31 12:34:56.123456",
          spark
              .sql("SELECT cast(ts as string) FROM safe.public.samples WHERE id=1")
              .head()
              .getString(0));
      Assertions.assertEquals(
          "0001-01-01 00:00:00",
          spark
              .sql("SELECT cast(ts as string) FROM safe.public.samples WHERE id=2")
              .head()
              .getString(0));
      // PostgreSQL EXTRACT(SECOND) keeps fractions, while Spark second() does not.
      Assertions.assertEquals(
          List.of(1), ids("SELECT id FROM safe.public.samples WHERE second(ts) = 56"));
      // Spark 3.5 reads timestamp array elements as TIMESTAMP; reading them must not fail.
      Assertions.assertEquals(
          1, spark.sql("SELECT tss FROM safe.public.samples WHERE id=1").head().getList(0).size());
    }
    String stringFilter = pushedFilters("SELECT id FROM safe.public.samples WHERE s = 'a'");
    Assertions.assertTrue(stringFilter.contains("s ="), stringFilter);
    String stringFilterPlan = plan("SELECT id FROM safe.public.samples WHERE s = 'a'");
    Assertions.assertTrue(stringFilterPlan.contains("Filter "), stringFilterPlan);
    // A filter kept in Spark also keeps the limit and the aggregate of the query in Spark.
    String limitAfterStringFilter =
        plan("SELECT id FROM safe.public.samples WHERE s = 'a' LIMIT 1");
    Assertions.assertFalse(limitAfterStringFilter.contains("PushedLimit"), limitAfterStringFilter);
    String countAfterStringFilter = plan("SELECT count(*) FROM safe.public.samples WHERE s = 'a'");
    Assertions.assertFalse(
        countAfterStringFilter.contains("PushedAggregates: [COUNT"), countAfterStringFilter);
    Assertions.assertEquals(
        1L, spark.sql("SELECT count(*) FROM safe.public.samples WHERE s = 'a'").head().getLong(0));
    String stringTopN = plan("SELECT s FROM safe.public.samples ORDER BY s LIMIT 1");
    Assertions.assertFalse(stringTopN.contains("PushedTopN"), stringTopN);
  }

  @Test
  void testSafeExpressionsArePushedOnTablesWithStringColumns() {
    String filter = pushedFilters("SELECT id FROM safe.public.samples WHERE id = 1");
    Assertions.assertTrue(filter.contains("id = 1"), filter);
    String limit = plan("SELECT id FROM safe.public.samples LIMIT 2");
    Assertions.assertTrue(limit.contains("PushedLimit: LIMIT 2"), limit);
    String count = plan("SELECT count(*) FROM safe.public.samples WHERE id > 0");
    Assertions.assertTrue(count.contains("PushedAggregates: [COUNT(*)]"), count);
    String topN = plan("SELECT id FROM safe.public.samples ORDER BY id LIMIT 2");
    Assertions.assertTrue(topN.contains("PushedTopN"), topN);
    Assertions.assertEquals(
        List.of(1, 2), ids("SELECT id FROM safe.public.samples ORDER BY id LIMIT 2"));
    Assertions.assertEquals(
        3L, spark.sql("SELECT count(*) FROM safe.public.samples WHERE id > 0").head().getLong(0));
  }

  @Test
  void testTimestampFilterOnTableWithoutStringColumns() {
    boolean ntzLiteralSupported = !spark.version().startsWith("3.5.");
    String query =
        "SELECT id FROM safe.public.timestamp_only"
            + " WHERE ts > TIMESTAMP_NTZ '2024-01-02 03:04:05.123456'";
    String filter = pushedFilters(query);
    Assertions.assertEquals(ntzLiteralSupported, filter.contains("ts >"), filter);
    Assertions.assertTrue(
        pushedFilters("SELECT id FROM safe.public.timestamp_only WHERE id = 1").contains("id = 1"));
    for (String zone : ZONES) {
      spark.conf().set("spark.sql.session.timeZone", zone);
      Assertions.assertEquals(List.of(2), ids(query));
      // A TIMESTAMP literal casts the NTZ column in the session time zone; it must stay in Spark.
      // The literal is 3 hours before row 2, so a pushed cast in another time zone drops it.
      Assertions.assertEquals(
          List.of(2),
          ids(
              "SELECT id FROM safe.public.timestamp_only"
                  + " WHERE ts > TIMESTAMP '2024-01-03 00:00:00'"));
      Assertions.assertEquals(
          "2024-01-03 03:04:05",
          spark
              .sql("SELECT cast(max(ts) AS string) FROM safe.public.timestamp_only")
              .head()
              .getString(0));
    }
  }

  @Test
  void testZonedTimestampTableKeepsPushdownAndAggregateTypes() {
    // On Spark 3.5 the reader sees timestamptz as NTZ while Spark sees TIMESTAMP.
    Assertions.assertTrue(
        pushedFilters("SELECT id FROM safe.public.zoned_only WHERE id = 1").contains("id = 1"));
    for (String zone : ZONES) {
      spark.conf().set("spark.sql.session.timeZone", zone);
      Assertions.assertEquals(
          1704153600000000L,
          spark.sql("SELECT unix_micros(max(tz)) FROM safe.public.zoned_only").head().getLong(0));
      // Date parts and casts of a TIMESTAMP column use the session time zone in Spark.
      boolean utc = zone.equals("UTC");
      Assertions.assertEquals(
          utc ? List.of(1, 2) : List.of(),
          ids("SELECT id FROM safe.public.zoned_only WHERE hour(tz) = 0 ORDER BY id"));
      Assertions.assertEquals(
          zone.equals("America/Los_Angeles") ? List.of(-1, 0) : List.of(0, 1),
          ids(
              "SELECT datediff(d, DATE '2024-01-01') FROM (SELECT cast(tz AS date) AS d, count(*)"
                  + " FROM safe.public.zoned_only GROUP BY 1) ORDER BY 1"));
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"safe", "legacy"})
  void testPerReadTimestampOptionCannotOverrideCatalog(String name) {
    TimeZone original = TimeZone.getDefault();
    String originalSessionZone = spark.conf().get("spark.sql.session.timeZone");
    try {
      TimeZone.setDefault(TimeZone.getTimeZone("Asia/Shanghai"));
      spark.conf().set("spark.sql.session.timeZone", "UTC");
      boolean ntz = name.equals("safe");
      for (String key :
          new String[] {"preferTimestampNTZ", "prefertimestampntz", "PrEfErTiMeStAmPnTz"}) {
        CaseInsensitiveStringMap options =
            new CaseInsensitiveStringMap(
                Map.of(key, Boolean.toString(!ntz), "pushDownLimit", "false"));
        Map<String, String> originalOptions = Map.copyOf(options.asCaseSensitiveMap());
        JDBCTableCatalog catalog =
            (JDBCTableCatalog) spark.sessionState().catalogManager().catalog(name);
        SparkJdbcTable table =
            (SparkJdbcTable)
                catalog.loadTable(Identifier.of(new String[] {"public"}, "timestamp_only"));
        JDBCScanBuilder builder = table.newScanBuilder(options);
        Assertions.assertEquals(ntz, builder.jdbcOptions().preferTimestampNTZ());
        Assertions.assertFalse(builder.jdbcOptions().pushDownLimit());
        Assertions.assertEquals(originalOptions, options.asCaseSensitiveMap());
        Dataset<Row> read =
            spark.read().option(key, Boolean.toString(!ntz)).table(name + ".public.timestamp_only");
        String expected =
            read.select("ts").collectAsList().stream()
                .map(row -> row.get(0).toString())
                .max(String::compareTo)
                .get();
        Assertions.assertEquals(expected, read.agg(functions.max("ts")).head().get(0).toString());
      }
    } finally {
      spark.conf().set("spark.sql.session.timeZone", originalSessionZone);
      TimeZone.setDefault(original);
    }
  }

  private List<Integer> ids(String sql) {
    List<Integer> ids = new ArrayList<>();
    for (Row row : spark.sql(sql).collectAsList()) {
      ids.add(row.getInt(0));
    }
    return ids;
  }

  private String pushedFilters(String sql) {
    String plan = plan(sql);
    Matcher matcher = Pattern.compile("PushedFilters: \\[([^\\]]*)\\]").matcher(plan);
    Assertions.assertTrue(matcher.find(), plan);
    return matcher.group(1);
  }

  private String plan(String sql) {
    Dataset<Row> dataset = spark.sql(sql);
    return dataset.queryExecution().executedPlan().toString();
  }
}
