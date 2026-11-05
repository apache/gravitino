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
import java.util.List;
import java.util.Map;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.types.Types;
import org.apache.gravitino.spark.connector.SparkTransformConverter;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCTable;
import org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCTableCatalog;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

/** Live Spark scans preserve string, binary, and wall-clock timestamp semantics. */
@Tag("gravitino-docker-test")
public class TestSparkJdbcReadCorrectness {
  /** Minimal catalog that installs the production JDBC table wrapper. */
  @SuppressWarnings("overrides")
  public static class SafeCatalog extends JDBCTableCatalog {
    /** {@inheritDoc} */
    @Override
    public JDBCTable loadTable(Identifier identifier) {
      JDBCTable physical = (JDBCTable) super.loadTable(identifier);
      Table metadata = mock(Table.class);
      when(metadata.columns())
          .thenReturn(
              identifier.name().equals("timestamp_only")
                  ? new Column[] {
                    Column.of("id", Types.IntegerType.get()),
                    Column.of("ts", Types.TimestampType.withoutTimeZone())
                  }
                  : new Column[] {
                    Column.of("id", Types.IntegerType.get()),
                        Column.of("s", Types.StringType.get()),
                    Column.of("b", Types.BinaryType.get()),
                        Column.of("ts", Types.TimestampType.withoutTimeZone()),
                    Column.of("tz", Types.TimestampType.withTimeZone())
                  });
      return new SparkJdbcTable(
          identifier,
          metadata,
          physical,
          this,
          JdbcPropertiesConverter.getInstance(),
          new SparkTransformConverter(false),
          new SparkJdbcTypeConverter(true));
    }
  }

  @Test
  void testActualQueriesKeepSparkSemanticsAcrossSessionTimeZones() throws Exception {
    try (GenericContainer<?> pg =
        new GenericContainer<>("postgres:16")
            .withEnv("POSTGRES_DB", "test")
            .withEnv("POSTGRES_PASSWORD", "test")
            .withExposedPorts(5432)
            .waitingFor(
                Wait.forLogMessage(".*database system is ready to accept connections.*\\n", 2))) {
      pg.start();
      String url = "jdbc:postgresql://" + pg.getHost() + ":" + pg.getMappedPort(5432) + "/test";
      try (Connection connection = DriverManager.getConnection(url, "postgres", "test");
          Statement statement = connection.createStatement()) {
        statement.execute(
            "CREATE TABLE samples(id int, s text COLLATE \"en-US-x-icu\", b bytea, ts timestamp, tz timestamptz)");
        statement.execute(
            "INSERT INTO samples VALUES (1, 'a', decode('01','hex'), '2026-10-31 12:34:56.123456', '2026-10-31 12:34:56.123456+00'), (2, 'B', decode('02','hex'), '0001-01-01 00:00:00', '2026-10-31 12:34:56.123456+00'), (3, '_x', decode('01','hex'), null, null)");
      }
      try (SparkSession spark =
          SparkSession.builder()
              .master("local[1]")
              .appName("jdbc-read-regression")
              .config("spark.ui.enabled", "false")
              .config("spark.sql.catalog.safe", SafeCatalog.class.getName())
              .config("spark.sql.catalog.safe.url", url)
              .config("spark.sql.catalog.safe.user", "postgres")
              .config("spark.sql.catalog.safe.password", "test")
              .config("spark.sql.catalog.safe.driver", "org.postgresql.Driver")
              .config("spark.sql.catalog.safe.preferTimestampNTZ", "true")
              .config("spark.sql.catalog.safe.pushDownPredicate", "true")
              .config("spark.sql.catalog.safe.pushDownAggregate", "true")
              .config("spark.sql.catalog.safe.pushDownLimit", "true")
              .getOrCreate()) {
        for (String zone : new String[] {"UTC", "America/Los_Angeles", "Asia/Shanghai"}) {
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
              spark
                  .sql("SELECT s FROM safe.public.samples ORDER BY s LIMIT 1")
                  .head()
                  .getString(0));
          List<Row> binary =
              spark
                  .sql("SELECT id FROM safe.public.samples WHERE b = X'01' ORDER BY id")
                  .collectAsList();
          Assertions.assertEquals(
              List.of(1, 3), List.of(binary.get(0).getInt(0), binary.get(1).getInt(0)));
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
        }
      }
    }
  }

  @Test
  void testTimestampFilterOnTableWithoutStringColumns() throws Exception {
    try (GenericContainer<?> pg =
        new GenericContainer<>("postgres:16")
            .withEnv("POSTGRES_DB", "test")
            .withEnv("POSTGRES_PASSWORD", "test")
            .withExposedPorts(5432)
            .waitingFor(
                Wait.forLogMessage(".*database system is ready to accept connections.*\\n", 2))) {
      pg.start();
      String url = "jdbc:postgresql://" + pg.getHost() + ":" + pg.getMappedPort(5432) + "/test";
      try (Connection connection = DriverManager.getConnection(url, "postgres", "test");
          Statement statement = connection.createStatement()) {
        statement.execute("CREATE TABLE timestamp_only(id int, ts timestamp)");
        statement.execute(
            "INSERT INTO timestamp_only VALUES (1, '2024-01-01 03:04:05'), (2, '2024-01-03 03:04:05')");
      }
      try (SparkSession spark =
          SparkSession.builder()
              .master("local[1]")
              .appName("timestamp-filter-regression")
              .config("spark.ui.enabled", "false")
              .config("spark.sql.catalog.safe", SafeCatalog.class.getName())
              .config("spark.sql.catalog.safe.url", url)
              .config("spark.sql.catalog.safe.user", "postgres")
              .config("spark.sql.catalog.safe.password", "test")
              .config("spark.sql.catalog.safe.driver", "org.postgresql.Driver")
              .config("spark.sql.catalog.safe.preferTimestampNTZ", "true")
              .config("spark.sql.catalog.safe.pushDownPredicate", "true")
              .getOrCreate()) {
        JDBCTableCatalog catalog =
            (JDBCTableCatalog) spark.sessionState().catalogManager().catalog("safe");
        SparkJdbcTable table =
            (SparkJdbcTable)
                catalog.loadTable(Identifier.of(new String[] {"public"}, "timestamp_only"));
        boolean supported = !spark.version().startsWith("3.5.");
        Assertions.assertEquals(supported, table.jdbcOptions().pushDownPredicate());
        Assertions.assertEquals(supported, table.jdbcOptions().pushDownAggregate());
        Assertions.assertEquals(supported, table.jdbcOptions().pushDownLimit());
        Assertions.assertEquals(supported, table.jdbcOptions().pushDownOffset());
        // Scan options must not override the safety policy installed on the table.
        Assertions.assertEquals(
            supported,
            table
                .newScanBuilder(new CaseInsensitiveStringMap(Map.of("pushDownPredicate", "true")))
                .jdbcOptions()
                .pushDownPredicate());
        for (String zone : new String[] {"UTC", "America/Los_Angeles"}) {
          spark.conf().set("spark.sql.session.timeZone", zone);
          List<Row> rows =
              spark
                  .sql(
                      "SELECT id FROM safe.public.timestamp_only WHERE ts > TIMESTAMP_NTZ '2024-01-02 03:04:05.123456'")
                  .collectAsList();
          Assertions.assertEquals(1, rows.size());
          Assertions.assertEquals(2, rows.get(0).getInt(0));
        }
      }
    }
  }
}
