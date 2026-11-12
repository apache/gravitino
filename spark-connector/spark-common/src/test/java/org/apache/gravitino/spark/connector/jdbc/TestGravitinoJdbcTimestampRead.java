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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.TableCatalog;
import org.apache.gravitino.rel.types.Types;
import org.apache.gravitino.spark.connector.catalog.GravitinoCatalogManager;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCRDD;
import org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCTable;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

/** MySQL timestamp options keep the production catalog schema and JDBC reader aligned. */
@Tag("gravitino-docker-test")
public class TestGravitinoJdbcTimestampRead {

  @Test
  void testMySqlIgnoresNtzCatalogBypassAndSessionOptions() throws Exception {
    TimeZone originalTimeZone = TimeZone.getDefault();
    TimeZone.setDefault(TimeZone.getTimeZone("Asia/Shanghai"));
    try (GenericContainer<?> database =
        new GenericContainer<>("mysql:8.0")
            .withEnv("MYSQL_DATABASE", "test")
            .withEnv("MYSQL_ROOT_PASSWORD", "test")
            .withEnv("MYSQL_ROOT_HOST", "%")
            .withExposedPorts(3306)
            .waitingFor(Wait.forLogMessage(".*mysqld: ready for connections.*\\n", 2))) {
      database.start();
      String url =
          "jdbc:mysql://"
              + database.getHost()
              + ":"
              + database.getMappedPort(3306)
              + "/test?allowPublicKeyRetrieval=true&useSSL=false"
              + "&connectionTimeZone=%2B08:00&forceConnectionTimeZoneToSession=true";
      try (Connection connection = DriverManager.getConnection(url, "root", "test");
          Statement statement = connection.createStatement()) {
        statement.execute("CREATE TABLE timestamps(id int, ts timestamp(6), dt datetime(6))");
        statement.execute(
            "INSERT INTO timestamps VALUES "
                + "(1, '2024-01-02 03:04:05.123456', '2024-01-02 03:04:05.123456'), "
                + "(2, NULL, NULL)");
      }
      GravitinoClient client = mock(GravitinoClient.class);
      GravitinoCatalogManager manager =
          GravitinoCatalogManager.create(new SparkConf(false), "user", identity -> client);
      try (SparkSession spark =
          SparkSession.builder()
              .master("local[1]")
              .appName("mysql-timestamp-options")
              .config("spark.ui.enabled", "false")
              .config("spark.sql.session.timeZone", "America/Los_Angeles")
              .getOrCreate()) {
        for (String source : List.of("catalog", "bypass", "session")) {
          String name = "ntz_" + source;
          Map<String, String> properties = new HashMap<>();
          properties.put("jdbc-url", url);
          properties.put("jdbc-driver", "com.mysql.cj.jdbc.Driver");
          properties.put("jdbc-user", "root");
          properties.put("jdbc-password", "test");
          if (source.equals("bypass")) {
            properties.put("spark.bypass.PrEfErTiMeStAmPnTz", "true");
          }
          Catalog catalog = mock(Catalog.class);
          when(catalog.type()).thenReturn(Catalog.Type.RELATIONAL);
          when(catalog.provider()).thenReturn("jdbc-mysql");
          when(catalog.properties()).thenReturn(properties);
          when(catalog.supportsSecrets()).thenThrow(UnsupportedOperationException.class);
          when(catalog.supportsCredentials()).thenThrow(UnsupportedOperationException.class);
          TableCatalog tables = mock(TableCatalog.class);
          when(catalog.asTableCatalog()).thenReturn(tables);
          Table metadata = mock(Table.class);
          when(metadata.columns())
              .thenReturn(
                  new Column[] {
                    Column.of("id", Types.IntegerType.get()),
                    Column.of("ts", Types.TimestampType.withoutTimeZone()),
                    Column.of("dt", Types.TimestampType.withoutTimeZone())
                  });
          when(tables.loadTable(any(NameIdentifier.class), anySet())).thenReturn(metadata);
          when(client.loadCatalog(name)).thenReturn(catalog);
          spark.conf().set("spark.sql.catalog." + name, GravitinoJdbcCatalog.class.getName());
          if (source.equals("catalog")) {
            spark.conf().set("spark.sql.catalog." + name + ".PrEfErTiMeStAmPnTz", "true");
          }
          spark
              .conf()
              .set(
                  "spark.sql.timestampType",
                  source.equals("session") ? "TIMESTAMP_NTZ" : "TIMESTAMP_LTZ");
          String table = name + ".test.timestamps";
          Dataset<Row> rows = spark.table(table);
          Assertions.assertEquals(DataTypes.TimestampType, rows.schema().apply("ts").dataType());
          Assertions.assertEquals(DataTypes.TimestampType, rows.schema().apply("dt").dataType());
          GravitinoJdbcCatalog initialized =
              (GravitinoJdbcCatalog) spark.sessionState().catalogManager().catalog(name);
          JDBCTable jdbcTable =
              (JDBCTable) initialized.loadTable(Identifier.of(new String[] {"test"}, "timestamps"));
          // Read the source schema with the actual catalog options, before the wrapper replaces it.
          StructType readerSchema = JDBCRDD.resolveTable(jdbcTable.jdbcOptions());
          Assertions.assertEquals(
              DataTypes.TimestampType, readerSchema.apply("ts").dataType(), source);
          Assertions.assertEquals(
              DataTypes.TimestampType, readerSchema.apply("dt").dataType(), source);
          String expected = "2024-01-01 11:04:05.123456";
          Assertions.assertEquals(
              List.of(RowFactory.create(1, expected, expected), RowFactory.create(2, null, null)),
              spark
                  .sql(
                      "SELECT id, CAST(ts AS STRING), CAST(dt AS STRING) FROM "
                          + table
                          + " ORDER BY id")
                  .collectAsList(),
              source);
          Assertions.assertEquals(
              RowFactory.create(1704135845123456L, 1704135845123456L, 1L, 1L),
              spark
                  .sql(
                      "SELECT unix_micros(min(ts)), unix_micros(max(dt)), count(ts), count(dt) FROM "
                          + table)
                  .head(),
              source);
        }
      } finally {
        manager.close();
      }
    } finally {
      TimeZone.setDefault(originalTimeZone);
    }
  }
}
