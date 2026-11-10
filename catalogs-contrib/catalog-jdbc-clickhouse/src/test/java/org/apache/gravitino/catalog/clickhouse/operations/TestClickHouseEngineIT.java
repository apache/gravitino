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
package org.apache.gravitino.catalog.clickhouse.operations;

import static org.apache.gravitino.catalog.clickhouse.ClickHouseTablePropertiesMetadata.ENGINE;
import static org.apache.gravitino.catalog.clickhouse.ClickHouseTablePropertiesMetadata.GRAVITINO_ENGINE_KEY;

import com.google.common.collect.Maps;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import javax.sql.DataSource;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.gravitino.catalog.clickhouse.converter.ClickHouseColumnDefaultValueConverter;
import org.apache.gravitino.catalog.clickhouse.converter.ClickHouseExceptionConverter;
import org.apache.gravitino.catalog.clickhouse.converter.ClickHouseTypeConverter;
import org.apache.gravitino.catalog.jdbc.JdbcColumn;
import org.apache.gravitino.catalog.jdbc.JdbcTable;
import org.apache.gravitino.catalog.jdbc.config.JdbcConfig;
import org.apache.gravitino.catalog.jdbc.utils.DataSourceUtils;
import org.apache.gravitino.integration.test.container.ClickHouseContainer;
import org.apache.gravitino.integration.test.container.ContainerSuite;
import org.apache.gravitino.integration.test.util.TestDatabaseName;
import org.apache.gravitino.rel.expressions.distributions.Distributions;
import org.apache.gravitino.rel.indexes.Index;
import org.apache.gravitino.rel.types.Types;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.testcontainers.DockerClientFactory;

/**
 * Integration tests that verify Gravitino can create and load ClickHouse tables using various
 * engine types, and that table metadata (not data) survives a ClickHouse server restart.
 *
 * <p>Key findings documented here:
 *
 * <ul>
 *   <li><b>TinyLog / StripeLog / Log</b>: Fully supported. Table definition and data persist after
 *       restart.
 *   <li><b>Memory</b>: Table definition persists after restart (loadTable succeeds), but all data
 *       is lost. Gravitino metadata and ClickHouse are not in conflict, but users must be aware of
 *       the data volatility.
 *   <li><b>Null</b>: Supported. Table definition persists. Data is always discarded by design.
 *   <li><b>Set</b>: Supported. Table definition and data persist after restart (ClickHouse 24.x+
 *       stores Set engine data on disk).
 *   <li><b>Join</b>: ❌ NOT directly creatable via Gravitino. ClickHouse requires {@code ENGINE =
 *       Join(strictness, join_type, keys...)} with at least 3 parameters; Gravitino passes only the
 *       bare engine name and ClickHouse rejects it.
 *   <li><b>Buffer</b>: ❌ Requires parameterized ENGINE clause (destination database/table and flush
 *       thresholds); Gravitino's CREATE TABLE does not support arbitrary ENGINE parameters, so
 *       Buffer is not directly creatable via the Gravitino API.
 *   <li><b>View</b>: Must be created with a SELECT query (CREATE VIEW ... AS SELECT); this is not a
 *       standard table ENGINE and cannot be created via Gravitino's CREATE TABLE API.
 *   <li><b>KeeperMap</b>: Requires a ZooKeeper/ClickHouse-Keeper path parameter in the ENGINE
 *       clause; not available in a single-node setup without Keeper.
 *   <li><b>File</b>: Requires a file format parameter and specific server-side storage
 *       configuration; not directly creatable via Gravitino's CREATE TABLE API.
 * </ul>
 */
@Tag("gravitino-docker-test")
@TestInstance(Lifecycle.PER_CLASS)
public class TestClickHouseEngineIT {

  private static final ContainerSuite containerSuite = ContainerSuite.getInstance();
  private static final TestDatabaseName TEST_DB_NAME = TestDatabaseName.CLICKHOUSE_ENGINE_IT;

  private ClickHouseContainer clickHouseContainer;
  private ClickHouseDatabaseOperations databaseOperations;
  private ClickHouseTableOperations tableOperations;
  private String schemaName;

  @BeforeAll
  public void setup() throws Exception {
    containerSuite.startClickHouseContainer(TEST_DB_NAME);
    clickHouseContainer = containerSuite.getClickHouseContainer();

    Map<String, String> catalogProperties = Maps.newHashMap();
    catalogProperties.put(JdbcConfig.JDBC_URL.getKey(), clickHouseContainer.getJdbcUrl());
    catalogProperties.put(
        JdbcConfig.JDBC_DRIVER.getKey(), clickHouseContainer.getDriverClassName(TEST_DB_NAME));
    catalogProperties.put(JdbcConfig.USERNAME.getKey(), clickHouseContainer.getUsername());
    catalogProperties.put(JdbcConfig.PASSWORD.getKey(), clickHouseContainer.getPassword());

    DataSource dataSource = DataSourceUtils.createDataSource(catalogProperties);

    ClickHouseExceptionConverter exceptionConverter = new ClickHouseExceptionConverter();
    databaseOperations = new ClickHouseDatabaseOperations();
    databaseOperations.initialize(dataSource, exceptionConverter, Collections.emptyMap());

    tableOperations = new ClickHouseTableOperations();
    tableOperations.initialize(
        dataSource,
        exceptionConverter,
        new ClickHouseTypeConverter(),
        new ClickHouseColumnDefaultValueConverter(),
        Collections.emptyMap());

    schemaName = "engine_it_" + RandomStringUtils.randomAlphanumeric(6).toLowerCase();
    databaseOperations.create(schemaName, null, Collections.emptyMap());
  }

  @AfterAll
  public void cleanup() {
    if (databaseOperations != null && schemaName != null) {
      databaseOperations.delete(schemaName, true);
    }
  }

  // ---------------------------------------------------------------------------
  // Log-family engines (TinyLog / StripeLog / Log)
  // ---------------------------------------------------------------------------

  @Test
  void testTinyLogEngineCreateLoadAndPersistAfterRestart() throws Exception {
    String tableName = "tinylog_" + RandomStringUtils.randomAlphanumeric(6).toLowerCase();
    createSimpleTable(schemaName, tableName, ENGINE.TINYLOG);

    JdbcTable before = tableOperations.load(schemaName, tableName);
    Assertions.assertEquals(
        ENGINE.TINYLOG.getValue(), before.properties().get(GRAVITINO_ENGINE_KEY));

    insertRow(tableName, 1);
    Assertions.assertEquals(1, countRows(tableName));

    restartClickHouseAndWait();

    JdbcTable after = tableOperations.load(schemaName, tableName);
    Assertions.assertEquals(
        ENGINE.TINYLOG.getValue(), after.properties().get(GRAVITINO_ENGINE_KEY));
    // TinyLog persists data on disk — row must still be there
    Assertions.assertEquals(1, countRows(tableName));
  }

  @Test
  void testStripeLogEngineCreateLoadAndPersistAfterRestart() throws Exception {
    String tableName = "stripelog_" + RandomStringUtils.randomAlphanumeric(6).toLowerCase();
    createSimpleTable(schemaName, tableName, ENGINE.STRIPELOG);

    JdbcTable before = tableOperations.load(schemaName, tableName);
    Assertions.assertEquals(
        ENGINE.STRIPELOG.getValue(), before.properties().get(GRAVITINO_ENGINE_KEY));

    insertRow(tableName, 2);
    Assertions.assertEquals(1, countRows(tableName));

    restartClickHouseAndWait();

    JdbcTable after = tableOperations.load(schemaName, tableName);
    Assertions.assertEquals(
        ENGINE.STRIPELOG.getValue(), after.properties().get(GRAVITINO_ENGINE_KEY));
    // StripeLog persists data on disk — row must still be there
    Assertions.assertEquals(1, countRows(tableName));
  }

  @Test
  void testLogEngineCreateLoadAndPersistAfterRestart() throws Exception {
    String tableName = "log_" + RandomStringUtils.randomAlphanumeric(6).toLowerCase();
    createSimpleTable(schemaName, tableName, ENGINE.LOG);

    JdbcTable before = tableOperations.load(schemaName, tableName);
    Assertions.assertEquals(ENGINE.LOG.getValue(), before.properties().get(GRAVITINO_ENGINE_KEY));

    insertRow(tableName, 3);
    Assertions.assertEquals(1, countRows(tableName));

    restartClickHouseAndWait();

    JdbcTable after = tableOperations.load(schemaName, tableName);
    Assertions.assertEquals(ENGINE.LOG.getValue(), after.properties().get(GRAVITINO_ENGINE_KEY));
    // Log persists data on disk — row must still be there
    Assertions.assertEquals(1, countRows(tableName));
  }

  // ---------------------------------------------------------------------------
  // Memory engine (volatile: data is lost on restart)
  // ---------------------------------------------------------------------------

  @Test
  void testMemoryEngineIsVolatileAfterRestart() throws Exception {
    String tableName = "memory_" + RandomStringUtils.randomAlphanumeric(6).toLowerCase();
    createSimpleTable(schemaName, tableName, ENGINE.MEMORY);

    JdbcTable before = tableOperations.load(schemaName, tableName);
    Assertions.assertEquals(
        ENGINE.MEMORY.getValue(), before.properties().get(GRAVITINO_ENGINE_KEY));

    insertRow(tableName, 10);
    Assertions.assertEquals(1, countRows(tableName));

    restartClickHouseAndWait();

    // Table DEFINITION persists — loadTable must still succeed
    JdbcTable after = tableOperations.load(schemaName, tableName);
    Assertions.assertEquals(ENGINE.MEMORY.getValue(), after.properties().get(GRAVITINO_ENGINE_KEY));

    // But DATA is volatile — must be empty after restart
    Assertions.assertEquals(
        0,
        countRows(tableName),
        "Memory engine data must be empty after ClickHouse restart (volatile engine)");
  }

  // ---------------------------------------------------------------------------
  // Null engine (data is always discarded)
  // ---------------------------------------------------------------------------

  @Test
  void testNullEngineCreateLoadAndPersistAfterRestart() throws Exception {
    String tableName = "null_" + RandomStringUtils.randomAlphanumeric(6).toLowerCase();
    createSimpleTable(schemaName, tableName, ENGINE.NULL);

    JdbcTable before = tableOperations.load(schemaName, tableName);
    Assertions.assertEquals(ENGINE.NULL.getValue(), before.properties().get(GRAVITINO_ENGINE_KEY));

    restartClickHouseAndWait();

    JdbcTable after = tableOperations.load(schemaName, tableName);
    Assertions.assertEquals(ENGINE.NULL.getValue(), after.properties().get(GRAVITINO_ENGINE_KEY));
  }

  // ---------------------------------------------------------------------------
  // Set engine
  // ---------------------------------------------------------------------------

  @Test
  void testSetEngineCreateLoadAndPersistAfterRestart() throws Exception {
    String tableName = "set_" + RandomStringUtils.randomAlphanumeric(6).toLowerCase();
    createSimpleTable(schemaName, tableName, ENGINE.SET);

    JdbcTable before = tableOperations.load(schemaName, tableName);
    Assertions.assertEquals(ENGINE.SET.getValue(), before.properties().get(GRAVITINO_ENGINE_KEY));

    restartClickHouseAndWait();

    JdbcTable after = tableOperations.load(schemaName, tableName);
    Assertions.assertEquals(ENGINE.SET.getValue(), after.properties().get(GRAVITINO_ENGINE_KEY));
  }

  // ---------------------------------------------------------------------------
  // Join engine — NOT directly creatable via Gravitino's CREATE TABLE API.
  // ClickHouse requires: ENGINE = Join(ANY|ALL|SEMI|ANTI, LEFT|INNER|RIGHT, keys...)
  // Gravitino passes only the bare engine name, which is rejected by ClickHouse:
  //   "Storage Join requires at least 3 parameters"
  // This is documented in the class Javadoc and in docs/jdbc-clickhouse-catalog.md.
  // ---------------------------------------------------------------------------

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  /** Creates a minimal two-column table with the given engine (no ORDER BY, no indexes). */
  private void createSimpleTable(String dbName, String tableName, ENGINE engine) {
    JdbcColumn[] columns = {
      JdbcColumn.builder()
          .withName("id")
          .withType(Types.IntegerType.get())
          .withNullable(false)
          .build(),
      JdbcColumn.builder()
          .withName("name")
          .withType(Types.StringType.get())
          .withNullable(true)
          .build()
    };
    Map<String, String> props = new HashMap<>();
    props.put(GRAVITINO_ENGINE_KEY, engine.getValue());

    tableOperations.create(
        dbName,
        tableName,
        columns,
        "engine test table",
        props,
        null,
        Distributions.NONE,
        new Index[0],
        null);
  }

  private void insertRow(String tableName, int id) throws Exception {
    try (Connection conn =
            DriverManager.getConnection(
                clickHouseContainer.getJdbcUrl(TEST_DB_NAME),
                clickHouseContainer.getUsername(),
                clickHouseContainer.getPassword());
        Statement stmt = conn.createStatement()) {
      stmt.execute(
          String.format(
              "INSERT INTO `%s`.`%s` (id, name) VALUES (%d, 'test')", schemaName, tableName, id));
    }
  }

  private int countRows(String tableName) throws Exception {
    try (Connection conn =
            DriverManager.getConnection(
                clickHouseContainer.getJdbcUrl(TEST_DB_NAME),
                clickHouseContainer.getUsername(),
                clickHouseContainer.getPassword());
        Statement stmt = conn.createStatement();
        ResultSet rs =
            stmt.executeQuery(
                String.format("SELECT count() FROM `%s`.`%s`", schemaName, tableName))) {
      return rs.next() ? rs.getInt(1) : 0;
    }
  }

  /**
   * Restarts the ClickHouse Docker container and waits until the server is ready again. After
   * restart the container keeps the same internal IP and mapped ports, so the existing DataSource
   * reconnects automatically via HikariCP's stale-connection detection.
   */
  private void restartClickHouseAndWait() throws Exception {
    String containerId = clickHouseContainer.getContainer().getContainerId();
    DockerClientFactory.instance().client().restartContainerCmd(containerId).exec();

    String jdbcUrl = clickHouseContainer.getJdbcUrl();
    String user = clickHouseContainer.getUsername();
    String password = clickHouseContainer.getPassword();

    Awaitility.await()
        .atMost(Duration.ofMinutes(2))
        .pollInterval(Duration.ofSeconds(3))
        .ignoreExceptions()
        .until(
            () -> {
              try (Connection conn = DriverManager.getConnection(jdbcUrl, user, password);
                  Statement stmt = conn.createStatement()) {
                stmt.execute("SELECT 1");
                return true;
              }
            });
  }
}
