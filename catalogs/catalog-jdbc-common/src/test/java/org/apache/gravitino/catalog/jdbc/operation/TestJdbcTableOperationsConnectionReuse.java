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
package org.apache.gravitino.catalog.jdbc.operation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import javax.sql.DataSource;
import org.apache.commons.io.FileUtils;
import org.apache.gravitino.catalog.jdbc.JdbcColumn;
import org.apache.gravitino.catalog.jdbc.JdbcTable;
import org.apache.gravitino.catalog.jdbc.config.JdbcConfig;
import org.apache.gravitino.catalog.jdbc.converter.SqliteColumnDefaultValueConverter;
import org.apache.gravitino.catalog.jdbc.converter.SqliteExceptionConverter;
import org.apache.gravitino.catalog.jdbc.converter.SqliteTypeConverter;
import org.apache.gravitino.catalog.jdbc.utils.DataSourceUtils;
import org.apache.gravitino.rel.TableChange;
import org.apache.gravitino.rel.expressions.distributions.Distributions;
import org.apache.gravitino.rel.indexes.Indexes;
import org.apache.gravitino.rel.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Guards against the connection-pool starvation described in apache/gravitino#13591: {@code
 * alterTable} occupies a connection to generate and execute the ALTER statement, and the statement
 * generator loads the original table. That load MUST reuse the caller's connection; otherwise every
 * concurrent ALTER holds one connection while waiting for a second one, and a pool of size N is
 * exhausted by N requests.
 */
public class TestJdbcTableOperationsConnectionReuse {

  private static final String DATABASE_NAME = "test";

  private File baseDir;
  private ProbeSqliteTableOperations operations;
  private final AtomicInteger activeBorrows = new AtomicInteger();
  private final AtomicInteger totalBorrows = new AtomicInteger();
  private final AtomicInteger maxActiveBorrows = new AtomicInteger();

  @BeforeEach
  public void setUp() throws IOException {
    baseDir = Files.createTempDirectory("gravitino-jdbc-connection-reuse").toFile();
    String jdbcUrl = "jdbc:sqlite:" + baseDir.getPath() + "/" + DATABASE_NAME;
    HashMap<String, String> properties = new HashMap<>();
    properties.put(JdbcConfig.JDBC_DRIVER.getKey(), "org.sqlite.JDBC");
    properties.put(JdbcConfig.JDBC_URL.getKey(), jdbcUrl);
    properties.put(JdbcConfig.USERNAME.getKey(), "test");
    properties.put(JdbcConfig.PASSWORD.getKey(), "test");

    operations = new ProbeSqliteTableOperations();
    operations.initialize(
        singleConnectionDataSource(DataSourceUtils.createDataSource(properties)),
        new SqliteExceptionConverter(),
        new SqliteTypeConverter(),
        new SqliteColumnDefaultValueConverter(),
        Collections.emptyMap());
  }

  @AfterEach
  public void tearDown() {
    FileUtils.deleteQuietly(baseDir);
  }

  @Test
  public void testAlterTableReusesCallerConnection() {
    String table = "alter_conn_reuse";
    operations.create(
        DATABASE_NAME,
        table,
        new JdbcColumn[] {
          JdbcColumn.builder()
              .withName("col_a")
              .withNullable(true)
              .withType(Types.IntegerType.get())
              .withComment(null)
              .withDefaultValue(null)
              .build()
        },
        null,
        Collections.emptyMap(),
        null,
        Distributions.NONE,
        Indexes.EMPTY_INDEXES);

    int borrowsBeforeAlter = totalBorrows.get();

    // The probe generator loads the original table, exactly like the MySQL/PostgreSQL generators
    // do.
    operations.alterTable(DATABASE_NAME, table, TableChange.updateComment("ignored"));

    assertEquals(
        borrowsBeforeAlter + 1,
        totalBorrows.get(),
        "alterTable must borrow exactly one connection and reuse it while generating the SQL");
    assertEquals(
        1,
        maxActiveBorrows.get(),
        "alterTable must never hold more than one connection from the pool at a time");

    JdbcTable renamed = operations.load(DATABASE_NAME, table + "_renamed");
    assertEquals(table + "_renamed", renamed.name());
    assertEquals(
        Collections.singletonList("col_a"),
        Arrays.stream(renamed.columns()).map(column -> column.name()).collect(Collectors.toList()));
  }

  /**
   * Wraps a {@link DataSource} so that borrowing a second connection while one is still checked out
   * fails fast, mirroring the pool starvation where every slot is held by an outer connection.
   */
  private DataSource singleConnectionDataSource(DataSource delegate) {
    InvocationHandler handler =
        (proxy, method, args) -> {
          if ("getConnection".equals(method.getName())) {
            if (activeBorrows.get() != 0) {
              throw new SQLException(
                  "Nested connection borrow detected: getConnection() was called while the caller's"
                      + " connection is still checked out");
            }
            Connection connection = (Connection) method.invoke(delegate, args);
            int active = activeBorrows.incrementAndGet();
            totalBorrows.incrementAndGet();
            maxActiveBorrows.accumulateAndGet(active, Math::max);
            return trackedConnection(connection);
          }
          return method.invoke(delegate, args);
        };
    return (DataSource)
        Proxy.newProxyInstance(
            DataSource.class.getClassLoader(), new Class<?>[] {DataSource.class}, handler);
  }

  private Connection trackedConnection(Connection delegate) {
    InvocationHandler handler =
        (proxy, method, args) -> {
          if ("close".equals(method.getName())) {
            activeBorrows.decrementAndGet();
          }
          return method.invoke(delegate, args);
        };
    return (Connection)
        Proxy.newProxyInstance(
            Connection.class.getClassLoader(), new Class<?>[] {Connection.class}, handler);
  }

  /**
   * A {@link SqliteTableOperations} whose alter path mirrors the real JDBC generators: it loads the
   * original table to build the statement, then emits a statement executed with the caller's
   * connection.
   */
  private static class ProbeSqliteTableOperations extends SqliteTableOperations {
    @Override
    protected String generateAlterTableSql(
        Connection connection, String databaseName, String tableName, TableChange... changes) {
      JdbcTable original = getOrCreateTable(connection, databaseName, tableName, null);
      assertTrue(original.columns().length > 0, "the original table must be loaded");
      return generateRenameTableSql(tableName, tableName + "_renamed");
    }

    @Override
    protected JdbcTable getOrCreateTable(
        Connection connection,
        String databaseName,
        String tableName,
        JdbcTable lazyLoadCreateTable) {
      return null != lazyLoadCreateTable
          ? lazyLoadCreateTable
          : load(connection, databaseName, tableName);
    }
  }
}
