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
package org.apache.gravitino.storage.relational;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.Entity;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.cache.EntityCache;
import org.apache.gravitino.exceptions.OptimisticLockException;
import org.apache.gravitino.meta.SchemaEntity;
import org.apache.gravitino.meta.TableEntity;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.gravitino.storage.relational.session.SqlSessions;
import org.apache.ibatis.mapping.Environment;
import org.apache.ibatis.session.Configuration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.function.Executable;
import org.mockito.Mockito;

/**
 * Verifies real backend lock timeouts, transaction cleanup, and retries through the entity store.
 */
@Tag("gravitino-docker-test")
public class TestRelationalEntityStoreBackendLockWaitTimeout extends TestJDBCBackend {

  @AfterEach
  void closeBackend() throws Exception {
    String databasePath = null;
    try {
      if ("h2".equals(backendType)) {
        BasicDataSource source =
            (BasicDataSource)
                SqlSessionFactoryHelper.getInstance()
                    .getSqlSessionFactory()
                    .getConfiguration()
                    .getEnvironment()
                    .getDataSource();
        databasePath = source.getUrl().substring("jdbc:h2:file:".length()).split(";")[0];
        // The shared fixture uses DB_CLOSE_DELAY=-1, so close the engine before deleting its files.
        try (Connection connection = source.getConnection();
            Statement statement = connection.createStatement()) {
          statement.execute("SHUTDOWN");
        }
      }
    } finally {
      try {
        backend.close();
      } finally {
        if (databasePath != null) {
          FileUtils.deleteDirectory(Path.of(databasePath).toFile());
          Files.deleteIfExists(Path.of(databasePath + ".mv.db"));
          Files.deleteIfExists(Path.of(databasePath + ".trace.db"));
        }
      }
    }
  }

  @TestTemplate
  void testDropTimeoutPreservesTableAndRetrySucceeds() throws Exception {
    createParents();
    TableEntity table =
        createAndInsertTableEntity(Namespace.of("timeout_metalake", "catalog", "schema"), "table");
    RelationalEntityStore store = createStore();

    writeWhileRowHeld(
        "table_meta",
        "table_id",
        table.id(),
        () -> store.delete(table.nameIdentifier(), Entity.EntityType.TABLE));

    Assertions.assertTrue(backend.exists(table.nameIdentifier(), Entity.EntityType.TABLE));
    Assertions.assertTrue(store.delete(table.nameIdentifier(), Entity.EntityType.TABLE));
    Assertions.assertFalse(backend.exists(table.nameIdentifier(), Entity.EntityType.TABLE));
  }

  @TestTemplate
  void testCreateTimeoutLeavesNoTableAndRetrySucceeds() throws Exception {
    SchemaEntity schema = createParents();
    TableEntity table =
        createTableEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            Namespace.of("timeout_metalake", "catalog", "schema"),
            "table",
            AUDIT_INFO);
    RelationalEntityStore store = createStore();

    writeWhileRowHeld("schema_meta", "schema_id", schema.id(), () -> store.put(table, false));

    Assertions.assertFalse(backend.exists(table.nameIdentifier(), Entity.EntityType.TABLE));
    store.put(table, false);
    Assertions.assertEquals(
        table.id(), backend.get(table.nameIdentifier(), Entity.EntityType.TABLE).id());
  }

  private SchemaEntity createParents() throws Exception {
    createAndInsertMakeLake("timeout_metalake");
    createAndInsertCatalog("timeout_metalake", "catalog");
    return createAndInsertSchema("timeout_metalake", "catalog", "schema");
  }

  private RelationalEntityStore createStore() throws IllegalAccessException {
    RelationalEntityStore store = new RelationalEntityStore();
    // Use the extension's real backend; only the unrelated cache is mocked.
    FieldUtils.writeField(store, "backend", backend, true);
    FieldUtils.writeField(store, "cache", Mockito.mock(EntityCache.class), true);
    return store;
  }

  @SuppressWarnings("deprecation")
  private void writeWhileRowHeld(String table, String idColumn, long id, Executable write)
      throws Exception {
    Configuration configuration =
        SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().getConfiguration();
    Environment original = configuration.getEnvironment();
    BasicDataSource source = (BasicDataSource) original.getDataSource();
    String timeoutSql;
    switch (backendType) {
      case "h2":
        timeoutSql = "SET LOCK_TIMEOUT 250";
        break;
      case "mysql":
        timeoutSql = "SET SESSION innodb_lock_wait_timeout = 1";
        break;
      case "postgresql":
        timeoutSql = "SET lock_timeout = '250ms'";
        break;
      default:
        throw new IllegalStateException("Unsupported backend: " + backendType);
    }

    // Configure a separate writer pool before it opens connections. The production write still
    // owns its ordinary transaction, so a test-managed rollback cannot hide missing cleanup.
    try (BasicDataSource writers = new BasicDataSource();
        Connection holder = source.getConnection()) {
      writers.setUrl(source.getUrl());
      writers.setDriverClassName(source.getDriverClassName());
      writers.setUsername(source.getUsername());
      writers.setPassword(source.getPassword());
      writers.setDefaultAutoCommit(false);
      writers.setConnectionInitSqls(List.of(timeoutSql));
      configuration.setEnvironment(
          new Environment(original.getId(), original.getTransactionFactory(), writers));
      holder.setAutoCommit(false);
      try {
        try (Statement statement = holder.createStatement();
            ResultSet rows =
                statement.executeQuery(
                    String.format(
                        "SELECT %s FROM %s WHERE %s = %d FOR UPDATE",
                        idColumn, table, idColumn, id))) {
          Assertions.assertTrue(rows.next());
        }
        OptimisticLockException conflict =
            Assertions.assertThrows(OptimisticLockException.class, write);
        Assertions.assertTrue(conflict.getMessage().contains("retry the operation"));
        Throwable cause = conflict;
        while (cause != null && !(cause instanceof SQLException)) {
          cause = cause.getCause();
        }
        Assertions.assertNotNull(cause, "the real driver timeout must remain attached");
        SQLException timeout = (SQLException) cause;
        if ("postgresql".equals(backendType)) {
          Assertions.assertEquals("55P03", timeout.getSQLState());
        } else {
          Assertions.assertEquals(
              "mysql".equals(backendType) ? 1205 : 50200, timeout.getErrorCode());
        }
        Assertions.assertFalse(
            SqlSessions.hasActiveSession(), "the failed writer must close its session");
      } finally {
        holder.rollback();
      }
    } finally {
      configuration.setEnvironment(original);
    }
  }
}
