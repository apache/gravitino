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

package org.apache.gravitino.lock;

import static org.apache.gravitino.Configs.ENTITY_RELATIONAL_JDBC_BACKEND_DRIVER;
import static org.apache.gravitino.Configs.ENTITY_RELATIONAL_JDBC_BACKEND_PASSWORD;
import static org.apache.gravitino.Configs.ENTITY_RELATIONAL_JDBC_BACKEND_URL;
import static org.apache.gravitino.Configs.ENTITY_RELATIONAL_JDBC_BACKEND_USER;
import static org.apache.gravitino.Configs.LOCK_BACKEND_JDBC_ACQUIRE_TIMEOUT_MS;
import static org.apache.gravitino.Configs.LOCK_BACKEND_JDBC_DRIVER;
import static org.apache.gravitino.Configs.LOCK_BACKEND_JDBC_PASSWORD;
import static org.apache.gravitino.Configs.LOCK_BACKEND_JDBC_POOL_SIZE;
import static org.apache.gravitino.Configs.LOCK_BACKEND_JDBC_URL;
import static org.apache.gravitino.Configs.LOCK_BACKEND_JDBC_USER;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import javax.sql.DataSource;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.gravitino.Config;
import org.apache.gravitino.NameIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link LockBackend} that coordinates locks across Gravitino server instances using a single
 * relational table and per-row {@code SELECT ... FOR UPDATE / FOR SHARE} statements.
 *
 * <p>This backend is opt-in via {@code gravitino.lock.backend.type=jdbc}. The default in-process
 * backend remains in place for single-node deployments — see {@link InProcessLockBackend}.
 *
 * <p>Design notes (full discussion in {@code design-docs/treelock-ha.md}):
 *
 * <ul>
 *   <li>Lock table schema is intentionally minimal: a single {@code lock_path VARCHAR PRIMARY KEY}
 *       column. Observability columns (holder, txn id, acquired_at) are deferred.
 *   <li>Acquire opens a dedicated JDBC connection from a private DBCP2 pool, runs all per-level
 *       {@code INSERT / SELECT FOR ...} statements, and hands the connection to the returned {@link
 *       LockHandle}. {@code close()} commits and returns the connection to the pool.
 *   <li>Reentrant acquisition by the same thread on the same leaf path is rejected — a follow-up PR
 *       may add thread-local connection threading.
 *   <li>Process crash mid-lock is safe: the database releases the abandoned connection's row locks
 *       within its own dead-connection-detection window.
 * </ul>
 */
final class JdbcLockBackend implements LockBackend {

  private static final Logger LOG = LoggerFactory.getLogger(JdbcLockBackend.class);

  static final String NAME = "jdbc";
  static final String ROOT_PATH = "/";

  private static final ThreadLocal<Set<String>> ACTIVE_PATHS_PER_THREAD =
      ThreadLocal.withInitial(HashSet::new);

  private final BasicDataSource dataSource;
  private final JdbcDialect dialect;
  private final long acquireTimeoutMs;

  JdbcLockBackend(Config config) {
    String url =
        resolve(
            LOCK_BACKEND_JDBC_URL.getKey(),
            ENTITY_RELATIONAL_JDBC_BACKEND_URL.getKey(),
            config.get(LOCK_BACKEND_JDBC_URL),
            config.get(ENTITY_RELATIONAL_JDBC_BACKEND_URL));
    String user =
        resolve(
            LOCK_BACKEND_JDBC_USER.getKey(),
            ENTITY_RELATIONAL_JDBC_BACKEND_USER.getKey(),
            config.get(LOCK_BACKEND_JDBC_USER),
            config.get(ENTITY_RELATIONAL_JDBC_BACKEND_USER));
    String password =
        resolveOrEmpty(
            config.get(LOCK_BACKEND_JDBC_PASSWORD),
            config.get(ENTITY_RELATIONAL_JDBC_BACKEND_PASSWORD));
    String driver =
        resolve(
            LOCK_BACKEND_JDBC_DRIVER.getKey(),
            ENTITY_RELATIONAL_JDBC_BACKEND_DRIVER.getKey(),
            config.get(LOCK_BACKEND_JDBC_DRIVER),
            config.get(ENTITY_RELATIONAL_JDBC_BACKEND_DRIVER));
    int poolSize = config.get(LOCK_BACKEND_JDBC_POOL_SIZE);
    Preconditions.checkArgument(
        poolSize > 0, "gravitino.lock.backend.jdbc.poolSize must be positive, was %s", poolSize);
    this.acquireTimeoutMs = config.get(LOCK_BACKEND_JDBC_ACQUIRE_TIMEOUT_MS);
    Preconditions.checkArgument(
        acquireTimeoutMs > 0,
        "gravitino.lock.backend.jdbc.acquireTimeoutMs must be positive, was %s",
        acquireTimeoutMs);

    this.dialect = JdbcDialect.fromUrl(url);
    this.dataSource = buildDataSource(url, user, password, driver, poolSize);
    initSchema();
    LOG.info(
        "JdbcLockBackend initialized (dialect={}, poolSize={}, acquireTimeoutMs={})",
        dialect,
        poolSize,
        acquireTimeoutMs);
  }

  /** Test-only: inject a pre-built data source and dialect to bypass JDBC URL resolution. */
  @VisibleForTesting
  JdbcLockBackend(BasicDataSource dataSource, JdbcDialect dialect, long acquireTimeoutMs) {
    this.dataSource = dataSource;
    this.dialect = dialect;
    this.acquireTimeoutMs = acquireTimeoutMs;
    initSchema();
  }

  @Override
  public LockHandle acquire(NameIdentifier identifier, LockType lockType) {
    List<String> paths = pathsRootDown(identifier);
    String leafPath = paths.get(paths.size() - 1);

    Set<String> activePaths = ACTIVE_PATHS_PER_THREAD.get();
    if (!activePaths.add(leafPath)) {
      throw new IllegalStateException(
          "JdbcLockBackend does not support reentrant acquisition. "
              + "Thread "
              + Thread.currentThread().getName()
              + " already holds lock for path: "
              + leafPath);
    }

    Connection connection = null;
    try {
      connection = dataSource.getConnection();
      connection.setAutoCommit(false);
      try (Statement s = connection.createStatement()) {
        s.execute(dialect.lockTimeoutStatement(acquireTimeoutMs));
      }
      for (int i = 0; i < paths.size(); i++) {
        String path = paths.get(i);
        LockType levelType = (i == paths.size() - 1) ? lockType : LockType.READ;
        ensureRow(connection, path);
        acquireRow(connection, path, levelType);
      }
      return new JdbcLockHandle(connection, leafPath);
    } catch (SQLException e) {
      activePaths.remove(leafPath);
      rollbackAndClose(connection);
      throw new LockBackendException(
          "Failed to acquire " + lockType + " lock on " + identifier + " via JDBC backend", e);
    } catch (RuntimeException e) {
      activePaths.remove(leafPath);
      rollbackAndClose(connection);
      throw e;
    }
  }

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public void close() {
    try {
      dataSource.close();
    } catch (SQLException e) {
      LOG.warn("Failed to close JdbcLockBackend data source", e);
    }
  }

  /**
   * Compute the root-down list of paths to lock for {@code identifier}. Mirrors the in-process
   * backend's traversal in {@link LockManager#createTreeLock(NameIdentifier)}.
   *
   * <p>Examples:
   *
   * <ul>
   *   <li>{@code NameIdentifier.of("/")} → {@code ["/"]}
   *   <li>{@code NameIdentifier.of("metalake")} → {@code ["/", "/metalake"]}
   *   <li>{@code NameIdentifier.of("metalake", "catalog", "db", "tbl")} → {@code ["/", "/metalake",
   *       "/metalake/catalog", "/metalake/catalog/db", "/metalake/catalog/db/tbl"]}
   * </ul>
   */
  @VisibleForTesting
  @SuppressWarnings("ReferenceEquality") // mirrors LockManager.createTreeLock — ROOT is a singleton
  static List<String> pathsRootDown(NameIdentifier identifier) {
    List<String> paths = new ArrayList<>();
    paths.add(ROOT_PATH);
    if (identifier == LockManager.ROOT) {
      return paths;
    }
    StringBuilder sb = new StringBuilder();
    for (String level : identifier.namespace().levels()) {
      sb.append('/').append(level);
      paths.add(sb.toString());
    }
    sb.append('/').append(identifier.name());
    paths.add(sb.toString());
    return paths;
  }

  @VisibleForTesting
  DataSource dataSource() {
    return dataSource;
  }

  @VisibleForTesting
  static Set<String> activePathsForCurrentThread() {
    return Sets.newHashSet(ACTIVE_PATHS_PER_THREAD.get());
  }

  // -- private ---------------------------------------------------------------------------------

  private static BasicDataSource buildDataSource(
      String url, String user, String password, String driver, int poolSize) {
    BasicDataSource ds = new BasicDataSource();
    ds.setUrl(url);
    ds.setUsername(user);
    ds.setPassword(password);
    ds.setDriverClassName(driver);
    ds.setMaxTotal(poolSize);
    ds.setMinIdle(0);
    ds.setDefaultAutoCommit(false);
    return ds;
  }

  private void initSchema() {
    try (Connection c = dataSource.getConnection()) {
      boolean previousAutoCommit = c.getAutoCommit();
      c.setAutoCommit(true);
      try (Statement s = c.createStatement()) {
        s.execute(dialect.createTableSql());
      } finally {
        c.setAutoCommit(previousAutoCommit);
      }
    } catch (SQLException e) {
      throw new LockBackendException(
          "Failed to initialize lock table " + JdbcDialect.LOCK_TABLE, e);
    }
  }

  private void ensureRow(Connection conn, String path) throws SQLException {
    try (PreparedStatement ps = conn.prepareStatement(dialect.upsertSql())) {
      ps.setString(1, path);
      ps.executeUpdate();
    }
  }

  private void acquireRow(Connection conn, String path, LockType type) throws SQLException {
    String sql =
        (type == LockType.READ) ? dialect.selectForShareSql() : dialect.selectForUpdateSql();
    try (PreparedStatement ps = conn.prepareStatement(sql)) {
      ps.setString(1, path);
      try (ResultSet rs = ps.executeQuery()) {
        // Result is irrelevant; the lock is held by the act of executing the statement.
        rs.next();
      }
    }
  }

  private static void rollbackAndClose(Connection connection) {
    if (connection == null) {
      return;
    }
    try {
      connection.rollback();
    } catch (SQLException ignored) {
      // The connection may already be in an unrecoverable state; closing it is what matters.
    }
    try {
      connection.close();
    } catch (SQLException ignored) {
      // Returning to the pool failed — nothing useful to do beyond logging.
    }
  }

  /**
   * Resolve a primary config value, falling back to a secondary if the primary is empty. Throws
   * with a clear message if both are empty.
   */
  private static String resolve(
      String primaryKey, String fallbackKey, String primary, String fallback) {
    if (primary != null && !primary.isEmpty()) {
      return primary;
    }
    if (fallback != null && !fallback.isEmpty()) {
      return fallback;
    }
    throw new IllegalStateException(
        "JdbcLockBackend requires "
            + primaryKey
            + " (or, as a fallback, "
            + fallbackKey
            + ") to be set");
  }

  /** Resolve a config value that may legitimately be empty (e.g., password). */
  private static String resolveOrEmpty(String primary, String fallback) {
    if (primary != null && !primary.isEmpty()) {
      return primary;
    }
    return fallback == null ? "" : fallback;
  }

  /** Idempotent handle that commits and releases the underlying JDBC connection. */
  private static final class JdbcLockHandle implements LockHandle {
    private final Connection connection;
    private final String leafPath;
    private boolean closed = false;

    JdbcLockHandle(Connection connection, String leafPath) {
      this.connection = connection;
      this.leafPath = leafPath;
    }

    @Override
    public synchronized void close() {
      if (closed) {
        return;
      }
      closed = true;
      try {
        connection.commit();
      } catch (SQLException e) {
        LOG.warn("Failed to commit lock release for path {}; attempting rollback", leafPath, e);
        try {
          connection.rollback();
        } catch (SQLException ignored) {
          // Connection is unusable; close() below will reclaim it.
        }
      } finally {
        try {
          connection.close();
        } catch (SQLException ignored) {
          // Pool will reap it.
        }
        ACTIVE_PATHS_PER_THREAD.get().remove(leafPath);
      }
    }
  }
}
