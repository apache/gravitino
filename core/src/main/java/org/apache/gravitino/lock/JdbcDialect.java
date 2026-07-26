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

import com.google.common.base.Preconditions;
import java.util.Locale;

/**
 * SQL dialect differences for {@link JdbcLockBackend}. Detected from the JDBC URL prefix at backend
 * construction time; the exact same heuristic the existing relational entity store uses.
 *
 * <p>Why this enum exists: portable {@code INSERT IF NOT EXISTS}, shared row locks, and lock-wait
 * timeouts have non-portable syntax across H2/MySQL/Postgres. Centralising the differences here
 * keeps {@link JdbcLockBackend} dialect-agnostic.
 */
enum JdbcDialect {
  POSTGRES,
  MYSQL,
  H2;

  static final String LOCK_TABLE = "gravitino_lock";

  static JdbcDialect fromUrl(String url) {
    Preconditions.checkArgument(url != null && !url.isEmpty(), "JDBC URL must not be empty");
    String lower = url.toLowerCase(Locale.ROOT);
    if (lower.startsWith("jdbc:postgresql:")) {
      return POSTGRES;
    }
    if (lower.startsWith("jdbc:mysql:")) {
      return MYSQL;
    }
    if (lower.startsWith("jdbc:h2:")) {
      return H2;
    }
    throw new IllegalArgumentException(
        "Unsupported JDBC URL for the lock backend: "
            + url
            + ". Supported prefixes: jdbc:postgresql:, jdbc:mysql:, jdbc:h2:");
  }

  /**
   * SQL that creates the lock table if it does not already exist. The table has a single {@code
   * lock_path VARCHAR PRIMARY KEY} column — observability columns (holder, txn id, acquired at) are
   * intentionally omitted in v1; see {@code design-docs/treelock-ha.md} §6.7.
   */
  String createTableSql() {
    switch (this) {
      case POSTGRES:
      case MYSQL:
      case H2:
        // VARCHAR(1024) accommodates the deepest reasonable metalake/catalog/schema/table path.
        return "CREATE TABLE IF NOT EXISTS "
            + LOCK_TABLE
            + " (lock_path VARCHAR(1024) NOT NULL, PRIMARY KEY (lock_path))";
      default:
        throw new IllegalStateException("Unhandled dialect: " + this);
    }
  }

  /**
   * SQL that sets the per-transaction lock-wait timeout. Must be invoked after {@code BEGIN} and
   * before any row-locking statements.
   */
  String lockTimeoutStatement(long timeoutMs) {
    Preconditions.checkArgument(timeoutMs > 0, "Lock timeout must be positive");
    switch (this) {
      case POSTGRES:
        return "SET LOCAL lock_timeout = '" + timeoutMs + "ms'";
      case MYSQL:
        // MySQL's innodb_lock_wait_timeout is integer seconds, minimum 1.
        return "SET innodb_lock_wait_timeout = " + Math.max(1, timeoutMs / 1000);
      case H2:
        return "SET LOCK_TIMEOUT " + timeoutMs;
      default:
        throw new IllegalStateException("Unhandled dialect: " + this);
    }
  }

  /** Idempotent INSERT — ensures a row exists for {@code lock_path} so subsequent locking works. */
  String upsertSql() {
    switch (this) {
      case POSTGRES:
        return "INSERT INTO " + LOCK_TABLE + " (lock_path) VALUES (?) ON CONFLICT DO NOTHING";
      case MYSQL:
        return "INSERT IGNORE INTO " + LOCK_TABLE + " (lock_path) VALUES (?)";
      case H2:
        return "MERGE INTO " + LOCK_TABLE + " (lock_path) KEY (lock_path) VALUES (?)";
      default:
        throw new IllegalStateException("Unhandled dialect: " + this);
    }
  }

  /**
   * Acquire an exclusive row lock on {@code lock_path}. Used for the leaf of {@link LockType#WRITE}
   * acquisitions.
   */
  String selectForUpdateSql() {
    return "SELECT 1 FROM " + LOCK_TABLE + " WHERE lock_path = ? FOR UPDATE";
  }

  /**
   * Acquire a shared row lock on {@code lock_path}. Used for ancestor nodes and for the leaf of
   * {@link LockType#READ} acquisitions.
   *
   * <p>H2's {@code FOR SHARE} is not as well-supported as Postgres/MySQL; on H2 we degrade to the
   * exclusive lock. Operators using H2 are by definition not running an HA deployment, so the extra
   * contention is acceptable.
   */
  String selectForShareSql() {
    switch (this) {
      case POSTGRES:
      case MYSQL:
        return "SELECT 1 FROM " + LOCK_TABLE + " WHERE lock_path = ? FOR SHARE";
      case H2:
        return selectForUpdateSql();
      default:
        throw new IllegalStateException("Unhandled dialect: " + this);
    }
  }
}
