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
package org.apache.gravitino.storage.relational.service;

import static org.awaitility.Awaitility.await;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.gravitino.storage.relational.session.SqlSessions;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.apache.ibatis.session.SqlSession;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.function.Executable;

/**
 * Runs one transaction while another holds a lock it needs, and waits until the database itself
 * reports that wait. Tests use it to order two transactions at a lock without relying on sleeps.
 */
final class TransactionLockWaits {

  private static final long TIMEOUT_SECONDS = 10;

  private final String backendType;

  TransactionLockWaits(String backendType) {
    this.backendType = backendType;
  }

  /**
   * Runs {@code holder} in a transaction on the calling thread, starts {@code contender} in its own
   * transaction on another thread, and waits until the contender is blocked on the holder. Then it
   * runs {@code beforeCompletion} in the holder's transaction and commits or rolls it back.
   *
   * @param holder the work that takes the lock in the holding transaction
   * @param contender the work that must wait for that lock
   * @param beforeCompletion more work for the holding transaction once the contender is waiting
   * @param commitHolder whether to commit the holding transaction rather than roll it back
   * @return the contender's failure, or null if its transaction committed
   * @throws Exception if the contender does not wait, or the holder fails
   */
  Throwable whileTransactionHeld(
      Executable holder, Executable contender, Executable beforeCompletion, boolean commitHolder)
      throws Exception {
    ExecutorService executor = Executors.newSingleThreadExecutor();
    CompletableFuture<Long> started = new CompletableFuture<>();
    SessionUtils.beginTransaction();
    try {
      long holderId = prepareTransaction();
      Assertions.assertDoesNotThrow(holder);
      Future<Throwable> result = submitTransaction(executor, started, contender);
      awaitBlockedBy(result, started.get(TIMEOUT_SECONDS, TimeUnit.SECONDS), holderId);
      Assertions.assertDoesNotThrow(beforeCompletion);
      if (commitHolder) {
        SessionUtils.commitTransaction();
      } else {
        SessionUtils.rollbackTransaction();
      }
      return result.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    } finally {
      SessionUtils.rollbackTransaction();
      executor.shutdownNow();
      Assertions.assertTrue(executor.awaitTermination(TIMEOUT_SECONDS, TimeUnit.SECONDS));
    }
  }

  /**
   * Runs {@code holder} in a transaction on the calling thread, then runs {@code contender} in its
   * own transaction on another thread while the holder is still open. The holder is rolled back
   * only after the contender has finished, so a contender that waits on the holder times out.
   *
   * @param holder the work whose transaction stays open
   * @param contender the work that must not wait for the holder
   * @return the contender's failure, or null if its transaction committed
   * @throws Exception if the contender waits on the holder, or the holder fails
   */
  Throwable whileTransactionOpen(Executable holder, Executable contender) throws Exception {
    ExecutorService executor = Executors.newSingleThreadExecutor();
    SessionUtils.beginTransaction();
    try {
      prepareTransaction();
      Assertions.assertDoesNotThrow(holder);
      return submitTransaction(executor, new CompletableFuture<>(), contender)
          .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    } finally {
      SessionUtils.rollbackTransaction();
      executor.shutdownNow();
      Assertions.assertTrue(executor.awaitTermination(TIMEOUT_SECONDS, TimeUnit.SECONDS));
    }
  }

  /**
   * Runs {@code operation} in its own transaction on {@code executor}.
   *
   * @param executor the executor that runs the transaction
   * @param started completed with the transaction's session ID once it has begun
   * @param operation the work to run in the transaction
   * @return the operation's failure, or null if its transaction committed
   */
  Future<Throwable> submitTransaction(
      ExecutorService executor, CompletableFuture<Long> started, Executable operation) {
    return executor.submit(
        () -> {
          SessionUtils.beginTransaction();
          try {
            started.complete(prepareTransaction());
            operation.execute();
            SessionUtils.commitTransaction();
            return null;
          } catch (Throwable failure) {
            started.completeExceptionally(failure);
            return failure;
          } finally {
            SessionUtils.rollbackTransaction();
          }
        });
  }

  /**
   * Sets the current transaction's lock timeout above the wait deadline and returns its session ID.
   *
   * @return the database session ID of the current transaction
   * @throws SQLException if the session cannot be configured
   */
  long prepareTransaction() throws SQLException {
    SqlSession session = SqlSessions.getSqlSession();
    try (Statement statement = session.getConnection().createStatement()) {
      String sessionIdQuery;
      switch (backendType) {
        case "h2":
          // Keep the engine timeout above the test's lock-observation deadline.
          statement.execute("SET LOCK_TIMEOUT 30000");
          sessionIdQuery = "SELECT SESSION_ID()";
          break;
        case "mysql":
          statement.execute("SET SESSION innodb_lock_wait_timeout = 30");
          sessionIdQuery = "SELECT CONNECTION_ID()";
          break;
        case "postgresql":
          statement.execute("SET LOCAL lock_timeout = '30s'");
          sessionIdQuery = "SELECT pg_backend_pid()";
          break;
        default:
          throw new IllegalStateException("Unsupported backend: " + backendType);
      }
      try (ResultSet rows = statement.executeQuery(sessionIdQuery)) {
        Assertions.assertTrue(rows.next());
        return rows.getLong(1);
      }
    } finally {
      SqlSessions.closeSqlSession();
    }
  }

  /**
   * Waits until the database reports the contender waiting on a lock the holder owns.
   *
   * @param result the contender's pending result
   * @param contenderId the contender's database session ID
   * @param holderId the holder's database session ID
   * @throws Exception if the contender finishes without waiting, or never waits in time
   */
  void awaitBlockedBy(Future<Throwable> result, long contenderId, long holderId) throws Exception {
    String query = blockedByQuery(contenderId, holderId);
    // Observe the actual waiter/blocker pair. A slow thread or connection checkout alone cannot
    // satisfy this assertion, and an unexpectedly completed operation fails immediately.
    try (SqlSession observer =
        SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true)) {
      Connection connection = observer.getConnection();
      await()
          .pollInSameThread()
          .atMost(TIMEOUT_SECONDS, TimeUnit.SECONDS)
          .pollInterval(10, TimeUnit.MILLISECONDS)
          .until(
              () -> {
                if (result.isDone()) {
                  throw new AssertionError(
                      "Operation completed without waiting for the holder", result.get());
                }
                try (Statement statement = connection.createStatement();
                    ResultSet rows = statement.executeQuery(query)) {
                  return rows.next() && rows.getLong(1) > 0;
                }
              });
    }
  }

  private String blockedByQuery(long contenderId, long holderId) {
    switch (backendType) {
      case "h2":
        return "SELECT COUNT(*) FROM INFORMATION_SCHEMA.SESSIONS WHERE SESSION_ID = "
            + contenderId
            + " AND BLOCKER_ID = "
            + holderId;
      case "mysql":
        return "SELECT COUNT(*) FROM performance_schema.data_lock_waits w"
            + " JOIN performance_schema.threads r ON r.THREAD_ID = w.REQUESTING_THREAD_ID"
            + " JOIN performance_schema.threads b ON b.THREAD_ID = w.BLOCKING_THREAD_ID"
            + " WHERE r.PROCESSLIST_ID = "
            + contenderId
            + " AND b.PROCESSLIST_ID = "
            + holderId;
      case "postgresql":
        return "SELECT COUNT(*) FROM unnest(pg_blocking_pids("
            + contenderId
            + ")) AS blocker(pid) WHERE pid = "
            + holderId;
      default:
        throw new IllegalStateException("Unsupported backend: " + backendType);
    }
  }
}
