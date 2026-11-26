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

package org.apache.gravitino.storage.relational.session;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.TransactionIsolationLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SqlSessions is a utility class to maintain the MyBatis's {@link SqlSession} object. It is a
 * thread local class and should be used to get the {@link SqlSession} object. It also provides the
 * methods to commit, rollback and close the {@link SqlSession} object.
 */
public final class SqlSessions {
  private static final Logger LOG = LoggerFactory.getLogger(SqlSessions.class);
  private static final ThreadLocal<SqlSession> sessions = new ThreadLocal<>();
  private static final ThreadLocal<AtomicInteger> sessionCount =
      ThreadLocal.withInitial(() -> new AtomicInteger(0));
  private static final ThreadLocal<Throwable> rollbackCause = new ThreadLocal<>();
  private static final ThreadLocal<List<Runnable>> afterCommitCallbacks = new ThreadLocal<>();

  private SqlSessions() {}

  @VisibleForTesting
  static ThreadLocal<SqlSession> getSessions() {
    return sessions;
  }

  @VisibleForTesting
  static Integer getSessionCount() {
    return sessionCount.get().get();
  }

  /**
   * Get the SqlSession object. If the SqlSession object is not present in the thread local, then
   * create a new SqlSession object and set it in the thread local. This method also increments the
   * session count.
   *
   * @return SqlSession object from the thread local storage.
   */
  public static SqlSession getSqlSession() {
    SqlSession sqlSession = sessions.get();
    if (sqlSession == null) {
      sqlSession =
          SqlSessionFactoryHelper.getInstance()
              .getSqlSessionFactory()
              .openSession(TransactionIsolationLevel.READ_COMMITTED);
      sessions.set(sqlSession);
    }
    sessionCount.get().incrementAndGet();
    return sqlSession;
  }

  /**
   * Runs a top-level multi-statement read on one database snapshot without locking writers.
   *
   * @param read the reads to run in the snapshot
   * @param <T> the result type
   * @return the result of the reads
   * @throws IllegalStateException if another session is active on this thread
   */
  public static <T> T readSnapshot(Supplier<T> read) {
    Preconditions.checkState(
        !hasActiveSession(), "A snapshot read must start outside an active SQL session");
    SqlSessionFactory factory = SqlSessionFactoryHelper.getInstance().getSqlSessionFactory();
    // H2's repeatable read permits phantoms. Its serializable isolation pins an MVStore snapshot.
    TransactionIsolationLevel isolation =
        "h2".equals(factory.getConfiguration().getDatabaseId())
            ? TransactionIsolationLevel.SERIALIZABLE
            : TransactionIsolationLevel.REPEATABLE_READ;
    SqlSession session = factory.openSession(isolation);
    sessions.set(session);
    sessionCount.get().incrementAndGet();
    try (SnapshotScope ignored = SqlSessions::rollbackAndCloseSqlSession) {
      return read.get();
    }
  }

  /**
   * Commit the SqlSession object and close it. It also removes the SqlSession object from the
   * thread local storage.
   */
  public static void commitAndCloseSqlSession() {
    handleSessionClose(true /* commit */, false /* rollback */);
  }

  /**
   * Rollback the SqlSession object and close it. It also removes the SqlSession object from the
   * thread local storage.
   */
  public static void rollbackAndCloseSqlSession() {
    handleSessionClose(false /* commit */, true /* rollback */);
  }

  /** Close the SqlSession object and remove it from the thread local storage. */
  public static void closeSqlSession() {
    handleSessionClose(false /* commit */, false /* rollback */);
  }

  /**
   * Get the Mapper object from the SqlSession object. This method will open a session if one is not
   * already opened.
   *
   * @param <T> the type of the mapper interface.
   * @param className the class name of the Mapper object.
   * @return the Mapper object.
   */
  public static <T> T getMapper(Class<T> className) {
    // getSqlSession() is called to ensure a session exists and increment the count.
    return getSqlSession().getMapper(className);
  }

  /**
   * Returns whether the current thread already owns an open SQL session.
   *
   * @return {@code true} when an SQL session is active on the current thread
   */
  public static boolean hasActiveSession() {
    return sessions.get() != null;
  }

  /**
   * Requires the session owner to roll back, even if it catches the failing operation's exception.
   *
   * @param cause the failure that invalidated the transaction
   */
  public static void markRollbackOnly(Throwable cause) {
    Preconditions.checkState(hasActiveSession(), "No active SQL session on the current thread");
    if (rollbackCause.get() == null) {
      rollbackCause.set(Preconditions.checkNotNull(cause, "Missing rollback cause"));
    }
  }

  /**
   * Returns the connection of the current thread's open session without changing the session count.
   *
   * <p>Callers must not commit, roll back, or close the connection. The session owner commits it.
   *
   * @return the connection bound to the current session
   * @throws IllegalStateException if the current thread has no open session
   * @throws SQLException if the session cannot provide its connection
   */
  public static Connection currentConnection() throws SQLException {
    SqlSession sqlSession = sessions.get();
    if (sqlSession == null) {
      throw new IllegalStateException("No active SQL session on the current thread");
    }
    return sqlSession.getConnection();
  }

  /**
   * Registers a callback to execute only after the outermost active SQL session commits
   * successfully. Callbacks are discarded on rollback or commit failure. If no session is active,
   * the callback runs immediately.
   *
   * @param callback the post-commit action
   */
  public static void afterCommit(Runnable callback) {
    Preconditions.checkArgument(callback != null, "callback cannot be null");
    if (sessions.get() == null) {
      callback.run();
      return;
    }
    List<Runnable> callbacks = afterCommitCallbacks.get();
    if (callbacks == null) {
      callbacks = new ArrayList<>();
      afterCommitCallbacks.set(callbacks);
    }
    callbacks.add(callback);
  }

  private static void handleSessionClose(boolean commit, boolean rollback) {
    SqlSession sqlSession = sessions.get();
    if (sqlSession == null) {
      return;
    }

    int count = sessionCount.get().decrementAndGet();
    if (count == 0) {
      List<Runnable> callbacks = afterCommitCallbacks.get();
      boolean committed = false;
      try {
        Throwable cause = rollbackCause.get();
        if (rollback || cause != null) {
          // Force the JDBC rollback. A policy lifecycle hook can write on this connection without
          // going
          // through a mapped statement, and MyBatis skips rollback when the session looks clean.
          // Closing that session then turns auto-commit back on, which would commit the hook write.
          sqlSession.rollback(true);
          if (commit) {
            throw new IllegalStateException(
                "Cannot commit a transaction marked for rollback", cause);
          }
        } else if (commit) {
          sqlSession.commit();
          committed = true;
        }
      } finally {
        try {
          // Ensure the session is always closed
          sqlSession.close();
        } finally {
          // Ensure ThreadLocal is always cleaned up
          sessions.remove();
          sessionCount.remove();
          rollbackCause.remove();
          afterCommitCallbacks.remove();
        }
      }
      if (committed && callbacks != null) {
        for (Runnable callback : callbacks) {
          try {
            callback.run();
          } catch (RuntimeException e) {
            LOG.warn("After-commit callback failed", e);
          }
        }
      }
    } else if (count < 0) {
      // This should not happen if the session management is correct.
      // Reset the count and remove the session to avoid further issues.
      LOG.warn(
          "Session count is negative: {}. Resetting session count and removing session.", count);
      sessions.remove();
      sessionCount.remove();
      rollbackCause.remove();
      afterCommitCallbacks.remove();
    }
  }

  @FunctionalInterface
  private interface SnapshotScope extends AutoCloseable {
    @Override
    void close();
  }
}
