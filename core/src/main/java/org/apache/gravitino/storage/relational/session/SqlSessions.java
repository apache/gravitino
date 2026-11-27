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
import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import javax.annotation.Nullable;
import org.apache.ibatis.session.SqlSession;
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

  // Run after the session is actually closed. An inner close only decrements the count.
  private static final CopyOnWriteArrayList<Runnable> CLOSED_LISTENERS =
      new CopyOnWriteArrayList<>();

  // A failure already caught after the session closed. The server installs its health check here.
  private static volatile Consumer<Throwable> closeFailureRecorder = failure -> {};

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
   * Registers work that runs when this thread's SQL session is actually closed.
   *
   * <p>The listener runs after the commit, rollback, or close, including when the session count was
   * already wrong and had to be reset. An inner close only decrements the count and does not run
   * it. Listeners must not throw; a failure is logged and the remaining listeners still run.
   *
   * @param listener work to run on the thread that closed the session
   */
  public static void addClosedListener(Runnable listener) {
    CLOSED_LISTENERS.addIfAbsent(listener);
  }

  /**
   * Removes a listener registered with {@link #addClosedListener}.
   *
   * @param listener the listener to remove
   */
  public static void removeClosedListener(Runnable listener) {
    CLOSED_LISTENERS.remove(listener);
  }

  /**
   * Installs the recorder for a failure already caught after the SQL session closed. The server
   * points this at its health check. A null recorder drops the failure.
   *
   * @param recorder the recorder, or null to drop the failure
   */
  public static void setCloseFailureRecorder(@Nullable Consumer<Throwable> recorder) {
    closeFailureRecorder = recorder == null ? failure -> {} : recorder;
  }

  /**
   * Passes a failure caught after the session closed to the recorder installed by {@link
   * #setCloseFailureRecorder}. A recorder that throws is ignored, so a successful commit stays
   * successful.
   *
   * @param failure the consumed failure, or null
   */
  public static void recordCloseFailure(@Nullable Throwable failure) {
    try {
      closeFailureRecorder.accept(failure);
    } catch (RuntimeException | Error ignored) {
      // The caller already consumed this failure. Recording it must not replace a successful
      // commit.
    }
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

  private static void handleSessionClose(boolean commit, boolean rollback) {
    SqlSession sqlSession = sessions.get();
    if (sqlSession == null) {
      return;
    }

    int count = sessionCount.get().decrementAndGet();
    if (count == 0) {
      try {
        if (commit) {
          sqlSession.commit();
        } else if (rollback) {
          // Force the JDBC rollback. A policy lifecycle hook can write on this connection without
          // going
          // through a mapped statement, and MyBatis skips rollback when the session looks clean.
          // Closing that session then turns auto-commit back on, which would commit the hook write.
          sqlSession.rollback(true);
        }
      } finally {
        try {
          // Ensure the session is always closed
          sqlSession.close();
        } finally {
          // Ensure ThreadLocal is always cleaned up
          sessions.remove();
          sessionCount.remove();
          notifyClosed();
        }
      }
    } else if (count < 0) {
      // This should not happen if the session management is correct.
      // Reset the count and remove the session to avoid further issues.
      LOG.warn(
          "Session count is negative: {}. Resetting session count and removing session.", count);
      try {
        // Do not let close() turn auto-commit back on and commit the unbalanced work.
        sqlSession.rollback(true);
      } finally {
        try {
          sqlSession.close();
        } finally {
          sessions.remove();
          sessionCount.remove();
          notifyClosed();
        }
      }
    }
  }

  private static void notifyClosed() {
    for (Runnable listener : CLOSED_LISTENERS) {
      try {
        listener.run();
      } catch (RuntimeException | Error e) {
        recordCloseFailure(e);
        LOG.warn("Failed to finish work deferred until the outermost SQL session closed", e);
      }
    }
  }
}
