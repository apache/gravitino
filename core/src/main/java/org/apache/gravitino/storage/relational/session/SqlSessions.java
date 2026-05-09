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
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.TransactionIsolationLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Thread-local MyBatis {@link SqlSession} management for the entity store.
 *
 * <p>Write paths ({@link #getWriteSqlSession()}, {@link #getWriteMapper}) use the primary JDBC URL.
 * Standalone reads ({@link #getReadMapper}) use the read-only JDBC URL when configured; if a write
 * session is already active on the thread, reads use that session so nested calls and read-after
 * write stay consistent.
 */
public final class SqlSessions {
  private static final Logger LOG = LoggerFactory.getLogger(SqlSessions.class);

  private static final ThreadLocal<SqlSession> writeSessions = new ThreadLocal<>();
  private static final ThreadLocal<AtomicInteger> writeSessionCount =
      ThreadLocal.withInitial(AtomicInteger::new);

  private static final ThreadLocal<SqlSession> readSessions = new ThreadLocal<>();
  private static final ThreadLocal<AtomicInteger> readSessionCount =
      ThreadLocal.withInitial(AtomicInteger::new);

  private SqlSessions() {}

  @VisibleForTesting
  static ThreadLocal<SqlSession> getWriteSessions() {
    return writeSessions;
  }

  @VisibleForTesting
  static ThreadLocal<SqlSession> getReadSessions() {
    return readSessions;
  }

  /** Returns true if a write transaction session is active on this thread. */
  public static boolean isWriteSessionActive() {
    return writeSessionCount.get().get() > 0;
  }

  @VisibleForTesting
  static Integer getSessionCount() {
    return writeSessionCount.get().get();
  }

  @VisibleForTesting
  static Integer getReadSessionCount() {
    return readSessionCount.get().get();
  }

  public static SqlSession getWriteSqlSession() {
    SqlSession sqlSession = writeSessions.get();
    if (sqlSession == null) {
      sqlSession =
          SqlSessionFactoryHelper.getInstance()
              .getWriteSqlSessionFactory()
              .openSession(TransactionIsolationLevel.READ_COMMITTED);
      writeSessions.set(sqlSession);
    }
    writeSessionCount.get().incrementAndGet();
    return sqlSession;
  }

  /**
   * Opens or reuses a read-only SqlSession (auto-commit). Not used when nested under an active
   * write session; use {@link #getReadMapper} instead.
   */
  private static SqlSession getOrCreateReadSqlSession() {
    SqlSession sqlSession = readSessions.get();
    if (sqlSession == null) {
      sqlSession =
          SqlSessionFactoryHelper.getInstance().getReadSqlSessionFactory().openSession(true);
      readSessions.set(sqlSession);
    }
    readSessionCount.get().incrementAndGet();
    return sqlSession;
  }

  public static <T> T getWriteMapper(Class<T> className) {
    return getWriteSqlSession().getMapper(className);
  }

  /**
   * Mapper for read-only access. Uses the read replica when configured and no write transaction is
   * active on this thread; otherwise uses the write session.
   */
  public static <T> T getReadMapper(Class<T> className) {
    if (writeSessionCount.get().get() > 0) {
      writeSessionCount.get().incrementAndGet();
      return writeSessions.get().getMapper(className);
    }
    return getOrCreateReadSqlSession().getMapper(className);
  }

  /** Same as {@link #getWriteSqlSession()} for backward compatibility. */
  public static SqlSession getSqlSession() {
    return getWriteSqlSession();
  }

  /** Prefer {@link #getWriteMapper(Class)}. */
  public static <T> T getMapper(Class<T> className) {
    return getWriteMapper(className);
  }

  public static void commitAndCloseSqlSession() {
    handleWriteSessionClose(true, false);
  }

  public static void rollbackAndCloseSqlSession() {
    handleWriteSessionClose(false, true);
  }

  /** Decrements the write-session ref count; closes when the outermost scope ends. */
  public static void closeSqlSession() {
    handleWriteSessionClose(false, false);
  }

  /** Decrements the read-session ref count; closes when the outermost read scope ends. */
  public static void closeReadSqlSession() {
    handleReadSessionClose();
  }

  private static void handleWriteSessionClose(boolean commit, boolean rollback) {
    SqlSession sqlSession = writeSessions.get();
    if (sqlSession == null) {
      return;
    }
    int count = writeSessionCount.get().decrementAndGet();
    if (count == 0) {
      try {
        if (commit) {
          sqlSession.commit();
        } else if (rollback) {
          sqlSession.rollback();
        }
      } finally {
        try {
          sqlSession.close();
        } finally {
          writeSessions.remove();
          writeSessionCount.remove();
        }
      }
    } else if (count < 0) {
      LOG.warn("Write session count is negative: {}. Resetting write session.", count);
      writeSessions.remove();
      writeSessionCount.remove();
    }
  }

  private static void handleReadSessionClose() {
    SqlSession sqlSession = readSessions.get();
    if (sqlSession == null) {
      return;
    }
    int count = readSessionCount.get().decrementAndGet();
    if (count == 0) {
      try {
        sqlSession.close();
      } finally {
        readSessions.remove();
        readSessionCount.remove();
      }
    } else if (count < 0) {
      LOG.warn("Read session count is negative: {}. Resetting read session.", count);
      readSessions.remove();
      readSessionCount.remove();
    }
  }
}
