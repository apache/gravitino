/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.storage.relation.mysql.orm;

import java.io.Closeable;
import java.io.IOException;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.TransactionIsolationLevel;

public final class SqlSessions implements Closeable {
  private static final ThreadLocal<SqlSession> sessions = new ThreadLocal<>();

  private SqlSessions() {}

  public static SqlSession getSqlSession() {
    SqlSession sqlSession = sessions.get();
    if (sqlSession == null) {
      sqlSession =
          SqlSessionFactoryHelper.getInstance()
              .getSqlSessionFactory()
              .openSession(TransactionIsolationLevel.READ_COMMITTED);
      sessions.set(sqlSession);
      return sqlSession;
    }
    return sqlSession;
  }

  public static void commitAndCloseSqlSession() {
    SqlSession sqlSession = sessions.get();
    if (sqlSession != null) {
      sqlSession.commit();
      sqlSession.close();
      sessions.remove();
    }
  }

  public static void rollbackAndCloseSqlSession() {
    SqlSession sqlSession = sessions.get();
    if (sqlSession != null) {
      sqlSession.rollback();
      sqlSession.close();
      sessions.remove();
    }
  }

  public static void closeSqlSession() {
    SqlSession sqlSession = sessions.get();
    if (sqlSession != null) {
      sqlSession.close();
      sessions.remove();
    }
  }

  public static <T> T getMapper(Class className) {
    return (T) getSqlSession().getMapper(className);
  }

  @Override
  public void close() throws IOException {
    sessions.remove();
  }
}
