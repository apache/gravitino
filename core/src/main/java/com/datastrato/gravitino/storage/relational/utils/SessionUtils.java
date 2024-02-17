/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.storage.relational.utils;

import com.datastrato.gravitino.storage.relational.session.SqlSessions;
import java.util.Arrays;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.ibatis.session.SqlSession;

public class SessionUtils {
  private SessionUtils() {}

  public static <T> void doWithCommit(Class<T> mapperClazz, Consumer<T> consumer) {
    try (SqlSession session = SqlSessions.getSqlSession()) {
      try {
        T mapper = SqlSessions.getMapper(mapperClazz);
        consumer.accept(mapper);
        SqlSessions.commitAndCloseSqlSession();
      } catch (Throwable t) {
        SqlSessions.rollbackAndCloseSqlSession();
        throw new RuntimeException(t);
      }
    }
  }

  public static <T, R> R doWithCommitAndFetchResult(Class<T> mapperClazz, Function<T, R> func) {
    try (SqlSession session = SqlSessions.getSqlSession()) {
      try {
        T mapper = SqlSessions.getMapper(mapperClazz);
        R result = func.apply(mapper);
        SqlSessions.commitAndCloseSqlSession();
        return result;
      } catch (Throwable t) {
        throw new RuntimeException(t);
      } finally {
        SqlSessions.closeSqlSession();
      }
    }
  }

  public static <T> void doWithoutCommit(Class<T> mapperClazz, Consumer<T> consumer) {
    try {
      T mapper = SqlSessions.getMapper(mapperClazz);
      consumer.accept(mapper);
    } catch (Throwable t) {
      throw new RuntimeException(t);
    }
  }

  public static <T, R> R getWithoutCommit(Class<T> mapperClazz, Function<T, R> func) {
    try (SqlSession session = SqlSessions.getSqlSession()) {
      try {
        T mapper = SqlSessions.getMapper(mapperClazz);
        return func.apply(mapper);
      } catch (Throwable t) {
        throw new RuntimeException(t);
      } finally {
        SqlSessions.closeSqlSession();
      }
    }
  }

  public static void doMultipleWithCommit(Runnable... operations) {
    try (SqlSession session = SqlSessions.getSqlSession()) {
      try {
        Arrays.stream(operations).forEach(Runnable::run);
        SqlSessions.commitAndCloseSqlSession();
      } catch (Throwable t) {
        SqlSessions.rollbackAndCloseSqlSession();
        throw new RuntimeException(t);
      }
    }
  }
}
