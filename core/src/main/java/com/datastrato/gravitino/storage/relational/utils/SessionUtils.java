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

/**
 * This class provides utility methods to perform database operations with MyBatis mappers in the
 * SqlSession.
 */
public class SessionUtils {
  private SessionUtils() {}

  /**
   * This method is used to perform a database operation with a commit. If the operation fails, the
   * transaction will roll back.
   *
   * @param mapperClazz mapper class to be used for the operation
   * @param consumer the operation to be performed with the mapper
   * @param <T> the type of the mapper
   */
  public static <T> void doWithCommit(Class<T> mapperClazz, Consumer<T> consumer) {
    try (SqlSession session = SqlSessions.getSqlSession()) {
      try {
        T mapper = SqlSessions.getMapper(mapperClazz);
        consumer.accept(mapper);
        SqlSessions.commitAndCloseSqlSession();
      } catch (Throwable t) {
        SqlSessions.rollbackAndCloseSqlSession();
        throw t;
      }
    }
  }

  /**
   * This method is used to perform a database operation with a commit and fetch the result. If the
   * operation fails, the transaction will roll back.
   *
   * @param mapperClazz mapper class to be used for the operation
   * @param func the operation to be performed with the mapper
   * @return the result of the operation
   * @param <T> the type of the mapper
   * @param <R> the type of the result
   */
  public static <T, R> R doWithCommitAndFetchResult(Class<T> mapperClazz, Function<T, R> func) {
    try (SqlSession session = SqlSessions.getSqlSession()) {
      try {
        T mapper = SqlSessions.getMapper(mapperClazz);
        R result = func.apply(mapper);
        SqlSessions.commitAndCloseSqlSession();
        return result;
      } catch (Throwable t) {
        SqlSessions.rollbackAndCloseSqlSession();
        throw t;
      }
    }
  }

  /**
   * This method is used to perform a database operation without a commit. If the operation fails,
   * will throw the RuntimeException.
   *
   * @param mapperClazz mapper class to be used for the operation
   * @param consumer the operation to be performed with the mapper
   * @param <T> the type of the mapper
   */
  public static <T> void doWithoutCommit(Class<T> mapperClazz, Consumer<T> consumer) {
    try {
      T mapper = SqlSessions.getMapper(mapperClazz);
      consumer.accept(mapper);
    } catch (Throwable t) {
      throw t;
    }
  }

  /**
   * This method is used to perform a database operation without a commit and fetch the result. If
   * the operation fails, will throw a RuntimeException.
   *
   * @param mapperClazz mapper class to be used for the operation
   * @param func the operation to be performed with the mapper
   * @return the result of the operation
   * @param <T> the type of the mapper
   * @param <R> the type of the result
   */
  public static <T, R> R getWithoutCommit(Class<T> mapperClazz, Function<T, R> func) {
    try (SqlSession session = SqlSessions.getSqlSession()) {
      try {
        T mapper = SqlSessions.getMapper(mapperClazz);
        return func.apply(mapper);
      } catch (Throwable t) {
        throw t;
      } finally {
        SqlSessions.closeSqlSession();
      }
    }
  }

  /**
   * This method is used to perform multiple database operations with a commit. If any of the
   * operations fail, the transaction will totally roll back.
   *
   * @param operations the operations to be performed
   */
  public static void doMultipleWithCommit(Runnable... operations) {
    try (SqlSession session = SqlSessions.getSqlSession()) {
      try {
        Arrays.stream(operations).forEach(Runnable::run);
        SqlSessions.commitAndCloseSqlSession();
      } catch (Throwable t) {
        SqlSessions.rollbackAndCloseSqlSession();
        throw t;
      }
    }
  }
}
