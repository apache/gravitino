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

package org.apache.gravitino.storage.relational.utils;

import java.util.Arrays;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.gravitino.storage.relational.session.SqlSessions;
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
   * This method is used to perform a database operation without a commit and fetch the result. If
   * the operation fails, will throw the RuntimeException.
   *
   * @param mapperClazz mapper class to be used for the operation
   * @param func the operation to be performed with the mapper
   * @return the result of the operation
   * @param <T> the type of the mapper
   * @param <R> the type of the result
   */
  public static <T, R> R doWithoutCommitAndFetchResult(Class<T> mapperClazz, Function<T, R> func) {
    T mapper = SqlSessions.getMapper(mapperClazz);
    return func.apply(mapper);
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
    T mapper = SqlSessions.getMapper(mapperClazz);
    consumer.accept(mapper);
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
