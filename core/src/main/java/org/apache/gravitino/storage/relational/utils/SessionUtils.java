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

/**
 * This class provides utility methods to perform database operations with MyBatis mappers in the
 * SqlSession.
 */
public class SessionUtils {
  private SessionUtils() {}

  /**
   * Performs a database operation with a commit. Manages the full transaction lifecycle. Can be
   * nested within other transactions.
   */
  public static <T> void doWithCommit(Class<T> mapperClazz, Consumer<T> consumer) {
    try {
      T mapper = SqlSessions.getMapper(mapperClazz);
      consumer.accept(mapper);
      SqlSessions.commitAndCloseSqlSession();
    } catch (Exception e) {
      SqlSessions.rollbackAndCloseSqlSession();
      throw e;
    }
  }

  /**
   * Performs a database operation with a commit and fetches a result. Manages the full transaction
   * lifecycle. Can be nested within other transactions.
   */
  public static <T, R> R doWithCommitAndFetchResult(Class<T> mapperClazz, Function<T, R> func) {
    try {
      T mapper = SqlSessions.getMapper(mapperClazz);
      R result = func.apply(mapper);
      SqlSessions.commitAndCloseSqlSession();
      return result;
    } catch (Exception e) {
      SqlSessions.rollbackAndCloseSqlSession();
      throw e;
    }
  }

  /**
   * Performs a read-only database operation without a commit. Can be used standalone or nested
   * within other transactions.
   */
  public static <T, R> R getWithoutCommit(Class<T> mapperClazz, Function<T, R> func) {
    try {
      T mapper = SqlSessions.getMapper(mapperClazz);
      return func.apply(mapper);
    } finally {
      // This will decrement the counter, the session is closed only when the counter is 0.
      SqlSessions.closeSqlSession();
    }
  }

  /**
   * Performs a database operation without a commit. Can be used standalone or nested within other
   * transactions. This method is for operations that do not return a result.
   */
  public static <T> void doWithoutCommit(Class<T> mapperClazz, Consumer<T> consumer) {
    try {
      T mapper = SqlSessions.getMapper(mapperClazz);
      consumer.accept(mapper);
    } finally {
      // This will decrement the counter, the session is closed only when the counter is 0.
      SqlSessions.closeSqlSession();
    }
  }

  /**
   * Performs multiple database operations within a single commit. Manages the full transaction
   * lifecycle.
   */
  public static void doMultipleWithCommit(Runnable... operations) {
    // This method acts as the outermost transaction boundary.
    // It increments the session count once.
    SqlSessions.getSqlSession();
    try {
      Arrays.stream(operations).forEach(Runnable::run);
      SqlSessions.commitAndCloseSqlSession();
    } catch (Exception e) {
      SqlSessions.rollbackAndCloseSqlSession();
      throw e;
    }
  }
}
