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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
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
      } catch (Exception e) {
        SqlSessions.rollbackAndCloseSqlSession();
        throw e;
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
      } catch (Exception e) {
        SqlSessions.rollbackAndCloseSqlSession();
        throw e;
      }
    }
  }

  /**
   * This method is used to perform a database operation without a commit and fetch the result. If
   * the operation fails, will throw the RuntimeException.
   *
   * <p><b>Warning:</b> This method is not transaction-safe and should <strong>not</strong> be used
   * in contexts where connection or transaction management is handled externally. If the caller
   * does not explicitly commit or close the session, it may lead to <b>connection leaks</b> or
   * <b>transaction hangs</b>.
   *
   * <p><b>Recommended:</b> For consistent transactional behavior, consider using {@link
   * #callWithoutCommit(Class, Function)} with {@link #doMultipleWithCommit(Operation[])}.
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
   * <p><b>Warning:</b> This method bypasses any transactional boundary and can lead to
   * <b>connection leaks</b> or <b>unmanaged transactions</b> if used improperly.
   *
   * <p><b>Recommended:</b> Use {@link #opWithoutCommit(Class, Consumer)} in combination with {@link
   * #doMultipleWithCommit(Operation[])} for safe transactional operations.
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
      } catch (Exception e) {
        throw e;
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
      } catch (Exception e) {
        SqlSessions.rollbackAndCloseSqlSession();
        throw e;
      }
    }
  }

  /**
   * Creates a database operation with the given mapper and function, to be executed without
   * committing. Intended for use inside a transactional block like {@link
   * SessionUtils#doMultipleWithCommit(Operation[])}}.
   *
   * @param mapperClass the mapper class
   * @param function the operation logic
   * @param <T> the mapper type
   * @param <R> the result type
   * @return the wrapped operation
   */
  public static <T, R> Operation<T, R> callWithoutCommit(
      Class<T> mapperClass, Function<T, R> function) {
    return new Operation<>(mapperClass, function);
  }

  /**
   * Creates a database operation that performs an action using the given mapper, without returning
   * a result or committing the transaction. The operation is intended to be used within a
   * higher-level transactional context such as {@link
   * SessionUtils#doMultipleWithCommit(Operation[])}}.
   *
   * @param mapperClass the mapper class to be used for the operation
   * @param consumer the operation logic to be performed using the mapper
   * @param <T> the type of the mapper
   * @return an {@link Operation} that performs the given consumer action
   */
  public static <T> Operation<T, Void> opWithoutCommit(Class<T> mapperClass, Consumer<T> consumer) {
    return new Operation<>(
        mapperClass,
        mapper -> {
          consumer.accept(mapper);
          return null;
        });
  }

  /**
   * Executes multiple database operations within a single transaction. If all operations succeed,
   * the transaction is committed. If any operation fails, the entire transaction is rolled back.
   *
   * <p>Example usage:
   *
   * <pre>{@code
   * int[] deleteCount1 = new int[1];
   * int[] deleteCount2 = new int[1];
   *
   * List<Object> results = SessionUtils.doMultipleWithCommit(
   *     SessionUtils.opWithoutCommit(ModelVersionMetaMapper.class, mapper ->
   *         deleteCount1[0] = mapper.deleteModelVersionMetasByLegacyTimeline(legacyTimeline, limit)),
   *     SessionUtils.callWithoutCommit(ModelVersionAliasRelMapper.class, mapper ->
   *         deleteCount2[0] = mapper.deleteModelVersionAliasRelsByLegacyTimeline(legacyTimeline, limit))
   * );
   *
   * System.out.println("Deleted ModelVersionMeta count: " + deleteCount1[0]);
   * System.out.println("Deleted ModelVersionAliasRel count: " + deleteCount2[0]);
   * }</pre>
   *
   * @param operations one or more database operations to execute transactionally
   * @return a list of results from each operation; null for operations without return value
   */
  public static List<Object> doMultipleWithCommit(Operation<?, ?>... operations) {
    try (SqlSession session = SqlSessions.getSqlSession()) {
      try {
        List<Object> results = new ArrayList<>();
        for (Operation<?, ?> op : operations) {
          results.add(op.execute());
        }
        SqlSessions.commitAndCloseSqlSession();
        return results;
      } catch (Throwable t) {
        SqlSessions.rollbackAndCloseSqlSession();
        throw t;
      }
    }
  }

  /**
   * Represents a database operation that can be executed with a given mapper. The operation
   * encapsulates a mapper class and a function that accepts the mapper instance and produces a
   * result.
   *
   * <p>This class is designed to be used with {@link
   * SessionUtils#doMultipleWithCommit(Operation[])} to batch execute multiple operations within a
   * single transaction.
   *
   * @param <T> the type of the mapper class
   * @param <R> the type of the result produced by the operation
   */
  public static class Operation<T, R> {

    /** The mapper class to be used for this operation */
    private final Class<T> mapperClass;

    /** The function representing the operation logic, applied to the mapper */
    private final Function<T, R> function;

    /**
     * Constructs an Operation with the given mapper class and function.
     *
     * @param mapperClass the class of the mapper to retrieve
     * @param function the function to execute using the mapper
     */
    private Operation(Class<T> mapperClass, Function<T, R> function) {
      this.mapperClass = mapperClass;
      // Unsafe cast but user responsible for correct type
      this.function = function;
    }

    /**
     * Executes the encapsulated operation by obtaining a mapper instance from {@link SqlSessions}
     * and applying the function to it.
     *
     * @return the result of applying the function on the mapper
     */
    R execute() {
      T mapper = SqlSessions.getMapper(mapperClass);
      return function.apply(mapper);
    }
  }
}
