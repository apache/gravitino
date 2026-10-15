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

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Deque;
import java.util.IdentityHashMap;
import java.util.Locale;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityAlreadyExistsException;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.exceptions.OptimisticLockException;
import org.apache.gravitino.storage.relational.converters.SQLExceptionConverterFactory;

public class ExceptionUtils {

  /** MySQL error code of a lock wait timeout (ER_LOCK_WAIT_TIMEOUT). */
  private static final int MYSQL_LOCK_WAIT_TIMEOUT = 1205;

  /** H2 error code of a lock wait timeout (LOCK_TIMEOUT_1, SQLState HYT00). */
  private static final int H2_LOCK_TIMEOUT = 50200;

  /** PostgreSQL SQLState of a lock that is not available within lock_timeout. */
  private static final String POSTGRESQL_LOCK_NOT_AVAILABLE = "55P03";

  private static final Set<String> LOCK_WAIT_TIMEOUT_SQL_STATES =
      Set.of(POSTGRESQL_LOCK_NOT_AVAILABLE);

  private static final Set<Integer> LOCK_WAIT_TIMEOUT_VENDOR_CODES =
      Set.of(MYSQL_LOCK_WAIT_TIMEOUT, H2_LOCK_TIMEOUT);

  private static final Set<String> DUPLICATE_KEY_SQL_STATES = Set.of("23505");
  private static final Set<Integer> DUPLICATE_KEY_VENDOR_CODES = Set.of(1062);

  private ExceptionUtils() {}

  /**
   * Converts a failure of a write whose cause is a {@link SQLException} into the matching Gravitino
   * exception. A lock wait timeout becomes an {@link OptimisticLockException}, see {@link
   * #lockWaitTimeoutAsConflict(RuntimeException, String)}. It is classified here, before the
   * backend converter, because the converter reports an unrecognised SQL error as an {@link
   * IOException}, which callers turn into an internal error. A duplicate-key failure takes
   * precedence over a timeout in the same exception chain: retrying cannot resolve that violation.
   *
   * @param re The failure of the write.
   * @param type The type of the entity being written.
   * @param entityName The name of the entity being written.
   * @throws IOException If the SQL error has no more specific mapping.
   * @throws EntityAlreadyExistsException If the failure contains a duplicate-key violation.
   * @throws OptimisticLockException If the failure contains a lock wait timeout without a
   *     duplicate-key violation.
   */
  public static void checkSQLException(
      RuntimeException re, Entity.EntityType type, String entityName) throws IOException {
    if (isDuplicateKey(re)) {
      throw new EntityAlreadyExistsException(
          re, "The %s entity: %s already exists.", type.name(), entityName);
    }
    throwIfLockWaitTimeout(
        re, String.format("The %s %s", type.name().toLowerCase(Locale.ROOT), entityName));
    if (re.getCause() instanceof SQLException) {
      SQLExceptionConverterFactory.getConverter()
          .toGravitinoException((SQLException) re.getCause(), type, entityName);
    }
  }

  /**
   * Creates an {@link OptimisticLockException} for an entity that was modified concurrently by
   * another writer, which makes the version-guarded write fail.
   *
   * @param type The type of the entity that was modified concurrently.
   * @param identifier The identifier of the entity that was modified concurrently.
   * @return The {@link OptimisticLockException} to throw.
   */
  public static OptimisticLockException concurrentModification(
      Entity.EntityType type, NameIdentifier identifier) {
    return new OptimisticLockException(
        "The %s %s was modified concurrently; retry the operation",
        type.name().toLowerCase(Locale.ROOT), identifier);
  }

  /**
   * Creates an {@link OptimisticLockException} for a child entity that was modified concurrently
   * while its parent was being deleted in cascade mode.
   *
   * @param childType The type of the child entity that was modified concurrently.
   * @param parentType The type of the parent entity being operated on.
   * @param parentIdentifier The identifier of the parent entity being operated on.
   * @return The {@link OptimisticLockException} to throw.
   */
  public static OptimisticLockException concurrentChildModification(
      Entity.EntityType childType, Entity.EntityType parentType, NameIdentifier parentIdentifier) {
    return new OptimisticLockException(
        "A %s under %s %s was modified concurrently; retry the operation",
        childType.name().toLowerCase(Locale.ROOT),
        parentType.name().toLowerCase(Locale.ROOT),
        parentIdentifier);
  }

  /**
   * Throws a write that failed on a lock wait timeout as a conflict, and returns otherwise. A
   * service calls this before {@link #checkSQLException} when the type and name it reports for
   * other SQL errors do not describe what was written, for example the metadata object of a tag
   * association.
   *
   * @param re The failure of the write.
   * @param target What was being written, as the subject of the message.
   * @throws OptimisticLockException If the failure is a lock wait timeout without a duplicate-key
   *     violation.
   * @see #lockWaitTimeoutAsConflict(RuntimeException, String)
   */
  public static void throwIfLockWaitTimeout(RuntimeException re, String target) {
    RuntimeException conflict = lockWaitTimeoutAsConflict(re, target);
    if (conflict != re) {
      throw conflict;
    }
  }

  /**
   * Returns whether a failure was caused by a statement that waited on a row lock longer than the
   * backend's lock wait timeout. MySQL reports error code 1205, H2 error code 50200 (SQLState
   * HYT00), and PostgreSQL SQLState 55P03 when lock_timeout is set. The MySQL and H2 vendor codes
   * are matched rather than their SQLState, because the MySQL driver reports a lock wait timeout as
   * SQLState 40001 or HY000 depending on its version, and 40001 is also used for deadlocks.
   *
   * <p>Deadlocks are deliberately not included: a deadlock on a write path points at an
   * inconsistent lock order, which should be fixed on that path rather than reported as a conflict.
   *
   * @param failure The failure to inspect, including its causes and chained SQL exceptions, or
   *     null.
   * @return {@code true} if a lock wait timeout is found.
   */
  public static boolean isLockWaitTimeout(@Nullable Throwable failure) {
    return hasSqlException(failure, LOCK_WAIT_TIMEOUT_SQL_STATES, LOCK_WAIT_TIMEOUT_VENDOR_CODES);
  }

  /**
   * Returns whether a failure contains a {@link SQLException} with one of the given SQLStates or
   * vendor error codes. Both causes and {@link SQLException#getNextException()} are inspected;
   * repeated exceptions are visited only once, even when the chains contain cycles.
   *
   * @param failure The failure to inspect, including its causes and chained SQL exceptions, or
   *     null.
   * @param sqlStates The SQLStates to match.
   * @param vendorErrorCodes The vendor error codes to match.
   * @return {@code true} if a matching {@link SQLException} is found.
   */
  public static boolean hasSqlException(
      @Nullable Throwable failure, Set<String> sqlStates, Set<Integer> vendorErrorCodes) {
    if (failure == null) {
      return false;
    }
    Set<Throwable> visited = Collections.newSetFromMap(new IdentityHashMap<>());
    Deque<Throwable> pending = new ArrayDeque<>();
    pending.add(failure);
    while (!pending.isEmpty()) {
      Throwable cause = pending.removeLast();
      if (!visited.add(cause)) {
        continue;
      }
      if (cause instanceof SQLException) {
        SQLException sql = (SQLException) cause;
        if ((sql.getSQLState() != null && sqlStates.contains(sql.getSQLState()))
            || vendorErrorCodes.contains(sql.getErrorCode())) {
          return true;
        }
        if (sql.getNextException() != null) {
          pending.add(sql.getNextException());
        }
      }
      if (cause.getCause() != null) {
        pending.add(cause.getCause());
      }
    }
    return false;
  }

  /**
   * Reports a write of one entity that failed on a lock wait timeout as a conflict.
   *
   * @param failure The failure of the write.
   * @param type The type of the entity being written.
   * @param identifier The identifier of the entity being written.
   * @return An {@link OptimisticLockException} for a lock wait timeout, otherwise {@code failure}.
   * @see #lockWaitTimeoutAsConflict(RuntimeException, String)
   */
  public static RuntimeException lockWaitTimeoutAsConflict(
      RuntimeException failure, Entity.EntityType type, NameIdentifier identifier) {
    return lockWaitTimeoutAsConflict(
        failure, String.format("The %s %s", type.name().toLowerCase(Locale.ROOT), identifier));
  }

  /**
   * Reports a write that failed on a lock wait timeout as a conflict. The statement waited behind a
   * concurrent writer of the same rows until the backend aborted it, and the store rolls the
   * transaction back, so the store write can be retried. Callers must account for any external side
   * effects that occurred before the store write before retrying the whole operation. Any other
   * failure, including one that is already an {@link OptimisticLockException}, is returned
   * unchanged. A failure containing a duplicate-key violation is also returned unchanged. Callers
   * that subsequently use {@link #checkSQLException} report that violation as an already-exists
   * error. Store write paths that do not use that classifier retain the original backend failure,
   * including for a chain containing both a duplicate key and a lock wait timeout.
   *
   * @param failure The failure of the write.
   * @param target What was being written, as the subject of the message, for example "The table
   *     m1.c1.s1.t1".
   * @return An {@link OptimisticLockException} for a lock wait timeout, otherwise {@code failure}.
   */
  public static RuntimeException lockWaitTimeoutAsConflict(
      RuntimeException failure, String target) {
    if (failure instanceof OptimisticLockException
        || !isLockWaitTimeout(failure)
        || isDuplicateKey(failure)) {
      return failure;
    }
    return new OptimisticLockException(
        failure,
        "%s could not be changed because a concurrent operation held its lock longer than the"
            + " backend's lock wait timeout; retry the operation",
        target);
  }

  /**
   * Returns whether a failure contains a duplicate primary-key or unique-key violation.
   *
   * <p>Matches MySQL vendor code 1062 and PostgreSQL/H2 SQLState 23505 through causes and chained
   * JDBC exceptions, using cycle protection. This only detects the violation; callers decide
   * whether it represents an already-existing entity or a concurrent statistics write.
   *
   * @param failure the failure to inspect, including its causes and chained JDBC exceptions, or
   *     null
   * @return true if a duplicate-key violation is found
   */
  public static boolean isDuplicateKey(@Nullable Throwable failure) {
    return hasSqlException(failure, DUPLICATE_KEY_SQL_STATES, DUPLICATE_KEY_VENDOR_CODES);
  }
}
