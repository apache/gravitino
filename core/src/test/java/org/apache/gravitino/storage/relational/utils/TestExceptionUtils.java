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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.BatchUpdateException;
import java.sql.SQLException;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityAlreadyExistsException;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.exceptions.OptimisticLockException;
import org.apache.ibatis.exceptions.PersistenceException;
import org.junit.jupiter.api.Test;

public class TestExceptionUtils {

  @Test
  public void testConcurrentModificationMessage() {
    OptimisticLockException e =
        ExceptionUtils.concurrentModification(
            Entity.EntityType.CATALOG, NameIdentifier.of("m1", "c1"));

    assertEquals(
        "The catalog m1.c1 was modified concurrently; retry the operation", e.getMessage());
  }

  @Test
  public void testConcurrentChildModificationMessage() {
    OptimisticLockException e =
        ExceptionUtils.concurrentChildModification(
            Entity.EntityType.SCHEMA, Entity.EntityType.CATALOG, NameIdentifier.of("m1", "c1"));

    assertEquals(
        "A schema under catalog m1.c1 was modified concurrently; retry the operation",
        e.getMessage());
  }

  @Test
  public void testIsLockWaitTimeout() {
    for (SQLException timeout :
        List.of(
            // The MySQL driver reports 1205 as SQLState 40001 or HY000 depending on its version.
            new SQLException("MySQL lock wait timeout", "40001", 1205),
            new SQLException("MySQL lock wait timeout", "HY000", 1205),
            new SQLException("H2 lock wait timeout", "HYT00", 50200),
            new SQLException("PostgreSQL lock timeout", "55P03"))) {
      assertTrue(ExceptionUtils.isLockWaitTimeout(timeout), timeout.getMessage());
      // MyBatis wraps the driver exception, possibly more than once.
      assertTrue(
          ExceptionUtils.isLockWaitTimeout(new RuntimeException(new PersistenceException(timeout))),
          timeout.getMessage());
    }

    for (Throwable other :
        List.of(
            new SQLException("MySQL deadlock", "40001", 1213),
            new SQLException("H2 deadlock", "40001", 40001),
            new SQLException("PostgreSQL deadlock", "40P01"),
            new SQLException("serialization failure", "40001"),
            new SQLException("H2 statement timeout", "57014", 57014),
            new SQLException("connection lost", "08006"),
            new PersistenceException("no SQL cause"))) {
      assertFalse(ExceptionUtils.isLockWaitTimeout(other), other.getMessage());
    }
  }

  @Test
  public void testLockWaitTimeoutAsConflict() {
    PersistenceException timeout =
        new PersistenceException(new SQLException("H2 lock wait timeout", "HYT00", 50200));
    RuntimeException conflict =
        ExceptionUtils.lockWaitTimeoutAsConflict(
            timeout, Entity.EntityType.TABLE, NameIdentifier.of("m1", "c1", "s1", "t1"));
    assertInstanceOf(OptimisticLockException.class, conflict);
    assertSame(timeout, conflict.getCause());
    assertEquals(
        "The table m1.c1.s1.t1 could not be changed because a concurrent operation held its lock"
            + " longer than the backend's lock wait timeout; retry the operation",
        conflict.getMessage());

    assertEquals(
        "The batch of 2 entities could not be changed because a concurrent operation held its"
            + " lock longer than the backend's lock wait timeout; retry the operation",
        ExceptionUtils.lockWaitTimeoutAsConflict(timeout, "The batch of 2 entities").getMessage());

    // A conflict is never wrapped again, and any other failure is returned as is.
    OptimisticLockException existing = new OptimisticLockException(timeout, "already a conflict");
    assertSame(existing, ExceptionUtils.lockWaitTimeoutAsConflict(existing, "The table t1"));
    PersistenceException deadlock =
        new PersistenceException(new SQLException("MySQL deadlock", "40001", 1213));
    assertSame(deadlock, ExceptionUtils.lockWaitTimeoutAsConflict(deadlock, "The table t1"));
  }

  @Test
  public void testCheckSQLExceptionReportsLockWaitTimeoutAsConflict() {
    // Services call checkSQLException before the store sees the failure, so the timeout must be
    // classified here; the backend converter would turn it into an IOException.
    PersistenceException timeout =
        new PersistenceException(new SQLException("MySQL lock wait timeout", "40001", 1205));
    OptimisticLockException conflict =
        assertThrows(
            OptimisticLockException.class,
            () ->
                ExceptionUtils.checkSQLException(timeout, Entity.EntityType.TABLE, "m1.c1.s1.t1"));
    assertSame(timeout, conflict.getCause());
    assertTrue(
        conflict.getMessage().startsWith("The table m1.c1.s1.t1 could not be changed"),
        conflict.getMessage());
  }

  @Test
  void testLockWaitTimeoutInBatchExceptionChain() {
    BatchUpdateException batch = new BatchUpdateException("batch failed", new int[0]);
    batch.setNextException(new SQLException("statement failed"));
    batch.setNextException(new SQLException("MySQL lock wait timeout", null, 1205));
    PersistenceException failure = new PersistenceException(batch);

    assertTrue(ExceptionUtils.isLockWaitTimeout(failure));
    OptimisticLockException conflict =
        assertThrows(
            OptimisticLockException.class,
            () ->
                ExceptionUtils.checkSQLException(failure, Entity.EntityType.TABLE, "m1.c1.s1.t1"));
    assertSame(failure, conflict.getCause());
    assertTrue(conflict.getMessage().contains("m1.c1.s1.t1"));
  }

  @Test
  void testSharedDuplicateKeyClassifier() {
    for (SQLException duplicate :
        List.of(
            new SQLException("MySQL duplicate", null, 1062),
            new SQLException("H2 duplicate", "23505", 23505),
            new SQLException("PostgreSQL duplicate", "23505"))) {
      assertTrue(ExceptionUtils.isDuplicateKey(duplicate));
      BatchUpdateException batch = new BatchUpdateException("batch failed", new int[0]);
      batch.setNextException(new SQLException("lock timeout", "55P03"));
      batch.setNextException(duplicate);
      assertTrue(ExceptionUtils.isDuplicateKey(new PersistenceException(batch)));
    }
    assertFalse(ExceptionUtils.isDuplicateKey(new SQLException("foreign key", "23503")));
    assertFalse(ExceptionUtils.isDuplicateKey(new SQLException("lock timeout", "HY000", 1205)));
    assertFalse(ExceptionUtils.isDuplicateKey(new IllegalStateException("not a SQL failure")));
    assertFalse(ExceptionUtils.isDuplicateKey(null));
  }

  @Test
  void testDuplicateKeyTakesPrecedenceOverChainedLockWaitTimeout() {
    for (int backend = 0; backend < 3; backend++) {
      for (boolean duplicateFirst : List.of(true, false)) {
        SQLException duplicate =
            new SQLException(
                "duplicate key", backend == 0 ? "23000" : "23505", backend == 0 ? 1062 : 23505);
        SQLException timeout =
            new SQLException(
                "lock wait timeout",
                backend == 0 ? "40001" : backend == 1 ? "HYT00" : "55P03",
                backend == 0 ? 1205 : backend == 1 ? 50200 : 0);
        BatchUpdateException batch = new BatchUpdateException("batch failed", new int[0]);
        batch.setNextException(duplicateFirst ? duplicate : timeout);
        batch.setNextException(duplicateFirst ? timeout : duplicate);
        PersistenceException failure = new PersistenceException(batch);

        assertTrue(ExceptionUtils.isLockWaitTimeout(failure));
        // Custom association messages must not classify the mixed chain before checkSQLException.
        assertSame(failure, ExceptionUtils.lockWaitTimeoutAsConflict(failure, "The association"));
        ExceptionUtils.throwIfLockWaitTimeout(failure, "The association");
        EntityAlreadyExistsException exists =
            assertThrows(
                EntityAlreadyExistsException.class,
                () ->
                    ExceptionUtils.checkSQLException(
                        failure, Entity.EntityType.TABLE, "m1.c1.s1.t1"));
        assertSame(failure, exists.getCause());
        assertEquals("The TABLE entity: m1.c1.s1.t1 already exists.", exists.getMessage());
      }
    }
  }

  @Test
  void testCauseOfChainedSqlExceptionIsInspected() {
    SQLException batch = new SQLException("batch failed");
    SQLException chained =
        new SQLException(
            "statement failed", new RuntimeException(new SQLException("lock timeout", "55P03")));
    batch.setNextException(chained);

    assertTrue(ExceptionUtils.isLockWaitTimeout(new PersistenceException(batch)));
  }

  @Test
  void testOtherSqlClassificationsInspectNextExceptions() {
    SQLException batch = new SQLException("batch failed");
    batch.setNextException(new SQLException("duplicate key", "23505"));
    assertTrue(ExceptionUtils.hasSqlException(batch, Set.of("23505"), Set.of(1062)));
    assertFalse(ExceptionUtils.isLockWaitTimeout(batch));

    SQLException deadlock = new SQLException("batch failed");
    deadlock.setNextException(new SQLException("MySQL deadlock", "40001", 1213));
    assertTrue(ExceptionUtils.hasSqlException(deadlock, Set.of("40P01"), Set.of(1213)));
    assertFalse(ExceptionUtils.isLockWaitTimeout(deadlock));
  }

  @Test
  void testExceptionCyclesTerminateWithoutMatch() {
    SQLException first = new SQLException("first");
    SQLException second = new SQLException("second");
    first.setNextException(second);
    second.setNextException(first);
    first.initCause(new RuntimeException(second));

    assertTimeoutPreemptively(
        Duration.ofSeconds(5), () -> assertFalse(ExceptionUtils.isLockWaitTimeout(first)));
  }

  @Test
  void testSharedCauseDoesNotHideChainedTimeout() {
    SQLException first = new SQLException("first");
    SQLException second = new SQLException("second");
    first.initCause(second);
    first.setNextException(second);
    second.setNextException(new SQLException("H2 lock timeout", "HYT00", 50200));

    assertTrue(ExceptionUtils.isLockWaitTimeout(first));
    assertFalse(ExceptionUtils.isLockWaitTimeout(null));
    assertFalse(ExceptionUtils.isLockWaitTimeout(new SQLException("no SQLState")));
  }
}
