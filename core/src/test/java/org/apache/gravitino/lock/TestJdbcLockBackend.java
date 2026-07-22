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

package org.apache.gravitino.lock;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.gravitino.NameIdentifier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestJdbcLockBackend {

  private BasicDataSource dataSource;
  private JdbcLockBackend backend;

  @BeforeEach
  void setup() {
    // Per-test in-memory H2 database (named so that the per-test DataSource sees its own).
    String url = "jdbc:h2:mem:lock_" + UUID.randomUUID().toString().replace('-', '_');
    dataSource = new BasicDataSource();
    dataSource.setUrl(url);
    dataSource.setUsername("sa");
    dataSource.setPassword("");
    dataSource.setDriverClassName("org.h2.Driver");
    dataSource.setMaxTotal(4);
    dataSource.setDefaultAutoCommit(false);
    backend = new JdbcLockBackend(dataSource, JdbcDialect.H2, 10_000L);
  }

  @AfterEach
  void teardown() throws Exception {
    if (backend != null) {
      backend.close();
    }
    if (dataSource != null && !dataSource.isClosed()) {
      dataSource.close();
    }
  }

  @Test
  void backendNameIsJdbc() {
    Assertions.assertEquals("jdbc", backend.name());
  }

  @Test
  void initSchemaCreatesLockTable() throws Exception {
    try (Connection c = dataSource.getConnection();
        Statement s = c.createStatement();
        ResultSet rs = s.executeQuery("SELECT COUNT(*) FROM " + JdbcDialect.LOCK_TABLE)) {
      Assertions.assertTrue(rs.next());
      Assertions.assertEquals(0, rs.getInt(1));
    }
  }

  @Test
  void acquireWriteAndReleaseInsertsLockRow() throws Exception {
    NameIdentifier ident = NameIdentifier.of("metalake", "catalog", "db", "tbl");
    try (LockHandle handle = backend.acquire(ident, LockType.WRITE)) {
      Assertions.assertNotNull(handle);
    }

    // After release, the lock-path rows persist (the table is monotonic in v1).
    try (Connection c = dataSource.getConnection();
        Statement s = c.createStatement();
        ResultSet rs = s.executeQuery("SELECT COUNT(*) FROM " + JdbcDialect.LOCK_TABLE)) {
      Assertions.assertTrue(rs.next());
      // Five rows: /, /metalake, /metalake/catalog, /metalake/catalog/db,
      // /metalake/catalog/db/tbl
      Assertions.assertEquals(5, rs.getInt(1));
    }
  }

  @Test
  void acquireRootLockUsesSinglePath() {
    try (LockHandle handle = backend.acquire(LockManager.ROOT, LockType.WRITE)) {
      Assertions.assertNotNull(handle);
    }
  }

  @Test
  void closeIsIdempotent() {
    LockHandle handle = backend.acquire(NameIdentifier.of("ns"), LockType.READ);
    handle.close();
    Assertions.assertDoesNotThrow(handle::close);
  }

  @Test
  void reentrantAcquisitionOnSameLeafThrows() {
    NameIdentifier ident = NameIdentifier.of("metalake", "catalog");
    try (LockHandle outer = backend.acquire(ident, LockType.WRITE)) {
      Assertions.assertThrows(
          IllegalStateException.class, () -> backend.acquire(ident, LockType.WRITE));
    }
    // After release, a fresh acquisition on the same leaf is allowed again.
    try (LockHandle h = backend.acquire(ident, LockType.WRITE)) {
      Assertions.assertNotNull(h);
    }
  }

  @Test
  void pathsRootDownProducesTreeWalkOrder() {
    Assertions.assertEquals(List.of("/"), JdbcLockBackend.pathsRootDown(LockManager.ROOT));
    Assertions.assertEquals(
        List.of("/", "/metalake"), JdbcLockBackend.pathsRootDown(NameIdentifier.of("metalake")));
    Assertions.assertEquals(
        List.of("/", "/m", "/m/c", "/m/c/db", "/m/c/db/tbl"),
        JdbcLockBackend.pathsRootDown(NameIdentifier.of("m", "c", "db", "tbl")));
  }

  /**
   * Two threads acquiring WRITE on the same path must serialize. The second waits for the first to
   * release before it can proceed.
   */
  @Test
  void writeWriteOnSamePathSerializes() throws Exception {
    NameIdentifier ident = NameIdentifier.of("ns", "shared");
    CountDownLatch firstAcquired = new CountDownLatch(1);
    ExecutorService pool = Executors.newFixedThreadPool(2);
    try {
      Future<Long> first =
          pool.submit(
              () -> {
                LockHandle h = backend.acquire(ident, LockType.WRITE);
                firstAcquired.countDown();
                // Hold the lock long enough for the second thread to queue behind us.
                Thread.sleep(500);
                long t = System.nanoTime();
                h.close();
                return t;
              });

      Future<Long> second =
          pool.submit(
              () -> {
                Assertions.assertTrue(firstAcquired.await(5, TimeUnit.SECONDS));
                // Blocks here until the first thread releases.
                LockHandle h = backend.acquire(ident, LockType.WRITE);
                long t = System.nanoTime();
                h.close();
                return t;
              });

      long firstReleaseNanos = first.get(10, TimeUnit.SECONDS);
      long secondAcquireNanos = second.get(10, TimeUnit.SECONDS);
      Assertions.assertTrue(
          secondAcquireNanos >= firstReleaseNanos,
          "second WRITE acquired before first WRITE released — locks did not serialize");
    } finally {
      pool.shutdownNow();
    }
  }
}
