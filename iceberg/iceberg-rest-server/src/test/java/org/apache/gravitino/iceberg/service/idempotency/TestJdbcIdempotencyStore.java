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

package org.apache.gravitino.iceberg.service.idempotency;

import com.google.common.collect.ImmutableMap;
import java.io.StringReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.util.Locale;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.iceberg.common.IcebergConfig;
import org.apache.gravitino.iceberg.common.idempotency.IdempotencyRecord;
import org.apache.gravitino.iceberg.common.idempotency.IdempotencyStore;
import org.apache.gravitino.iceberg.common.idempotency.IdempotencyStore.ReserveResult;
import org.apache.gravitino.iceberg.common.idempotency.IdempotencyStoreFactory;
import org.apache.gravitino.storage.relational.TestJDBCBackend;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.apache.ibatis.jdbc.ScriptRunner;
import org.apache.ibatis.session.SqlSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.mockito.Mockito;

/**
 * Shared {@link JdbcIdempotencyStore} test logic exercised against the same relational backend
 * matrix as core metadata service tests. {@link TestJDBCBackend} initializes H2 by default, adds
 * MySQL and PostgreSQL when {@code dockerTest=true}, and empties backend tables before each
 * invocation.
 */
public class TestJdbcIdempotencyStore extends TestJDBCBackend {

  private static final String KEY = "017f22e2-79b0-7cc3-98c4-dc0c0c07398f";
  private static final String OTHER_KEY = "017f22e2-79b0-7cc3-98c4-dc0c0c073990";
  private static final String BINDING = "POST v1/cat1/namespaces/ns1/tables";
  private static final long FAR_FUTURE_MS = Long.MAX_VALUE;

  private final AtomicLong now = new AtomicLong();
  private JdbcIdempotencyStore store;
  private JdbcIdempotencyStore peer;

  // TestJDBCBackend's BackendTestExtension overwrites GravitinoEnv's singleton "config" and
  // "idGenerator" fields with a backend-only Mockito mock and never restores them. Because the
  // whole iceberg-rest-server module runs in one JVM, that mock would leak into later test classes
  // (e.g. credential vending), where MetadataAuthzHelper.enableAuthorization() unboxes the
  // unstubbed config.get(ENABLE_AUTHORIZATION) -> null and NPEs. Snapshot the pre-test fields and
  // restore them after this class so it leaves GravitinoEnv exactly as it found it.
  private Object originalConfig;
  private Object originalIdGenerator;

  /** Saves the process-wide environment before the backend test fixture replaces it. */
  @BeforeAll
  public void snapshotGravitinoEnv() throws IllegalAccessException {
    originalConfig = FieldUtils.readField(GravitinoEnv.getInstance(), "config", true);
    originalIdGenerator = FieldUtils.readField(GravitinoEnv.getInstance(), "idGenerator", true);
  }

  /** Restores the environment so other REST server tests retain their original configuration. */
  @AfterAll
  public void restoreGravitinoEnv() throws IllegalAccessException {
    FieldUtils.writeField(GravitinoEnv.getInstance(), "config", originalConfig, true);
    FieldUtils.writeField(GravitinoEnv.getInstance(), "idGenerator", originalIdGenerator, true);
  }

  /** Creates two store instances sharing the test database and a controllable clock. */
  @BeforeEach
  public void prepareIdempotencyStore() {
    now.set(1_000_000L);
    Clock clock = Mockito.mock(Clock.class);
    Mockito.when(clock.millis()).thenAnswer(invocation -> now.get());
    store = new JdbcIdempotencyStore(clock);
    peer = new JdbcIdempotencyStore(clock);
    // A long interval keeps the background task out of the way: every test below drives the purge
    // itself, so a concurrent scheduled run would only make assertions racy.
    store.initialize(ImmutableMap.of("idempotency-cleanup-interval", "PT1H"));
    peer.initialize(ImmutableMap.of("idempotency-cleanup-interval", "PT1H"));
  }

  /** Stops the stores' cleanup tasks without closing the fixture's connection pool. */
  @AfterEach
  public void closeIdempotencyStore() {
    store.close();
    peer.close();
  }

  @TestTemplate
  void testReserveClaimsKeyOnlyOnce() {
    Assertions.assertEquals(ReserveResult.RESERVED, store.reserve(KEY, BINDING, 1, FAR_FUTURE_MS));
    Assertions.assertEquals(ReserveResult.DUPLICATE, store.reserve(KEY, BINDING, 1, FAR_FUTURE_MS));
    Assertions.assertEquals(
        ReserveResult.RESERVED, store.reserve(OTHER_KEY, BINDING, 1, FAR_FUTURE_MS));
  }

  @TestTemplate
  void testConcurrentReserveOnOneKeyHasExactlyOneWinner() throws InterruptedException {
    int threads = 16;
    ExecutorService executor = Executors.newFixedThreadPool(threads);
    CountDownLatch start = new CountDownLatch(1);
    CountDownLatch done = new CountDownLatch(threads);
    AtomicInteger reserved = new AtomicInteger();
    AtomicInteger failed = new AtomicInteger();

    try {
      for (int i = 0; i < threads; i++) {
        long claim = i + 1L;
        JdbcIdempotencyStore contender = i % 2 == 0 ? store : peer;
        executor.submit(
            () -> {
              try {
                start.await();
                if (contender.reserve(KEY, BINDING, claim, FAR_FUTURE_MS)
                    == ReserveResult.RESERVED) {
                  reserved.incrementAndGet();
                }
              } catch (Exception e) {
                // A duplicate-key error rather than a zero-row insert would break the contract as
                // surely as two winners would, so count it instead of letting it vanish.
                failed.incrementAndGet();
              } finally {
                done.countDown();
              }
            });
      }
      start.countDown();
      Assertions.assertTrue(done.await(60, TimeUnit.SECONDS));
    } finally {
      executor.shutdownNow();
    }

    Assertions.assertEquals(0, failed.get(), "reserve should never raise on a lost race");
    Assertions.assertEquals(1, reserved.get(), "exactly one caller should claim the key");
  }

  @TestTemplate
  void testLoadReturnsEmptyUntilFinalized() {
    store.reserve(KEY, BINDING, 1, FAR_FUTURE_MS);
    Assertions.assertEquals(Optional.empty(), store.load(KEY));

    store.finalizeRecord(KEY, 1, 200, "{\"a\":1}");

    IdempotencyRecord record = store.load(KEY).orElseThrow(AssertionError::new);
    Assertions.assertTrue(record.isFinalized());
    Assertions.assertEquals(KEY, record.idempotencyKey());
    Assertions.assertEquals(BINDING, record.operationBinding());
    Assertions.assertEquals(200, record.httpStatus().intValue());
    Assertions.assertEquals("{\"a\":1}", record.responseSummary());
    Assertions.assertEquals(FAR_FUTURE_MS, record.expiresAtMs());
  }

  @TestTemplate
  void testFinalizeWithoutBodyIsReplayable() {
    store.reserve(KEY, BINDING, 1, FAR_FUTURE_MS);
    store.finalizeRecord(KEY, 1, 204, null);

    IdempotencyRecord record = store.load(KEY).orElseThrow(AssertionError::new);
    Assertions.assertEquals(204, record.httpStatus().intValue());
    Assertions.assertNull(record.responseSummary());
  }

  @TestTemplate
  void testFinalizeDoesNotResurrectPurgedRecord() {
    store.finalizeRecord(KEY, 1, 200, "{}");
    Assertions.assertEquals(Optional.empty(), store.load(KEY));
  }

  @TestTemplate
  void testReleaseFreesReservationButKeepsFinalizedResponse() {
    store.reserve(KEY, BINDING, 1, FAR_FUTURE_MS);
    store.release(KEY, 1);
    Assertions.assertEquals(ReserveResult.RESERVED, store.reserve(KEY, BINDING, 1, FAR_FUTURE_MS));

    store.finalizeRecord(KEY, 1, 201, "{\"b\":2}");
    store.release(KEY, 1);
    Assertions.assertTrue(store.load(KEY).isPresent(), "a finalized response survives a release");
  }

  @TestTemplate
  void testPurgeExpiredRemovesOnlyExpiredRecords() {
    long now = this.now.get();
    store.reserve(KEY, BINDING, 1, now - 1);
    store.reserve(OTHER_KEY, BINDING, 1, FAR_FUTURE_MS);

    Assertions.assertEquals(1, store.purgeExpired(now));

    Assertions.assertEquals(ReserveResult.RESERVED, store.reserve(KEY, BINDING, 1, FAR_FUTURE_MS));
    Assertions.assertEquals(
        ReserveResult.DUPLICATE,
        store.reserve(OTHER_KEY, BINDING, 1, FAR_FUTURE_MS),
        "an unexpired record must survive the purge");
  }

  @TestTemplate
  void testPurgeTaskRemovesOnlyExpiredRecords() throws Exception {
    store.reserve(KEY, BINDING, 1, now.get());
    store.reserve(OTHER_KEY, BINDING, 1, FAR_FUTURE_MS);
    store.finalizeRecord(OTHER_KEY, 1, 200, "{}");
    store.purgeExpiredRecords();
    Assertions.assertEquals(1, rowCount());
    Assertions.assertTrue(store.load(OTHER_KEY).isPresent());
  }

  @TestTemplate
  void testPurgeExpiredDeletesMoreThanOneBatch() {
    long now = this.now.get();
    int records = JdbcIdempotencyStore.PURGE_BATCH_SIZE + 5;
    for (int i = 0; i < records; i++) {
      store.reserve(newKey(), BINDING, 1, now - 1);
    }
    store.reserve(OTHER_KEY, BINDING, 1, FAR_FUTURE_MS);

    Assertions.assertEquals(records, store.purgeExpired(now));
    Assertions.assertEquals(
        ReserveResult.DUPLICATE, store.reserve(OTHER_KEY, BINDING, 1, FAR_FUTURE_MS));
  }

  @TestTemplate
  void testReplaySurvivesStoreRestartAndIsVisibleToPeer() throws Exception {
    store.reserve(KEY, BINDING, -7, FAR_FUTURE_MS);
    Assertions.assertEquals(ReserveResult.DUPLICATE, peer.reserve(KEY, BINDING, 2, FAR_FUTURE_MS));
    Assertions.assertTrue(peer.load(KEY).isEmpty());
    String response = "{\"metadata\":\"" + "x".repeat(100_000) + "\"}";
    store.finalizeRecord(KEY, -7, 200, response);
    IdempotencyRecord original = peer.load(KEY).orElseThrow();
    Assertions.assertEquals(-7, original.claim());
    store.close();
    try (IdempotencyStore restarted =
        IdempotencyStoreFactory.create(
            new IcebergConfig(ImmutableMap.of("idempotency-store-type", "jdbc")))) {
      Assertions.assertEquals(original, restarted.load(KEY).orElseThrow());
      Assertions.assertEquals(response, restarted.load(KEY).orElseThrow().responseSummary());
    }
    // Closing the store must not close the entity store's connection pool.
    Assertions.assertEquals(original, peer.load(KEY).orElseThrow());
  }

  @TestTemplate
  void testExpiredReservationIsReclaimedAndOldOwnerIsFenced() {
    store.reserve(KEY, BINDING, 1, now.get() + 1);
    now.incrementAndGet();
    Assertions.assertEquals(
        ReserveResult.RESERVED, peer.reserve(KEY, "new binding", 2, FAR_FUTURE_MS));
    store.finalizeRecord(KEY, 1, 500, "stale");
    store.release(KEY, 1);
    Assertions.assertEquals(ReserveResult.DUPLICATE, store.reserve(KEY, BINDING, 3, FAR_FUTURE_MS));
    Assertions.assertTrue(peer.load(KEY).isEmpty());
    peer.finalizeRecord(KEY, 2, 201, "new");
    IdempotencyRecord record = store.load(KEY).orElseThrow();
    Assertions.assertEquals(2, record.claim());
    Assertions.assertEquals("new binding", record.operationBinding());
    Assertions.assertEquals("new", record.responseSummary());
  }

  @TestTemplate
  void testExpiredResponseIsNotLoadedAndCanBeReplacedWithoutPurge() {
    store.reserve(KEY, BINDING, 1, now.get() + 1);
    store.finalizeRecord(KEY, 1, 200, "old");
    now.incrementAndGet();
    Assertions.assertTrue(peer.load(KEY).isEmpty());
    Assertions.assertEquals(ReserveResult.RESERVED, peer.reserve(KEY, BINDING, 2, FAR_FUTURE_MS));
    Assertions.assertTrue(peer.load(KEY).isEmpty());
    peer.finalizeRecord(KEY, 2, 204, null);
    Assertions.assertNull(store.load(KEY).orElseThrow().responseSummary());
  }

  @TestTemplate
  void testFinalizeDoesNotResurrectExpiredReservation() throws Exception {
    store.reserve(KEY, BINDING, 1, now.get());
    store.finalizeRecord(KEY, 1, 200, "expired");
    // Inspect persisted state so a read-time expiry filter cannot hide an incorrect finalization.
    try (SqlSession session =
            SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession();
        var statement = session.getConnection().createStatement();
        var result =
            statement.executeQuery("SELECT http_status FROM iceberg_idempotency_key_meta")) {
      Assertions.assertTrue(result.next());
      Assertions.assertNull(result.getObject(1));
    }
  }

  @TestTemplate
  void testPurgeIncludesExactCutoff() {
    store.reserve(KEY, BINDING, 1, now.get());
    store.reserve(OTHER_KEY, BINDING, 1, now.get() + 1);
    Assertions.assertEquals(1, store.purgeExpired(now.get()));
    Assertions.assertEquals(
        ReserveResult.DUPLICATE, peer.reserve(OTHER_KEY, BINDING, 2, FAR_FUTURE_MS));
  }

  @TestTemplate
  void testKeyCaseNormalizationOnAllOperations() {
    String upper = KEY.toUpperCase(Locale.ROOT);
    store.reserve(upper, BINDING, 1, FAR_FUTURE_MS);
    Assertions.assertEquals(ReserveResult.DUPLICATE, peer.reserve(KEY, BINDING, 2, FAR_FUTURE_MS));
    peer.release(upper, 1);
    Assertions.assertEquals(ReserveResult.RESERVED, store.reserve(KEY, BINDING, 2, FAR_FUTURE_MS));
    peer.finalizeRecord(upper, 2, 200, "{}");
    Assertions.assertEquals(KEY, store.load(upper).orElseThrow().idempotencyKey());
  }

  @TestTemplate
  void testInvalidInputDoesNotReserveAKey() throws Exception {
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> store.reserve("invalid", BINDING, 1, FAR_FUTURE_MS));
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> store.reserve(KEY, null, 1, FAR_FUTURE_MS));
    Assertions.assertThrows(IllegalArgumentException.class, () -> store.load("invalid"));
    Assertions.assertThrows(IllegalArgumentException.class, () -> store.release("invalid", 1));
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> store.finalizeRecord("invalid", 1, 200, null));
    Assertions.assertEquals(0, rowCount());
  }

  @TestTemplate
  void testReservationCommitsIndependentlyOfCallerTransaction() {
    SessionUtils.beginTransaction();
    try {
      store.reserve(KEY, BINDING, 1, FAR_FUTURE_MS);
    } finally {
      SessionUtils.rollbackTransaction();
    }
    Assertions.assertEquals(ReserveResult.DUPLICATE, peer.reserve(KEY, BINDING, 2, FAR_FUTURE_MS));
  }

  @TestTemplate
  void testOnlyOneCallerCanReclaimExpiredKey() throws InterruptedException {
    store.reserve(KEY, BINDING, -1, now.get());
    testConcurrentReserveOnOneKeyHasExactlyOneWinner();
  }

  @TestTemplate
  void testLongBindingIsPreserved() {
    String binding = "POST /" + "x".repeat(1024);
    store.reserve(KEY, binding, 1, FAR_FUTURE_MS);
    store.finalizeRecord(KEY, 1, 200, "{}");
    Assertions.assertEquals(binding, peer.load(KEY).orElseThrow().operationBinding());
  }

  @TestTemplate
  void testCleanupContinuesAfterTransientFailure() throws Exception {
    store.reserve(KEY, BINDING, 1, now.get());
    AtomicInteger attempts = new AtomicInteger();
    CountDownLatch cleaned = new CountDownLatch(1);
    try (JdbcIdempotencyStore scheduled =
        new JdbcIdempotencyStore() {
          @Override
          public int purgeExpired(long beforeMs) {
            if (attempts.incrementAndGet() == 1) {
              throw new IllegalStateException("temporary database failure");
            }
            int deleted = super.purgeExpired(beforeMs);
            cleaned.countDown();
            return deleted;
          }
        }) {
      scheduled.initialize(ImmutableMap.of("idempotency-cleanup-interval", "PT0.01S"));
      Assertions.assertTrue(cleaned.await(10, TimeUnit.SECONDS));
      Assertions.assertTrue(attempts.get() >= 2);
      Assertions.assertEquals(0, rowCount());
    }
  }

  @TestTemplate
  void testMissingSchemaFailsInitializationAndDoesNotHideSqlErrors() throws Exception {
    try (SqlSession session =
            SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true);
        var statement = session.getConnection().createStatement()) {
      statement.execute(
          "ALTER TABLE iceberg_idempotency_key_meta RENAME TO saved_idempotency_keys");
      try {
        Assertions.assertThrows(
            RuntimeException.class,
            () ->
                IdempotencyStoreFactory.create(
                    new IcebergConfig(ImmutableMap.of("idempotency-store-type", "jdbc"))));
        Assertions.assertThrows(
            RuntimeException.class, () -> store.reserve(KEY, BINDING, 1, FAR_FUTURE_MS));
      } finally {
        statement.execute(
            "ALTER TABLE saved_idempotency_keys RENAME TO iceberg_idempotency_key_meta");
      }
    }
  }

  @TestTemplate
  void testUpgradeCreatesUsableIdempotencyTable() throws Exception {
    String script =
        Files.readString(
            Path.of(
                System.getenv("GRAVITINO_ROOT_DIR"),
                "scripts",
                backendType,
                "upgrade-1.3.0-to-2.0.0-" + backendType + ".sql"));
    // Exercise this feature's additive migration against each database, without rerunning unrelated
    // 1.3-to-2.0 migrations on the fixture's already-current metadata tables.
    int tablePosition = script.indexOf("iceberg_idempotency_key_meta");
    int start = script.lastIndexOf("CREATE TABLE", tablePosition);
    Assertions.assertTrue(start >= 0);
    try (SqlSession session =
            SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession();
        var statement = session.getConnection().createStatement()) {
      statement.execute("DROP TABLE iceberg_idempotency_key_meta");
      ScriptRunner runner = new ScriptRunner(session.getConnection());
      runner.setLogWriter(null);
      runner.setStopOnError(true);
      runner.runScript(new StringReader(script.substring(start)));
      session.commit();
    }
    try (IdempotencyStore migrated =
        IdempotencyStoreFactory.create(
            new IcebergConfig(ImmutableMap.of("idempotency-store-type", "jdbc")))) {
      Assertions.assertEquals(
          ReserveResult.RESERVED, migrated.reserve(KEY, BINDING, -1, FAR_FUTURE_MS));
      migrated.finalizeRecord(KEY, -1, 200, "{}");
      Assertions.assertEquals(-1, migrated.load(KEY).orElseThrow().claim());
    }
  }

  private int rowCount() throws Exception {
    try (SqlSession session =
            SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession();
        var statement = session.getConnection().createStatement();
        var result = statement.executeQuery("SELECT COUNT(*) FROM iceberg_idempotency_key_meta")) {
      result.next();
      return result.getInt(1);
    }
  }

  private static String newKey() {
    UUID uuid = UUID.randomUUID();
    return new UUID(
            (uuid.getMostSignificantBits() & ~0xf000L) | 0x7000L, uuid.getLeastSignificantBits())
        .toString();
  }
}
