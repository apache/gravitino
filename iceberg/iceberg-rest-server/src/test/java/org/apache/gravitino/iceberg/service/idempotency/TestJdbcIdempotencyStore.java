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

import static org.awaitility.Awaitility.await;

import com.google.common.collect.ImmutableMap;
import java.io.IOException;
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
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergConstants;
import org.apache.gravitino.iceberg.common.IcebergConfig;
import org.apache.gravitino.iceberg.service.idempotency.mapper.IcebergIdempotencyKeyMapper;
import org.apache.gravitino.iceberg.service.idempotency.po.IcebergIdempotencyKeyPO;
import org.apache.gravitino.storage.relational.TestJDBCBackend;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.apache.ibatis.executor.Executor;
import org.apache.ibatis.jdbc.ScriptRunner;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.plugin.Interceptor;
import org.apache.ibatis.plugin.Intercepts;
import org.apache.ibatis.plugin.Invocation;
import org.apache.ibatis.plugin.Signature;
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
  private Clock clock;
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
    clock = Mockito.mock(Clock.class);
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
  void testFactoryCreatesInitializedJdbcStore() throws IOException {
    IcebergConfig config =
        new IcebergConfig(
            ImmutableMap.of(
                IcebergConstants.ICEBERG_IDEMPOTENCY_STORE_TYPE, "jdbc",
                IcebergConstants.ICEBERG_IDEMPOTENCY_CLEANUP_INTERVAL, "PT1H"));
    try (IdempotencyStore configured = IdempotencyStoreFactory.create(config)) {
      Assertions.assertInstanceOf(JdbcIdempotencyStore.class, configured);
      Assertions.assertTrue(configured.reserve(KEY, BINDING, FAR_FUTURE_MS));
      configured.finalizeRecord(KEY, 200, "{}");
      Assertions.assertEquals(BINDING, peer.load(KEY).orElseThrow().operationBinding());
    }
  }

  @TestTemplate
  void testReserveClaimsKeyOnlyOnce() {
    Assertions.assertTrue(store.reserve(KEY, BINDING, FAR_FUTURE_MS));
    Assertions.assertFalse(store.reserve(KEY, BINDING, FAR_FUTURE_MS));
    Assertions.assertTrue(store.reserve(OTHER_KEY, BINDING, FAR_FUTURE_MS));
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
        JdbcIdempotencyStore contender = i % 2 == 0 ? store : peer;
        executor.submit(
            () -> {
              try {
                start.await();
                if (contender.reserve(KEY, BINDING, FAR_FUTURE_MS)) {
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
    Assertions.assertTrue(store.reserve(KEY, BINDING, FAR_FUTURE_MS));
    Assertions.assertEquals(Optional.empty(), store.load(KEY));

    store.finalizeRecord(KEY, 200, "{\"a\":1}");

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
    Assertions.assertTrue(store.reserve(KEY, BINDING, FAR_FUTURE_MS));
    store.finalizeRecord(KEY, 204, null);

    IdempotencyRecord record = store.load(KEY).orElseThrow(AssertionError::new);
    Assertions.assertEquals(204, record.httpStatus().intValue());
    Assertions.assertNull(record.responseSummary());
  }

  @TestTemplate
  void testFinalizeDoesNotResurrectPurgedRecord() {
    store.finalizeRecord(KEY, 200, "{}");
    Assertions.assertEquals(Optional.empty(), store.load(KEY));
  }

  @TestTemplate
  void testReleaseFreesReservationButKeepsFinalizedResponse() {
    Assertions.assertTrue(store.reserve(KEY, BINDING, FAR_FUTURE_MS));
    store.release(KEY);
    Assertions.assertTrue(store.reserve(KEY, BINDING, FAR_FUTURE_MS));

    store.finalizeRecord(KEY, 201, "{\"b\":2}");
    store.release(KEY);
    Assertions.assertTrue(store.load(KEY).isPresent(), "a finalized response survives a release");
  }

  @TestTemplate
  void testReReserveAfterReleaseReusesFixedClaim() {
    Assertions.assertTrue(store.reserve(KEY, BINDING, FAR_FUTURE_MS));
    store.release(KEY);
    Assertions.assertTrue(peer.reserve(KEY, BINDING, FAR_FUTURE_MS));

    peer.finalizeRecord(KEY, 201, "current");
    IdempotencyRecord record = store.load(KEY).orElseThrow();
    Assertions.assertEquals("current", record.responseSummary());
  }

  @TestTemplate
  void testDifferentBindingStillReturnsDuplicateWithoutReplacingOriginal() {
    String otherBinding = "DELETE v1/cat1/namespaces/ns1/tables/t1";
    Assertions.assertTrue(store.reserve(KEY, BINDING, FAR_FUTURE_MS));

    Assertions.assertFalse(peer.reserve(KEY, otherBinding, FAR_FUTURE_MS));
    Assertions.assertTrue(peer.load(KEY).isEmpty());

    store.finalizeRecord(KEY, 201, "original");
    IdempotencyRecord original = store.load(KEY).orElseThrow();
    Assertions.assertEquals(BINDING, original.operationBinding());
    Assertions.assertEquals(201, original.httpStatus());
    Assertions.assertEquals("original", original.responseSummary());
    Assertions.assertEquals(FAR_FUTURE_MS, original.expiresAtMs());
    Assertions.assertFalse(peer.reserve(KEY, otherBinding, FAR_FUTURE_MS));
    Assertions.assertEquals(original, peer.load(KEY).orElseThrow());
  }

  @TestTemplate
  void testPurgeExpiredRemovesOnlyExpiredRecords() {
    long now = this.now.get();
    store.reserve(KEY, BINDING, now - 1);
    store.reserve(OTHER_KEY, BINDING, FAR_FUTURE_MS);

    Assertions.assertEquals(1, store.purgeExpired(now));

    Assertions.assertTrue(store.reserve(KEY, BINDING, FAR_FUTURE_MS));
    Assertions.assertTrue(
        !store.reserve(OTHER_KEY, BINDING, FAR_FUTURE_MS),
        "an unexpired record must survive the purge");
  }

  @TestTemplate
  void testPurgeTaskRemovesOnlyExpiredRecords() throws Exception {
    store.reserve(KEY, BINDING, now.get());
    Assertions.assertTrue(store.reserve(OTHER_KEY, BINDING, FAR_FUTURE_MS));
    store.finalizeRecord(OTHER_KEY, 200, "{}");
    store.purgeExpiredRecords();
    Assertions.assertEquals(1, rowCount());
    Assertions.assertTrue(store.load(OTHER_KEY).isPresent());
  }

  @TestTemplate
  void testPurgeExpiredDeletesMoreThanOneBatch() {
    long now = this.now.get();
    int records = JdbcIdempotencyStore.PURGE_BATCH_SIZE + 5;
    for (int i = 0; i < records; i++) {
      store.reserve(newKey(), BINDING, now - 1);
    }
    store.reserve(OTHER_KEY, BINDING, FAR_FUTURE_MS);

    Assertions.assertEquals(records, store.purgeExpired(now));
    Assertions.assertFalse(store.reserve(OTHER_KEY, BINDING, FAR_FUTURE_MS));
  }

  @TestTemplate
  void testReplaySurvivesStoreRestartAndIsVisibleToPeer() throws Exception {
    Assertions.assertTrue(store.reserve(KEY, BINDING, FAR_FUTURE_MS));
    Assertions.assertFalse(peer.reserve(KEY, BINDING, FAR_FUTURE_MS));
    Assertions.assertTrue(peer.load(KEY).isEmpty());
    String response = "{\"metadata\":\"" + "x".repeat(100_000) + "\"}";
    store.finalizeRecord(KEY, 200, response);
    IdempotencyRecord original = peer.load(KEY).orElseThrow();
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
  void testPurgedKeyCanBeReservedAgainWithFixedClaim() {
    Assertions.assertTrue(store.reserve(KEY, BINDING, now.get()));
    Assertions.assertEquals(1, peer.purgeExpired(now.get()));
    Assertions.assertTrue(peer.reserve(KEY, BINDING, FAR_FUTURE_MS));

    peer.finalizeRecord(KEY, 201, "current");
    Assertions.assertEquals("current", store.load(KEY).orElseThrow().responseSummary());
  }

  @TestTemplate
  void testExpiredReservationIsReclaimedAsNewKey() {
    Assertions.assertTrue(store.reserve(KEY, BINDING, now.get() + 1));
    now.incrementAndGet();
    Assertions.assertTrue(peer.reserve(KEY, "PUT /replacement", FAR_FUTURE_MS));
    Assertions.assertFalse(store.reserve(KEY, BINDING, FAR_FUTURE_MS));
    peer.finalizeRecord(KEY, 201, "new");
    IdempotencyRecord record = store.load(KEY).orElseThrow();
    Assertions.assertEquals("PUT /replacement", record.operationBinding());
    Assertions.assertEquals("new", record.responseSummary());
  }

  @TestTemplate
  void testReserveReclaimsKeyThatExpiresBeforeSecondAttempt() {
    Assertions.assertTrue(store.reserve(KEY, BINDING, now.get() + 1));
    Mockito.when(clock.millis()).thenReturn(now.get(), now.get() + 1);

    Assertions.assertTrue(peer.reserve(KEY, "PUT /replacement", FAR_FUTURE_MS));
    peer.finalizeRecord(KEY, 201, "current");
    IdempotencyRecord record = store.load(KEY).orElseThrow();
    Assertions.assertEquals("PUT /replacement", record.operationBinding());
    Assertions.assertEquals("current", record.responseSummary());
  }

  @TestTemplate
  void testExpiredResponseIsNotLoadedAndCanBeReplacedWithoutPurge() {
    Assertions.assertTrue(store.reserve(KEY, BINDING, now.get() + 1));
    store.finalizeRecord(KEY, 200, "old");
    now.incrementAndGet();
    Assertions.assertTrue(peer.load(KEY).isEmpty());
    Assertions.assertTrue(peer.reserve(KEY, BINDING, FAR_FUTURE_MS));
    Assertions.assertTrue(peer.load(KEY).isEmpty());
    peer.finalizeRecord(KEY, 204, null);
    Assertions.assertNull(store.load(KEY).orElseThrow().responseSummary());
  }

  @TestTemplate
  void testFinalizeDoesNotResurrectExpiredReservation() throws Exception {
    Assertions.assertTrue(store.reserve(KEY, BINDING, now.get()));
    store.finalizeRecord(KEY, 200, "expired");
    // Inspect persisted state so a read-time expiry filter cannot hide an incorrect finalization.
    try (SqlSession session =
            SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession();
        var statement = session.getConnection().createStatement();
        var result = statement.executeQuery("SELECT http_status FROM iceberg_idempotency_meta")) {
      Assertions.assertTrue(result.next());
      Assertions.assertNull(result.getObject(1));
    }
  }

  @TestTemplate
  void testPurgeIncludesExactCutoff() {
    store.reserve(KEY, BINDING, now.get());
    store.reserve(OTHER_KEY, BINDING, now.get() + 1);
    Assertions.assertEquals(1, store.purgeExpired(now.get()));
    Assertions.assertFalse(peer.reserve(OTHER_KEY, BINDING, FAR_FUTURE_MS));
  }

  @TestTemplate
  void testKeyCaseNormalizationOnAllOperations() {
    String upper = KEY.toUpperCase(Locale.ROOT);
    Assertions.assertTrue(store.reserve(upper, BINDING, FAR_FUTURE_MS));
    Assertions.assertFalse(peer.reserve(KEY, BINDING, FAR_FUTURE_MS));
    peer.release(upper);
    Assertions.assertTrue(store.reserve(KEY, BINDING, FAR_FUTURE_MS));
    peer.finalizeRecord(upper, 200, "{}");
    Assertions.assertEquals(KEY, store.load(upper).orElseThrow().idempotencyKey());
  }

  @TestTemplate
  void testInvalidInputDoesNotReserveAKey() throws Exception {
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> store.reserve("invalid", BINDING, FAR_FUTURE_MS));
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> store.reserve(KEY, null, FAR_FUTURE_MS));
    Assertions.assertThrows(IllegalArgumentException.class, () -> store.load("invalid"));
    Assertions.assertThrows(IllegalArgumentException.class, () -> store.release("invalid"));
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> store.finalizeRecord("invalid", 200, null));
    Assertions.assertEquals(0, rowCount());
  }

  @TestTemplate
  void testReservationCommitsIndependentlyOfCallerTransaction() {
    SessionUtils.beginTransaction();
    try {
      store.reserve(KEY, BINDING, FAR_FUTURE_MS);
    } finally {
      SessionUtils.rollbackTransaction();
    }
    Assertions.assertFalse(peer.reserve(KEY, BINDING, FAR_FUTURE_MS));
  }

  @TestTemplate
  void testOnlyOneCallerCanReclaimExpiredKey() throws InterruptedException {
    store.reserve(KEY, BINDING, now.get());
    testConcurrentReserveOnOneKeyHasExactlyOneWinner();
  }

  @TestTemplate
  void testStoresMethodAndCompleteRequestUri() throws Exception {
    // Jetty accepts request targets longer than MySQL's 64 KiB TEXT limit by default.
    String uri = "/v1/cat1/namespaces/ns%2Fone/tables?tag=" + "x".repeat(70_000) + "%26y";
    String binding = "POST " + uri;
    Assertions.assertTrue(store.reserve(KEY, binding, FAR_FUTURE_MS));
    store.finalizeRecord(KEY, 200, "{}");
    Assertions.assertEquals(binding, peer.load(KEY).orElseThrow().operationBinding());
    try (SqlSession session =
            SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession();
        var statement = session.getConnection().createStatement();
        var result =
            statement.executeQuery(
                "SELECT http_method, request_uri FROM iceberg_idempotency_meta")) {
      Assertions.assertTrue(result.next());
      Assertions.assertEquals("POST", result.getString("http_method"));
      String storedUri = result.getString("request_uri");
      Assertions.assertEquals(uri.length(), storedUri.length());
      Assertions.assertEquals(uri, storedUri);
    }
  }

  @TestTemplate
  void testCleanupContinuesAfterTransientFailure() throws Exception {
    store.reserve(KEY, BINDING, now.get());
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
  void testMissingSchemaFailsInitializationAndCanRetryAfterRepair() throws Exception {
    try (JdbcIdempotencyStore retrying = new JdbcIdempotencyStore();
        SqlSession session =
            SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true);
        var statement = session.getConnection().createStatement()) {
      statement.execute("ALTER TABLE iceberg_idempotency_meta RENAME TO saved_idempotency_keys");
      try {
        Assertions.assertThrows(
            RuntimeException.class,
            () ->
                IdempotencyStoreFactory.create(
                    new IcebergConfig(ImmutableMap.of("idempotency-store-type", "jdbc"))));
        Assertions.assertThrows(
            RuntimeException.class, () -> store.reserve(KEY, BINDING, FAR_FUTURE_MS));
        Assertions.assertThrows(
            RuntimeException.class,
            () -> retrying.initialize(ImmutableMap.of("idempotency-cleanup-interval", "PT1H")));
      } finally {
        statement.execute("ALTER TABLE saved_idempotency_keys RENAME TO iceberg_idempotency_meta");
      }
      retrying.initialize(ImmutableMap.of("idempotency-cleanup-interval", "PT1H"));
      Assertions.assertTrue(retrying.reserve(KEY, BINDING, FAR_FUTURE_MS));
      retrying.finalizeRecord(KEY, 201, "recovered");
      Assertions.assertEquals("recovered", peer.load(KEY).orElseThrow().responseSummary());
      Assertions.assertThrows(
          IllegalStateException.class,
          () -> retrying.initialize(ImmutableMap.of("idempotency-cleanup-interval", "PT1H")));
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
    int tablePosition = script.indexOf("iceberg_idempotency_meta");
    int start = script.lastIndexOf("CREATE TABLE", tablePosition);
    Assertions.assertTrue(start >= 0);
    try (SqlSession session =
            SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession();
        var statement = session.getConnection().createStatement()) {
      statement.execute("DROP TABLE iceberg_idempotency_meta");
      ScriptRunner runner = new ScriptRunner(session.getConnection());
      runner.setLogWriter(null);
      runner.setStopOnError(true);
      runner.runScript(new StringReader(script.substring(start)));
      session.commit();
    }
    try (IdempotencyStore migrated =
        IdempotencyStoreFactory.create(
            new IcebergConfig(ImmutableMap.of("idempotency-store-type", "jdbc")))) {
      Assertions.assertTrue(migrated.reserve(KEY, BINDING, FAR_FUTURE_MS));
      migrated.finalizeRecord(KEY, 200, "{}");
    }
  }

  @TestTemplate
  void testReserveRetriesWhenPurgeRemovesExpiredRow() throws Exception {
    reserveDuringPurge(false);
  }

  @TestTemplate
  void testReserveRetryPreservesPeerReservation() throws Exception {
    reserveDuringPurge(true);
  }

  @TestTemplate
  void testBlockedPurgePreservesReclaimedRow() throws Exception {
    store.reserve(KEY, BINDING, now.get());
    ExecutorService executor = Executors.newSingleThreadExecutor();
    try (SqlSession holder =
        SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession()) {
      String sessionIdQuery;
      switch (backendType) {
        case "h2":
          sessionIdQuery = "SELECT SESSION_ID()";
          break;
        case "mysql":
          sessionIdQuery = "SELECT CONNECTION_ID()";
          break;
        case "postgresql":
          sessionIdQuery = "SELECT pg_backend_pid()";
          break;
        default:
          throw new IllegalStateException("Unsupported backend: " + backendType);
      }
      long holderId;
      try (var statement = holder.getConnection().createStatement();
          var result = statement.executeQuery(sessionIdQuery)) {
        Assertions.assertTrue(result.next());
        holderId = result.getLong(1);
      }
      IcebergIdempotencyKeyMapper mapper = holder.getMapper(IcebergIdempotencyKeyMapper.class);
      Assertions.assertEquals(
          1,
          mapper.replaceExpired(
              IcebergIdempotencyKeyPO.reserved(KEY, BINDING, now.get(), FAR_FUTURE_MS)));
      Future<Integer> purge =
          executor.submit(
              () -> {
                try (SqlSession session =
                    SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession()) {
                  // Keep the engine timeout above the deadline for observing its row-lock wait.
                  try (var statement = session.getConnection().createStatement()) {
                    switch (backendType) {
                      case "h2":
                        statement.execute("SET LOCK_TIMEOUT 30000");
                        break;
                      case "mysql":
                        statement.execute("SET SESSION innodb_lock_wait_timeout = 30");
                        break;
                      case "postgresql":
                        statement.execute("SET LOCAL lock_timeout = '30s'");
                        break;
                      default:
                        throw new IllegalStateException("Unsupported backend: " + backendType);
                    }
                  }
                  int deleted =
                      session
                          .getMapper(IcebergIdempotencyKeyMapper.class)
                          .deleteExpiredBatch(now.get(), JdbcIdempotencyStore.PURGE_BATCH_SIZE);
                  session.commit();
                  return deleted;
                }
              });
      awaitBlockedPurge(purge, holderId);
      holder.commit();
      Assertions.assertEquals(0, purge.get(30, TimeUnit.SECONDS));
      peer.finalizeRecord(KEY, 201, "renewed");
      Assertions.assertEquals("renewed", store.load(KEY).orElseThrow().responseSummary());
    } finally {
      executor.shutdownNow();
      Assertions.assertTrue(executor.awaitTermination(30, TimeUnit.SECONDS));
    }
  }

  private void reserveDuringPurge(boolean peerWins) throws Exception {
    store.reserve(KEY, BINDING, now.get());
    CountDownLatch duplicateInsert = new CountDownLatch(1);
    CountDownLatch purged = new CountDownLatch(1);
    SqlSessionFactoryHelper.getInstance()
        .getSqlSessionFactory()
        .getConfiguration()
        .addInterceptor(new PauseReplacement(duplicateInsert, purged));
    ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      Future<Boolean> result = executor.submit(() -> store.reserve(KEY, BINDING, FAR_FUTURE_MS));
      Assertions.assertTrue(duplicateInsert.await(30, TimeUnit.SECONDS));
      Assertions.assertEquals(1, peer.purgeExpired(now.get()));
      if (peerWins) {
        Assertions.assertTrue(peer.reserve(KEY, "PUT /peer", FAR_FUTURE_MS));
      }
      purged.countDown();
      boolean reserved = result.get(30, TimeUnit.SECONDS);
      Assertions.assertEquals(!peerWins, reserved);
      peer.finalizeRecord(KEY, 201, "retained");
      IdempotencyRecord record = store.load(KEY).orElseThrow();
      Assertions.assertEquals(peerWins ? "PUT /peer" : BINDING, record.operationBinding());
      Assertions.assertEquals("retained", record.responseSummary());
    } finally {
      purged.countDown();
      executor.shutdownNow();
      Assertions.assertTrue(executor.awaitTermination(30, TimeUnit.SECONDS));
    }
  }

  private void awaitBlockedPurge(Future<Integer> purge, long holderId) throws Exception {
    String query;
    switch (backendType) {
      case "h2":
        query = "SELECT COUNT(*) FROM INFORMATION_SCHEMA.SESSIONS WHERE BLOCKER_ID = " + holderId;
        break;
      case "mysql":
        // InnoDB can attribute a wait on an uncommitted index entry to the waiting thread itself.
        query =
            "SELECT COUNT(*) FROM performance_schema.data_lock_waits w"
                + " JOIN performance_schema.threads r ON r.THREAD_ID = w.REQUESTING_THREAD_ID"
                + " JOIN performance_schema.threads b ON b.THREAD_ID = w.BLOCKING_THREAD_ID"
                + " WHERE b.PROCESSLIST_ID = "
                + holderId
                + " OR b.THREAD_ID = r.THREAD_ID";
        break;
      case "postgresql":
        query =
            "SELECT COUNT(*) FROM pg_stat_activity a WHERE "
                + holderId
                + " = ANY(pg_blocking_pids(a.pid))";
        break;
      default:
        throw new IllegalStateException("Unsupported backend: " + backendType);
    }
    try (SqlSession observer =
        SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true)) {
      await()
          .pollInSameThread()
          .atMost(10, TimeUnit.SECONDS)
          .pollInterval(10, TimeUnit.MILLISECONDS)
          .until(
              () -> {
                Assertions.assertFalse(
                    purge.isDone(), "purge must block on the uncommitted replacement");
                try (var statement = observer.getConnection().createStatement();
                    var result = statement.executeQuery(query)) {
                  return result.next() && result.getLong(1) > 0;
                }
              });
    }
  }

  // Pause before replaceExpired locks the row, after the duplicate INSERT rolls back.
  @Intercepts(
      @Signature(
          type = Executor.class,
          method = "update",
          args = {MappedStatement.class, Object.class}))
  private static class PauseReplacement implements Interceptor {
    private final AtomicInteger attempts = new AtomicInteger();
    private final CountDownLatch duplicateInsert;
    private final CountDownLatch purged;

    private PauseReplacement(CountDownLatch duplicateInsert, CountDownLatch purged) {
      this.duplicateInsert = duplicateInsert;
      this.purged = purged;
    }

    @Override
    public Object intercept(Invocation invocation) throws Throwable {
      if (((MappedStatement) invocation.getArgs()[0])
              .getId()
              .equals(IcebergIdempotencyKeyMapper.class.getName() + ".replaceExpired")
          && attempts.incrementAndGet() == 1) {
        duplicateInsert.countDown();
        Assertions.assertTrue(purged.await(30, TimeUnit.SECONDS));
      }
      return invocation.proceed();
    }
  }

  private int rowCount() throws Exception {
    try (SqlSession session =
            SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession();
        var statement = session.getConnection().createStatement();
        var result = statement.executeQuery("SELECT COUNT(*) FROM iceberg_idempotency_meta")) {
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
