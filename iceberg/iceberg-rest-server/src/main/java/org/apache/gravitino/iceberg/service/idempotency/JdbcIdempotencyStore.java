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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.time.Clock;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import javax.annotation.Nullable;
import org.apache.gravitino.iceberg.common.IcebergConfig;
import org.apache.gravitino.iceberg.common.idempotency.IdempotencyKeys;
import org.apache.gravitino.iceberg.common.idempotency.IdempotencyRecord;
import org.apache.gravitino.iceberg.common.idempotency.IdempotencyStore;
import org.apache.gravitino.iceberg.service.idempotency.mapper.IcebergIdempotencyKeyMapper;
import org.apache.gravitino.iceberg.service.idempotency.po.IcebergIdempotencyKeyPO;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.gravitino.storage.relational.utils.ExceptionUtils;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Durable idempotency records shared by replicas using the same Gravitino relational entity store.
 * Requires an initialized entity store, as provided in auxiliary mode. The database schema must be
 * upgraded before this store is enabled.
 *
 * <p>Reservations commit independently of catalog transactions. Claim tokens fence stale finalize
 * and release calls, but cannot undo catalog side effects or make the catalog mutation and response
 * persistence atomic. Expired keys can be reserved again without waiting for background cleanup.
 */
public class JdbcIdempotencyStore implements IdempotencyStore {
  private static final Logger LOG = LoggerFactory.getLogger(JdbcIdempotencyStore.class);
  @VisibleForTesting static final int PURGE_BATCH_SIZE = 1000;
  @VisibleForTesting static final int MAX_PURGE_BATCHES = 100;

  private final Clock clock;
  private SqlSessionFactory sessionFactory;
  private ScheduledExecutorService purgeScheduler;

  /** Creates a store using the system UTC clock. */
  public JdbcIdempotencyStore() {
    this(Clock.systemUTC());
  }

  @VisibleForTesting
  JdbcIdempotencyStore(Clock clock) {
    this.clock = clock;
  }

  @Override
  public void initialize(Map<String, String> properties) {
    Preconditions.checkState(
        sessionFactory == null, "JDBC idempotency store is already initialized");
    Duration interval =
        Duration.parse(
            new IcebergConfig(properties).get(IcebergConfig.ICEBERG_IDEMPOTENCY_CLEANUP_INTERVAL));
    long intervalMs = interval.toMillis();
    Preconditions.checkArgument(
        intervalMs > 0, "Idempotency cleanup interval must be at least 1ms");
    try {
      sessionFactory = SqlSessionFactoryHelper.getInstance().getSqlSessionFactory();
    } catch (IllegalStateException e) {
      throw new IllegalStateException(
          "JDBC idempotency storage requires an initialized Gravitino relational entity store "
              + "(auxiliary mode); it is not available in the standalone Iceberg REST server",
          e);
    }
    // Core cannot discover mappers in the auxiliary service's class loader during its startup.
    Configuration configuration = sessionFactory.getConfiguration();
    synchronized (configuration) {
      if (!configuration.hasMapper(IcebergIdempotencyKeyMapper.class)) {
        configuration.addMapper(IcebergIdempotencyKeyMapper.class);
      }
    }
    // Fail startup, rather than the first mutation, when the database has not been upgraded.
    execute(mapper -> mapper.selectRecord("00000000-0000-7000-8000-000000000000", clock.millis()));
    purgeScheduler =
        Executors.newSingleThreadScheduledExecutor(
            runnable -> {
              Thread thread = new Thread(runnable, "iceberg-idempotency-purge");
              thread.setDaemon(true);
              return thread;
            });
    purgeScheduler.scheduleWithFixedDelay(
        this::purgeExpiredRecords, intervalMs, intervalMs, TimeUnit.MILLISECONDS);
  }

  @Override
  public ReserveResult reserve(
      String idempotencyKey, String operationBinding, long claim, long expiresAtMs) {
    String key = IdempotencyKeys.canonicalize(idempotencyKey);
    Preconditions.checkArgument(operationBinding != null, "operationBinding should not be null");
    IcebergIdempotencyKeyPO po =
        IcebergIdempotencyKeyPO.reserved(key, operationBinding, claim, clock.millis(), expiresAtMs);
    try {
      execute(mapper -> mapper.insert(po));
      return ReserveResult.RESERVED;
    } catch (RuntimeException e) {
      if (!ExceptionUtils.isDuplicateKey(e)) {
        throw e;
      }
    }
    // The failed insert has rolled back before this conditional update, including on PostgreSQL.
    // Keeping these transactions separate also avoids upgrading duplicate-insert locks on MySQL.
    int replaced = execute(mapper -> mapper.replaceExpired(po));
    return replaced > 0 ? ReserveResult.RESERVED : ReserveResult.DUPLICATE;
  }

  @Override
  public Optional<IdempotencyRecord> load(String idempotencyKey) {
    String key = IdempotencyKeys.canonicalize(idempotencyKey);
    return Optional.ofNullable(execute(mapper -> mapper.selectRecord(key, clock.millis())))
        .map(IcebergIdempotencyKeyPO::toIdempotencyRecord);
  }

  @Override
  public void finalizeRecord(
      String idempotencyKey, long claim, int httpStatus, @Nullable String responseSummary) {
    String key = IdempotencyKeys.canonicalize(idempotencyKey);
    execute(
        mapper -> mapper.finalizeRecord(key, claim, httpStatus, responseSummary, clock.millis()));
  }

  @Override
  public void release(String idempotencyKey, long claim) {
    String key = IdempotencyKeys.canonicalize(idempotencyKey);
    execute(mapper -> mapper.deleteReservation(key, claim));
  }

  @Override
  public int purgeExpired(long beforeMs) {
    int purged = 0;
    for (int batch = 0; batch < MAX_PURGE_BATCHES; batch++) {
      int deleted = execute(mapper -> mapper.deleteExpiredBatch(beforeMs, PURGE_BATCH_SIZE));
      purged += deleted;
      if (deleted < PURGE_BATCH_SIZE) {
        break;
      }
    }
    return purged;
  }

  @Override
  public void close() {
    if (purgeScheduler != null) {
      purgeScheduler.shutdown();
      try {
        if (!purgeScheduler.awaitTermination(5, TimeUnit.SECONDS)) {
          purgeScheduler.shutdownNow();
        }
      } catch (InterruptedException e) {
        purgeScheduler.shutdownNow();
        Thread.currentThread().interrupt();
      }
    }
    // The entity store owns the shared connection pool and closes it after auxiliary services.
  }

  @VisibleForTesting
  void purgeExpiredRecords() {
    try {
      purgeExpired(clock.millis());
    } catch (Exception e) {
      // A transient database failure must not cancel all future scheduled cleanup attempts.
      LOG.warn("Purging expired Iceberg idempotency records failed", e);
    }
  }

  private <T> T execute(Function<IcebergIdempotencyKeyMapper, T> operation) {
    try (SqlSession session = sessionFactory.openSession()) {
      T result = operation.apply(session.getMapper(IcebergIdempotencyKeyMapper.class));
      session.commit();
      return result;
    }
  }
}
