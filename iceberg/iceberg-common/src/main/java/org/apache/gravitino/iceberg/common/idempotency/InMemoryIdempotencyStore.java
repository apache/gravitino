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
package org.apache.gravitino.iceberg.common.idempotency;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Expiry;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.google.common.annotations.VisibleForTesting;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
import javax.annotation.Nullable;
import org.apache.gravitino.iceberg.common.IcebergConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Node-local {@link IdempotencyStore} backed by a Caffeine cache.
 *
 * <p>This store is best-effort and intended for single-node deployments, development, and tests. It
 * is not durable, and it is not shared between replicas: a retry routed to another node, or to a
 * node that restarted, finds no record and re-executes the mutation. Its size bound can also evict
 * a record before its reuse window elapses. Production deployments that run more than one replica
 * should use a shared, durable store instead.
 *
 * <p>Each record's own {@code expiresAtMs} decides whether it still counts, so a record past its
 * deadline reads as absent whether or not the cache has reclaimed it yet. Caffeine's expiry only
 * reclaims memory, which keeps behavior independent of when maintenance runs and matches how a
 * database-backed store compares {@code expires_at} in its queries.
 */
public class InMemoryIdempotencyStore implements IdempotencyStore {

  private static final Logger LOG = LoggerFactory.getLogger(InMemoryIdempotencyStore.class);

  /** Evictions between two warnings, so an undersized cache is visible without flooding the log. */
  private static final long EVICTION_LOG_INTERVAL = 1000;

  private final AtomicLong sizeEvictions = new AtomicLong();
  private final LongSupplier clock;

  private Cache<String, IdempotencyRecord> cache;

  /** Creates a store reading wall-clock time. */
  public InMemoryIdempotencyStore() {
    this(System::currentTimeMillis);
  }

  @VisibleForTesting
  InMemoryIdempotencyStore(LongSupplier clock) {
    this.clock = clock;
  }

  @Override
  public void initialize(Map<String, String> properties) {
    IcebergConfig icebergConfig = new IcebergConfig(properties);
    int maxEntries = icebergConfig.get(IcebergConfig.ICEBERG_IDEMPOTENCY_MAX_ENTRIES);
    this.cache =
        Caffeine.newBuilder()
            // Expiry off each record's own deadline rather than expireAfterWrite, which restarts on
            // every write: finalizing a record is a write, and would otherwise push a record
            // finalized late in its window well past the lifetime advertised to clients.
            .expireAfter(
                new Expiry<String, IdempotencyRecord>() {
                  @Override
                  public long expireAfterCreate(
                      String key, IdempotencyRecord record, long currentTimeNanos) {
                    return remainingNanos(record);
                  }

                  @Override
                  public long expireAfterUpdate(
                      String key,
                      IdempotencyRecord record,
                      long currentTimeNanos,
                      long currentDurationNanos) {
                    return remainingNanos(record);
                  }

                  @Override
                  public long expireAfterRead(
                      String key,
                      IdempotencyRecord record,
                      long currentTimeNanos,
                      long currentDurationNanos) {
                    return remainingNanos(record);
                  }
                })
            .maximumSize(maxEntries)
            .removalListener(
                (String key, IdempotencyRecord record, RemovalCause cause) -> {
                  if (cause == RemovalCause.SIZE) {
                    onSizeEviction(maxEntries);
                  }
                })
            .build();
    LOG.info("Initialized in-memory Iceberg idempotency store, max entries: {}.", maxEntries);
  }

  @Override
  public ReserveResult reserve(
      String idempotencyKey, String operationBinding, long claim, long expiresAtMs) {
    String key = IdempotencyKeys.canonicalize(idempotencyKey);
    IdempotencyRecord reserved =
        IdempotencyRecord.reserved(key, operationBinding, claim, clock.getAsLong(), expiresAtMs);
    // A record past its deadline is treated as absent rather than waiting for the cache to reclaim
    // it, so behavior turns on the record's own expiry instead of when maintenance happens to run.
    AtomicBoolean claimed = new AtomicBoolean();
    cache
        .asMap()
        .compute(
            key,
            (cacheKey, existing) -> {
              if (existing == null || isExpired(existing)) {
                claimed.set(true);
                return reserved;
              }
              return existing;
            });
    return claimed.get() ? ReserveResult.RESERVED : ReserveResult.DUPLICATE;
  }

  @Override
  public Optional<IdempotencyRecord> load(String idempotencyKey) {
    return Optional.ofNullable(cache.getIfPresent(IdempotencyKeys.canonicalize(idempotencyKey)))
        .filter(record -> !isExpired(record))
        .filter(IdempotencyRecord::isFinalized);
  }

  @Override
  public void finalizeRecord(
      String idempotencyKey, long claim, int httpStatus, @Nullable String responseSummary) {
    // computeIfPresent, so a record purged while the mutation was running is not resurrected. The
    // claim check leaves a record reserved by someone else untouched.
    cache
        .asMap()
        .computeIfPresent(
            IdempotencyKeys.canonicalize(idempotencyKey),
            (key, record) ->
                record.claim() == claim && !isExpired(record)
                    ? record.withResponse(httpStatus, responseSummary)
                    : record);
  }

  @Override
  public void release(String idempotencyKey, long claim) {
    // Returning null removes the entry. A finalized record is left alone so that a late release
    // cannot drop a response another request may still replay, and so is a record whose claim has
    // moved on, so a caller that lost its reservation cannot free a key someone else is executing.
    cache
        .asMap()
        .computeIfPresent(
            IdempotencyKeys.canonicalize(idempotencyKey),
            (key, record) -> record.isFinalized() || record.claim() != claim ? record : null);
  }

  @Override
  public int purgeExpired(long beforeMs) {
    int purged = 0;
    Iterator<Map.Entry<String, IdempotencyRecord>> entries = cache.asMap().entrySet().iterator();
    while (entries.hasNext()) {
      Map.Entry<String, IdempotencyRecord> entry = entries.next();
      if (entry.getValue().expiresAtMs() < beforeMs
          && cache.asMap().remove(entry.getKey(), entry.getValue())) {
        purged += 1;
      }
    }
    return purged;
  }

  @Override
  public void close() {
    if (cache != null) {
      cache.invalidateAll();
    }
  }

  @VisibleForTesting
  long size() {
    cache.cleanUp();
    return cache.estimatedSize();
  }

  private boolean isExpired(IdempotencyRecord record) {
    return record.expiresAtMs() <= clock.getAsLong();
  }

  private long remainingNanos(IdempotencyRecord record) {
    long remainingMs = record.expiresAtMs() - clock.getAsLong();
    return remainingMs <= 0 ? 0 : TimeUnit.MILLISECONDS.toNanos(remainingMs);
  }

  private void onSizeEviction(int maxEntries) {
    long evictions = sizeEvictions.incrementAndGet();
    if (evictions % EVICTION_LOG_INTERVAL == 1) {
      LOG.warn(
          "In-memory Iceberg idempotency store evicted {} record(s) before their reuse window "
              + "elapsed; retries for evicted keys re-execute the mutation. Raise "
              + "`idempotency-max-entries` (currently {}) or use a shared, durable store.",
          evictions,
          maxEntries);
    }
  }
}
