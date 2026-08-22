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
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.google.common.annotations.VisibleForTesting;
import java.time.Duration;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
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
 */
public class InMemoryIdempotencyStore implements IdempotencyStore {

  private static final Logger LOG = LoggerFactory.getLogger(InMemoryIdempotencyStore.class);

  /** Evictions between two warnings, so an undersized cache is visible without flooding the log. */
  private static final long EVICTION_LOG_INTERVAL = 1000;

  private final AtomicLong sizeEvictions = new AtomicLong();

  private Cache<String, IdempotencyRecord> cache;

  @Override
  public void initialize(Map<String, String> properties) {
    IcebergConfig icebergConfig = new IcebergConfig(properties);
    Duration lifetime =
        Duration.parse(icebergConfig.get(IcebergConfig.ICEBERG_IDEMPOTENCY_KEY_LIFETIME));
    int maxEntries = icebergConfig.get(IcebergConfig.ICEBERG_IDEMPOTENCY_MAX_ENTRIES);
    this.cache =
        Caffeine.newBuilder()
            .expireAfterWrite(lifetime)
            .maximumSize(maxEntries)
            .removalListener(
                (String key, IdempotencyRecord record, RemovalCause cause) -> {
                  if (cause == RemovalCause.SIZE) {
                    onSizeEviction(maxEntries);
                  }
                })
            .build();
    LOG.info(
        "Initialized in-memory Iceberg idempotency store, key lifetime: {}, max entries: {}.",
        lifetime,
        maxEntries);
  }

  @Override
  public ReserveResult reserve(String idempotencyKey, String operationBinding, long expiresAtMs) {
    IdempotencyRecord reserved =
        IdempotencyRecord.reserved(
            idempotencyKey, operationBinding, System.currentTimeMillis(), expiresAtMs);
    IdempotencyRecord existing = cache.asMap().putIfAbsent(idempotencyKey, reserved);
    return existing == null ? ReserveResult.RESERVED : ReserveResult.DUPLICATE;
  }

  @Override
  public Optional<IdempotencyRecord> load(String idempotencyKey) {
    return Optional.ofNullable(cache.getIfPresent(idempotencyKey))
        .filter(IdempotencyRecord::isFinalized);
  }

  @Override
  public void finalizeRecord(
      String idempotencyKey, int httpStatus, @Nullable String responseSummary) {
    // computeIfPresent, so a record purged while the mutation was running is not resurrected.
    cache
        .asMap()
        .computeIfPresent(
            idempotencyKey, (key, record) -> record.withResponse(httpStatus, responseSummary));
  }

  @Override
  public void release(String idempotencyKey) {
    // Returning null removes the entry; a finalized record is left alone so that a late release
    // cannot drop a response another request may still replay.
    cache
        .asMap()
        .computeIfPresent(idempotencyKey, (key, record) -> record.isFinalized() ? record : null);
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
