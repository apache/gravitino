/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.storage.kv;

import static com.datastrato.gravitino.Configs.ENTITY_KV_TTL;
import static com.datastrato.gravitino.storage.kv.TransactionalKvBackendImpl.endOfTransactionId;
import static com.datastrato.gravitino.storage.kv.TransactionalKvBackendImpl.generateCommitKey;
import static com.datastrato.gravitino.storage.kv.TransactionalKvBackendImpl.generateKey;
import static com.datastrato.gravitino.storage.kv.TransactionalKvBackendImpl.getBinaryTransactionId;

import com.datastrato.gravitino.Config;
import com.datastrato.gravitino.utils.Bytes;
import com.google.common.annotations.VisibleForTesting;
import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link KvGarbageCollector} is a garbage collector for the kv backend. It will collect the version
 * of data which is not committed or exceed the ttl.
 */
public final class KvGarbageCollector implements Closeable {
  private static final Logger LOGGER = LoggerFactory.getLogger(KvGarbageCollector.class);
  private final KvBackend kvBackend;
  private final Config config;

  public final ScheduledExecutorService garbageCollectorPool =
      new ScheduledThreadPoolExecutor(
          2,
          r -> new Thread(r, "KvEntityStore-Garbage-Collector-%d"),
          new ThreadPoolExecutor.AbortPolicy());

  public KvGarbageCollector(KvBackend kvBackend, Config config) {
    this.kvBackend = kvBackend;
    this.config = config;
  }

  public void start() {
    garbageCollectorPool.scheduleAtFixedRate(this::collectGarbage, 5, 10, TimeUnit.MINUTES);
  }

  @VisibleForTesting
  void collectGarbage() {
    LOGGER.info("Start to collect garbage...");
    try {
      LOGGER.info("start to collect uncommitted data...");
      collectUncommittedData();

      LOGGER.info("start to collect old version data...");
      collectOldVersionData();
    } catch (Exception e) {
      LOGGER.error("Failed to collect garbage", e);
    }
  }

  private void collectUncommittedData() throws IOException {
    List<Pair<byte[], byte[]>> kvs =
        kvBackend.scan(
            new KvRangeScan.KvRangeScanBuilder()
                .start(new byte[] {0x00})
                .end(new byte[] {0x7F})
                .startInclusive(true)
                .endInclusive(false)
                .predicate(
                    (k, v) -> {
                      if (TransactionIdGeneratorImpl.LAST_TIMESTAMP.equals(
                          new String(k, StandardCharsets.UTF_8))) {
                        return false;
                      }

                      byte[] transactionId = getBinaryTransactionId(k);
                      return kvBackend.get(generateCommitKey(transactionId)) == null;
                    })
                .limit(10000) /* Each time we only collect 10000 entities at most*/
                .build());

    for (Pair<byte[], byte[]> pair : kvs) {
      kvBackend.delete(pair.getKey());
    }
  }

  private void collectOldVersionData() throws IOException {
    long deleteTimeLine =
        System.currentTimeMillis() - TimeUnit.DAYS.toMillis(config.get(ENTITY_KV_TTL));
    // Why should we leave shift 18 bits? please refer to TransactionIdGeneratorImpl#nextId
    long transactionIdToDelete = deleteTimeLine << 18;
    byte[] startKey = TransactionalKvBackendImpl.generateCommitKey(transactionIdToDelete);
    byte[] endKey = endOfTransactionId();

    // Get all commit marks
    // TODO(yuqi), Use multi-thread to scan the data in case of the data is too large.
    List<Pair<byte[], byte[]>> kvs =
        kvBackend.scan(
            new KvRangeScan.KvRangeScanBuilder()
                .start(startKey)
                .end(endKey)
                .startInclusive(true)
                .endInclusive(false)
                .build());

    // Why should we reverse the order? Because we need to delete the data from the oldest to
    // latest. kvs is sorted by transaction id in ascending order, so we need to reverse it.
    Collections.sort(kvs, (o1, o2) -> Bytes.wrap(o2.getKey()).compareTo(o1.getKey()));
    for (Pair<byte[], byte[]> kv : kvs) {
      List<byte[]> keysInTheTransaction = SerializationUtils.deserialize(kv.getValue());
      byte[] transactionId = getBinaryTransactionId(kv.getKey());

      int keysDeletedCount = 0;
      for (byte[] key : keysInTheTransaction) {
        // Raw key format: {key} + {separator} + {transaction_id}
        byte[] rawKey = generateKey(key, transactionId);
        byte[] rawValue = kvBackend.get(rawKey);
        if (null == rawValue) {
          // It has been deleted
          keysDeletedCount++;
          continue;
        }

        // Value has deleted mark, we can remove it.
        if (null == TransactionalKvBackendImpl.getRealValue(rawValue)) {
          kvBackend.delete(rawKey);
          keysDeletedCount++;
        }

        // If the key is not marked as deleted, then we need to check whether there is a newer
        // version of the key. If there is a newer version of the key, then we can delete it
        // directly.
        List<Pair<byte[], byte[]>> newVersionOfKey =
            kvBackend.scan(
                new KvRangeScan.KvRangeScanBuilder()
                    .start(key)
                    .end(generateKey(key, transactionId))
                    .startInclusive(false)
                    .endInclusive(false)
                    .limit(1)
                    .build());
        if (!newVersionOfKey.isEmpty()) {
          // Has a newer version, we can remove it.
          kvBackend.delete(rawKey);
          keysDeletedCount++;
        }
      }

      // All keys in this transaction have been deleted, we can remove the commit mark.
      if (keysDeletedCount == keysInTheTransaction.size()) {
        kvBackend.delete(kv.getKey());
      }
    }
  }

  @Override
  public void close() throws IOException {
    garbageCollectorPool.shutdown();
    try {
      garbageCollectorPool.awaitTermination(5, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      LOGGER.error("Failed to close garbage collector", e);
    }
  }
}
