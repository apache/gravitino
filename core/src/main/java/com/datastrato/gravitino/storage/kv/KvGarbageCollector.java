/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.storage.kv;

import static com.datastrato.gravitino.Configs.KV_DELETE_AFTER_TIME;
import static com.datastrato.gravitino.storage.kv.TransactionalKvBackendImpl.endOfTransactionId;
import static com.datastrato.gravitino.storage.kv.TransactionalKvBackendImpl.generateCommitKey;
import static com.datastrato.gravitino.storage.kv.TransactionalKvBackendImpl.generateKey;
import static com.datastrato.gravitino.storage.kv.TransactionalKvBackendImpl.getBinaryTransactionId;

import com.datastrato.gravitino.Config;
import com.datastrato.gravitino.utils.Bytes;
import com.google.common.annotations.VisibleForTesting;
import java.io.Closeable;
import java.io.IOException;
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
  private static final Logger LOG = LoggerFactory.getLogger(KvGarbageCollector.class);
  private final KvBackend kvBackend;
  private final Config config;

  private static final long MAX_DELETE_TIME_ALLOW = 1000 * 60 * 60 * 24 * 30L; // 30 days
  private static final long MIN_DELETE_TIME_ALLOW = 1000 * 60 * 10L; // 10 minutes

  @VisibleForTesting
  final ScheduledExecutorService garbageCollectorPool =
      new ScheduledThreadPoolExecutor(
          2,
          r -> new Thread(r, "KvEntityStore-Garbage-Collector-%d"),
          new ThreadPoolExecutor.AbortPolicy());

  public KvGarbageCollector(KvBackend kvBackend, Config config) {
    this.kvBackend = kvBackend;
    this.config = config;

    long deleteTimeLine = config.get(KV_DELETE_AFTER_TIME);
    if (deleteTimeLine > MAX_DELETE_TIME_ALLOW || deleteTimeLine < MIN_DELETE_TIME_ALLOW) {
      throw new IllegalArgumentException(
          "The delete time line is too long or too short, "
              + "please check it. The delete time line is "
              + deleteTimeLine
              + " ms");
    }
  }

  public void start() {
    garbageCollectorPool.scheduleAtFixedRate(this::collectAndClean, 5, 10, TimeUnit.MINUTES);
  }

  @VisibleForTesting
  void collectAndClean() {
    LOG.info("Start to collect garbage...");
    try {
      LOG.info("Start to collect and delete uncommitted data...");
      collectAndRemoveUncommittedData();

      LOG.info("Start to collect and delete old version data...");
      collectAndRemoveOldVersionData();
    } catch (Exception e) {
      LOG.error("Failed to collect garbage", e);
    }
  }

  private void collectAndRemoveUncommittedData() throws IOException {
    List<Pair<byte[], byte[]>> kvs =
        kvBackend.scan(
            new KvRangeScan.KvRangeScanBuilder()
                .start(new byte[] {0x20}) // below 0x20 is control character
                .end(new byte[] {0x7F}) // above 0x7F is control character
                .startInclusive(true)
                .endInclusive(false)
                .predicate(
                    (k, v) -> {
                      byte[] transactionId = getBinaryTransactionId(k);
                      return kvBackend.get(generateCommitKey(transactionId)) == null;
                    })
                .limit(10000) /* Each time we only collect 10000 entities at most*/
                .build());

    LOG.info("Start to remove {} uncommitted data", kvs.size());
    for (Pair<byte[], byte[]> pair : kvs) {
      kvBackend.delete(pair.getKey());
    }
  }

  private void collectAndRemoveOldVersionData() throws IOException {
    long deleteTimeLine = System.currentTimeMillis() - config.get(KV_DELETE_AFTER_TIME);
    // Why should we leave shift 18 bits? please refer to TransactionIdGeneratorImpl#nextId
    // We can delete the data which is older than deleteTimeLine.(old data with transaction id that
    // is smaller than transactionIdToDelete)
    long transactionIdToDelete = deleteTimeLine << 18;
    LOG.info("Start to remove data which is older than {}", transactionIdToDelete);
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

    // Why should we reverse the order? Because we need to delete the data from the oldest data to
    // the latest ones. kvs is sorted by transaction id in ascending order (Keys with bigger
    // transaction id
    // is smaller than keys with smaller transaction id). So we need to reverse the order.
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
          LOG.info("Physically delete key that has marked deleted: {}", Bytes.wrap(key));
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
          LOG.info(
              "Physically delete key that has newer version: {}, a newer version {}",
              Bytes.wrap(key),
              Bytes.wrap(newVersionOfKey.get(0).getKey()));
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
    garbageCollectorPool.shutdownNow();
    try {
      garbageCollectorPool.awaitTermination(5, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      LOG.error("Failed to close garbage collector", e);
    }
  }
}
