/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.storage.kv;

import static com.datastrato.gravitino.Configs.KV_DELETE_AFTER_TIME;
import static com.datastrato.gravitino.storage.kv.KvNameMappingService.GENERAL_NAME_MAPPING_PREFIX;
import static com.datastrato.gravitino.storage.kv.TransactionalKvBackendImpl.endOfTransactionId;
import static com.datastrato.gravitino.storage.kv.TransactionalKvBackendImpl.generateCommitKey;
import static com.datastrato.gravitino.storage.kv.TransactionalKvBackendImpl.generateKey;
import static com.datastrato.gravitino.storage.kv.TransactionalKvBackendImpl.getBinaryTransactionId;

import com.datastrato.gravitino.Config;
import com.datastrato.gravitino.Entity.EntityType;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.storage.EntityKeyEncoder;
import com.datastrato.gravitino.utils.Bytes;
import com.google.common.annotations.VisibleForTesting;
import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
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
  private final EntityKeyEncoder<byte[]> entityKeyEncoder;

  private static final long MAX_DELETE_TIME_ALLOW = 1000 * 60 * 60 * 24 * 30L; // 30 days
  private static final long MIN_DELETE_TIME_ALLOW = 1000 * 60 * 10L; // 10 minutes

  private static final String TIME_STAMP_FORMAT = "yyyy-MM-dd HH:mm:ss.SSS";

  @VisibleForTesting
  final ScheduledExecutorService garbageCollectorPool =
      new ScheduledThreadPoolExecutor(
          2,
          r -> {
            Thread t = new Thread(r, "KvEntityStore-Garbage-Collector");
            t.setDaemon(true);
            return t;
          },
          new ThreadPoolExecutor.AbortPolicy());

  public KvGarbageCollector(
      KvBackend kvBackend, Config config, EntityKeyEncoder<byte[]> entityKeyEncoder) {
    this.kvBackend = kvBackend;
    this.config = config;
    this.entityKeyEncoder = entityKeyEncoder;

    long deleteTimeLine = config.get(KV_DELETE_AFTER_TIME);
    if (deleteTimeLine > MAX_DELETE_TIME_ALLOW || deleteTimeLine < MIN_DELETE_TIME_ALLOW) {
      throw new IllegalArgumentException(
          String.format(
              "The delete time line is too long or too short, "
                  + "please check it. The delete time line is %s ms,"
                  + "max delete time allow is %s ms(30 days),"
                  + "min delete time allow is %s ms(10 minutes)",
              deleteTimeLine, MAX_DELETE_TIME_ALLOW, MIN_DELETE_TIME_ALLOW));
    }
  }

  public void start() {
    long dateTimeLineMinute = config.get(KV_DELETE_AFTER_TIME) / 1000 / 60;

    // We will collect garbage every 10 minutes at least. If the dateTimeLineMinute is larger than
    // 100 minutes, we would collect garbage every dateTimeLineMinute/10 minutes.
    long frequency = Math.max(dateTimeLineMinute / 10, 10);
    garbageCollectorPool.scheduleAtFixedRate(this::collectAndClean, 5, frequency, TimeUnit.MINUTES);
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
            new KvRange.KvRangeBuilder()
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
      // Remove is a high-risk operation, So we log every delete operation
      LogHelper logHelper = decodeKey(pair.getKey());
      LOG.info(
          "Physically delete key that has marked uncommitted: name identity: '{}', entity type: '{}', createTime: '{}({})', key: '{}'",
          logHelper.identifier,
          logHelper.type,
          logHelper.createTimeAsString,
          logHelper.createTimeInMs,
          pair.getKey());
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
            new KvRange.KvRangeBuilder()
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
          LogHelper logHelper = decodeKey(key, transactionId);
          LOG.info(
              "Physically delete key that has marked deleted: name identifier: '{}', entity type: '{}', createTime: '{}({})', key: '{}'",
              logHelper.identifier,
              logHelper.type,
              logHelper.createTimeAsString,
              logHelper.createTimeInMs,
              Bytes.wrap(key));
          kvBackend.delete(rawKey);
          keysDeletedCount++;
          continue;
        }

        // If the key is not marked as deleted, then we need to check whether there is a newer
        // version of the key. If there is a newer version of the key, then we can delete it
        // directly.
        List<Pair<byte[], byte[]>> newVersionOfKey =
            kvBackend.scan(
                new KvRange.KvRangeBuilder()
                    .start(key)
                    .end(generateKey(key, transactionId))
                    .startInclusive(false)
                    .endInclusive(false)
                    .limit(1)
                    .build());
        if (!newVersionOfKey.isEmpty()) {
          // Has a newer version, we can remove it.
          LogHelper logHelper = decodeKey(key, transactionId);
          byte[] newVersionKey = newVersionOfKey.get(0).getKey();
          LogHelper newVersionLogHelper = decodeKey(newVersionKey);
          LOG.info(
              "Physically delete key that has newer version: name identifier: '{}', entity type: '{}', createTime: '{}({})', newVersion createTime: '{}({})',"
                  + " key: '{}', newVersion key: '{}'",
              logHelper.identifier,
              logHelper.type,
              logHelper.createTimeAsString,
              logHelper.createTimeInMs,
              newVersionLogHelper.createTimeAsString,
              newVersionLogHelper.createTimeInMs,
              Bytes.wrap(rawKey),
              Bytes.wrap(newVersionKey));
          kvBackend.delete(rawKey);
          keysDeletedCount++;
        }
      }

      // All keys in this transaction have been deleted, we can remove the commit mark.
      if (keysDeletedCount == keysInTheTransaction.size()) {
        long timestamp = TransactionalKvBackendImpl.getTransactionId(transactionId) >> 18;
        LOG.info(
            "Physically delete commit mark: {}, createTime: '{}({})', key: '{}'",
            Bytes.wrap(kv.getKey()),
            DateFormatUtils.format(timestamp, TIME_STAMP_FORMAT),
            timestamp,
            Bytes.wrap(kv.getKey()));
        kvBackend.delete(kv.getKey());
      }
    }
  }

  static class LogHelper {

    @VisibleForTesting final NameIdentifier identifier;
    @VisibleForTesting final EntityType type;
    @VisibleForTesting final long createTimeInMs;
    // Formatted createTime
    @VisibleForTesting final String createTimeAsString;

    public static final LogHelper NONE = new LogHelper(null, null, 0L, null);

    public LogHelper(
        NameIdentifier identifier,
        EntityType type,
        long createTimeInMs,
        String createTimeAsString) {
      this.identifier = identifier;
      this.type = type;
      this.createTimeInMs = createTimeInMs;
      this.createTimeAsString = createTimeAsString;
    }
  }

  @VisibleForTesting
  LogHelper decodeKey(byte[] key, byte[] timestampArray) {
    if (entityKeyEncoder == null) {
      return LogHelper.NONE;
    }

    // Name mapping data, we do not support it now.
    if (Arrays.equals(GENERAL_NAME_MAPPING_PREFIX, ArrayUtils.subarray(key, 0, 3))) {
      return LogHelper.NONE;
    }

    Pair<NameIdentifier, EntityType> entityTypePair;
    try {
      entityTypePair = entityKeyEncoder.decode(key);
    } catch (Exception e) {
      LOG.warn("Unable to decode key: {}", Bytes.wrap(key), e);
      return LogHelper.NONE;
    }
    long timestamp = TransactionalKvBackendImpl.getTransactionId(timestampArray) >> 18;
    String ts = DateFormatUtils.format(timestamp, TIME_STAMP_FORMAT);

    return new LogHelper(entityTypePair.getKey(), entityTypePair.getValue(), timestamp, ts);
  }

  @VisibleForTesting
  LogHelper decodeKey(byte[] rawKey) {
    byte[] key = TransactionalKvBackendImpl.getRealKey(rawKey);
    byte[] timestampArray = TransactionalKvBackendImpl.getBinaryTransactionId(rawKey);
    return decodeKey(key, timestampArray);
  }

  @Override
  public void close() throws IOException {
    garbageCollectorPool.shutdownNow();
    try {
      garbageCollectorPool.awaitTermination(5, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOG.error("Failed to close garbage collector", e);
    }
  }
}
