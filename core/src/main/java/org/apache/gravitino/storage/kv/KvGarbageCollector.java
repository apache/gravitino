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

package org.apache.gravitino.storage.kv;

import static org.apache.gravitino.Configs.STORE_DELETE_AFTER_TIME;
import static org.apache.gravitino.storage.kv.KvNameMappingService.GENERAL_NAME_MAPPING_PREFIX;
import static org.apache.gravitino.storage.kv.TransactionalKvBackendImpl.endOfTransactionId;
import static org.apache.gravitino.storage.kv.TransactionalKvBackendImpl.generateCommitKey;
import static org.apache.gravitino.storage.kv.TransactionalKvBackendImpl.generateKey;
import static org.apache.gravitino.storage.kv.TransactionalKvBackendImpl.getBinaryTransactionId;
import static org.apache.gravitino.storage.kv.TransactionalKvBackendImpl.getTransactionId;

import com.google.common.annotations.VisibleForTesting;
import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.gravitino.Config;
import org.apache.gravitino.Entity.EntityType;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.storage.EntityKeyEncoder;
import org.apache.gravitino.utils.Bytes;
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
  private static final byte[] LAST_COLLECT_COMMIT_ID_KEY =
      Bytes.concat(
          new byte[] {0x1D, 0x00, 0x03}, "last_collect_commit_id".getBytes(StandardCharsets.UTF_8));

  // Keep the last collect commit id to avoid collecting the same data multiple times, the first
  // time the commit is 1 (minimum), and assuming we have collected the data with transaction id
  // (1, 100], then the second time we collect the data and current tx_id is 200,
  // then the current transaction id range is (100, 200] and so on.
  byte[] commitIdHasBeenCollected;
  private long frequencyInMinutes;

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
  }

  public void start() {
    long dateTimelineMinute = config.get(STORE_DELETE_AFTER_TIME) / 1000 / 60;

    // We will collect garbage every 10 minutes at least. If the dateTimelineMinute is larger than
    // 100 minutes, we would collect garbage every dateTimelineMinute/10 minutes.
    this.frequencyInMinutes = Math.max(dateTimelineMinute / 10, 10);
    garbageCollectorPool.scheduleAtFixedRate(
        this::collectAndClean, 5, frequencyInMinutes, TimeUnit.MINUTES);
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

                      // Only remove the uncommitted data that were written frequencyInMinutes
                      // minutes ago.
                      // It may have concurrency issues with TransactionalKvBackendImpl#commit.
                      long writeTime = getTransactionId(transactionId) >> 18;
                      if (writeTime
                          < (System.currentTimeMillis() - frequencyInMinutes * 60 * 1000 * 2)) {
                        return false;
                      }

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
    long deleteTimeline = System.currentTimeMillis() - config.get(STORE_DELETE_AFTER_TIME);
    // Why should we leave shift 18 bits? please refer to TransactionIdGeneratorImpl#nextId
    // We can delete the data which is older than deleteTimeline.(old data with transaction id that
    // is smaller than transactionIdToDelete)
    long transactionIdToDelete = deleteTimeline << 18;
    LOG.info("Start to remove data which is older than {}", transactionIdToDelete);
    byte[] startKey = TransactionalKvBackendImpl.generateCommitKey(transactionIdToDelete);
    commitIdHasBeenCollected = kvBackend.get(LAST_COLLECT_COMMIT_ID_KEY);
    if (commitIdHasBeenCollected == null) {
      commitIdHasBeenCollected = endOfTransactionId();
    }

    long lastGCId = getTransactionId(getBinaryTransactionId(commitIdHasBeenCollected));
    LOG.info(
        "Start to collect data which is modified between '{}({})' (exclusive) and '{}({})' (inclusive)",
        lastGCId,
        lastGCId == 1 ? lastGCId : DateFormatUtils.format(lastGCId >> 18, TIME_STAMP_FORMAT),
        transactionIdToDelete,
        DateFormatUtils.format(deleteTimeline, TIME_STAMP_FORMAT));

    // Get all commit marks
    // TODO(yuqi), Use multi-thread to scan the data in case of the data is too large.
    List<Pair<byte[], byte[]>> kvs =
        kvBackend.scan(
            new KvRange.KvRangeBuilder()
                .start(startKey)
                .end(commitIdHasBeenCollected)
                .startInclusive(true)
                .endInclusive(false)
                .build());

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
          // Delete the key of all versions.
          removeAllVersionsOfKey(rawKey, key, false);

          LogHelper logHelper = decodeKey(key, transactionId);
          kvBackend.delete(rawKey);
          LOG.info(
              "Physically delete key that has marked deleted: name identifier: '{}', entity type: '{}',"
                  + " createTime: '{}({})', key: '{}'",
              logHelper.identifier,
              logHelper.type,
              logHelper.createTimeAsString,
              logHelper.createTimeInMs,
              Bytes.wrap(key));
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
          // Have a new version, we can safely remove all old versions.
          removeAllVersionsOfKey(rawKey, key, false);

          // Has a newer version, we can remove it.
          LogHelper logHelper = decodeKey(key, transactionId);
          byte[] newVersionKey = newVersionOfKey.get(0).getKey();
          LogHelper newVersionLogHelper = decodeKey(newVersionKey);
          kvBackend.delete(rawKey);
          LOG.info(
              "Physically delete key that has newer version: name identifier: '{}', entity type: '{}',"
                  + " createTime: '{}({})', newVersion createTime: '{}({})',"
                  + " key: '{}', newVersion key: '{}'",
              logHelper.identifier,
              logHelper.type,
              logHelper.createTimeAsString,
              logHelper.createTimeInMs,
              newVersionLogHelper.createTimeAsString,
              newVersionLogHelper.createTimeInMs,
              Bytes.wrap(rawKey),
              Bytes.wrap(newVersionKey));
          keysDeletedCount++;
        }
      }

      // All keys in this transaction have been deleted, we can remove the commit mark.
      if (keysDeletedCount == keysInTheTransaction.size()) {
        kvBackend.delete(kv.getKey());
        long timestamp = getTransactionId(transactionId) >> 18;
        LOG.info(
            "Physically delete commit mark: {}, createTime: '{}({})', key: '{}'",
            Bytes.wrap(kv.getKey()),
            DateFormatUtils.format(timestamp, TIME_STAMP_FORMAT),
            timestamp,
            Bytes.wrap(kv.getKey()));
      }
    }

    commitIdHasBeenCollected = kvs.isEmpty() ? startKey : kvs.get(0).getKey();
    kvBackend.put(LAST_COLLECT_COMMIT_ID_KEY, commitIdHasBeenCollected, true);
  }

  /**
   * Remove all versions of the key.
   *
   * @param rawKey raw key, it contains the transaction id.
   * @param key key, it's the real key and does not contain the transaction id
   * @param includeStart whether include the start key.
   * @throws IOException if an I/O exception occurs during deletion.
   */
  private void removeAllVersionsOfKey(byte[] rawKey, byte[] key, boolean includeStart)
      throws IOException {
    List<Pair<byte[], byte[]>> kvs =
        kvBackend.scan(
            new KvRange.KvRangeBuilder()
                .start(rawKey)
                .end(generateKey(key, 1))
                .startInclusive(includeStart)
                .endInclusive(false)
                .build());

    for (Pair<byte[], byte[]> kv : kvs) {
      // Delete real data.
      kvBackend.delete(kv.getKey());

      LogHelper logHelper = decodeKey(kv.getKey());
      LOG.info(
          "Physically delete key that has marked deleted: name identifier: '{}', entity type: '{}',"
              + " createTime: '{}({})', key: '{}'",
          logHelper.identifier,
          logHelper.type,
          logHelper.createTimeAsString,
          logHelper.createTimeInMs,
          Bytes.wrap(key));

      // Try to delete commit id if the all keys in the transaction id have been dropped.
      byte[] transactionId = getBinaryTransactionId(kv.getKey());
      byte[] transactionKey = generateCommitKey(transactionId);
      byte[] transactionValue = kvBackend.get(transactionKey);

      List<byte[]> keysInTheTransaction = SerializationUtils.deserialize(transactionValue);

      boolean allDropped = true;
      for (byte[] keyInTheTransaction : keysInTheTransaction) {
        if (kvBackend.get(generateKey(keyInTheTransaction, transactionId)) != null) {
          // There is still a key in the transaction, we cannot delete the commit mark.
          allDropped = false;
          break;
        }
      }

      // Try to delete the commit mark.
      if (allDropped) {
        kvBackend.delete(transactionKey);
        long timestamp = TransactionalKvBackendImpl.getTransactionId(transactionId) >> 18;
        LOG.info(
            "Physically delete commit mark: {}, createTime: '{}({})', key: '{}'",
            Bytes.wrap(kv.getKey()),
            DateFormatUtils.format(timestamp, TIME_STAMP_FORMAT),
            timestamp,
            Bytes.wrap(kv.getKey()));
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
    long timestamp = getTransactionId(timestampArray) >> 18;
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
