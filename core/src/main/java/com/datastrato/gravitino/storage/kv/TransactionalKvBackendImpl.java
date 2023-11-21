/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.storage.kv;

import com.datastrato.gravitino.Config;
import com.datastrato.gravitino.EntityAlreadyExistsException;
import com.datastrato.gravitino.storage.TransactionIdGenerator;
import com.datastrato.gravitino.utils.ByteUtils;
import com.datastrato.gravitino.utils.Bytes;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.commons.lang3.tuple.Pair;

/**
 * KvTransactionManagerImpl is an implementation of {@link TransactionalKvBackend} that uses 2PC
 * (Two-Phase Commit) to support transaction.
 *
 * <p>Assuming we have a key-value pair (k1, v1) and a transaction id 1, the key-value pair will be
 * store as
 *
 * <pre>
 *       KEY                         VALUE
 *   key1 + empty space + 1 --> status_code  + v1
 *   tx + empty space + 1 --> empty
 * </pre>
 *
 * tx + empty space + 1 is a flag to indicate that the transaction 1 has been successfully committed
 * and key1 is visible, if transaction 1 fails and there is no tx + empty space + 1, the key1 is not
 * visible.
 *
 * <p>The status_code is a 4-byte integer that indicates the status of the value. The status code
 * can be one of the following values:
 *
 * <pre>
 *   0x00000000(Metrication: 0) -- NORMAL, the value is visible
 *   0x00000001(Metrication: 1) -- DELETED, the value is deleted and not visible
 * </pre>
 */
public class TransactionalKvBackendImpl implements TransactionalKvBackend {
  private final KvBackend kvBackend;
  private final TransactionIdGenerator transactionIdGenerator;

  @VisibleForTesting final List<Pair<byte[], byte[]>> putPairs = Lists.newArrayList();
  private List<byte[]> originalKeys = Lists.newArrayList();

  @VisibleForTesting long txId;

  public static final String TRANSACTION_PREFIX = "tx ";

  private static final int VALUE_PREFIX_LENGTH = 20;

  private static final byte[] REAL_KEY_AND_TX_ID_SEPARATOR = " ".getBytes(StandardCharsets.UTF_8);

  public TransactionalKvBackendImpl(
      KvBackend kvBackend, TransactionIdGenerator transactionIdGenerator) {
    this.kvBackend = kvBackend;
    this.transactionIdGenerator = transactionIdGenerator;
  }

  public synchronized void begin() {
    txId = transactionIdGenerator.nextId();
  }

  @Override
  public void initialize(Config config) throws IOException {}

  @Override
  public void put(byte[] key, byte[] value, boolean overwrite)
      throws IOException, EntityAlreadyExistsException {
    byte[] oldValue = get(key);
    if (oldValue != null && !overwrite) {
      throw new EntityAlreadyExistsException(
          "Key already exists: " + ByteUtils.formatByteArray(key));
    }
    putPairs.add(Pair.of(generateKey(key, txId), constructValue(value, ValueStatusEnum.NORMAL)));
    originalKeys.add(key);
  }

  @Override
  public byte[] get(byte[] key) throws IOException {
    byte[] rawValue = getNextReadableValue(key);
    if (rawValue == null) {
      return null;
    }

    return getRealValue(rawValue);
  }

  @Override
  public boolean delete(byte[] key) throws IOException {
    byte[] oldValue = get(key);
    if (oldValue == null) {
      return false;
    }

    byte[] deletedValue = constructValue(oldValue, ValueStatusEnum.DELETED);
    putPairs.add(Pair.of(generateKey(key, txId), deletedValue));
    originalKeys.add(key);
    return true;
  }

  @Override
  public boolean deleteRange(KvRangeScan kvRangeScan) throws IOException {
    List<Pair<byte[], byte[]>> pairs = scan(kvRangeScan);
    pairs.forEach(
        p ->
            putPairs.add(
                Pair.of(
                    generateKey(p.getKey(), txId),
                    constructValue(p.getValue(), ValueStatusEnum.DELETED))));
    return false;
  }

  @Override
  public List<Pair<byte[], byte[]>> scan(KvRangeScan scanRange) throws IOException {
    byte[] end = scanRange.getEnd();
    boolean endInclude = scanRange.isEndInclusive();
    if (endInclude) {
      end = Bytes.increment(Bytes.wrap(end)).get();
      endInclude = false;
    }

    KvRangeScan kvRangeScan =
        new KvRangeScan.KvRangeScanBuilder()
            .start(scanRange.getStart())
            .end(end)
            .startInclusive(scanRange.isStartInclusive())
            .endInclusive(endInclude)
            .predicate(
                (k, v) -> {
                  byte[] transactionId = getBinaryTransactionId(k);
                  return kvBackend.get(generateCommitKey(transactionId)) != null;
                })
            .limit(Integer.MAX_VALUE)
            .build();

    List<Pair<byte[], byte[]>> rawPairs = kvBackend.scan(kvRangeScan);
    List<Pair<byte[], byte[]>> result = Lists.newArrayList();
    int i = 0, j = 0;
    while (i < scanRange.getLimit() && j < rawPairs.size()) {
      Pair<byte[], byte[]> pair = rawPairs.get(j);
      byte[] rawKey = pair.getKey();
      byte[] realKey = ArrayUtils.subarray(rawKey, 0, rawKey.length - 9);
      Bytes minNextKey =
          Bytes.increment(Bytes.wrap(Bytes.concat(realKey, REAL_KEY_AND_TX_ID_SEPARATOR)));

      if (!scanRange.isStartInclusive()
          && Bytes.wrap(realKey).compareTo(scanRange.getStart()) == 0) {
        while (j < rawPairs.size() && minNextKey.compareTo(rawPairs.get(j).getKey()) >= 0) {
          j++;
        }
        continue;
      }

      if (!scanRange.isEndInclusive() && Bytes.wrap(realKey).compareTo(scanRange.getEnd()) == 0) {
        // Skip all versions of the same key.
        while (j < rawPairs.size() && minNextKey.compareTo(rawPairs.get(j).getKey()) >= 0) {
          j++;
        }
        continue;
      }

      byte[] value = getRealValue(pair.getValue());
      if (value != null) {
        result.add(Pair.of(realKey, value));
        i++;
      }

      j++;
      // Skip all versions of the same key.
      while (j < rawPairs.size() && minNextKey.compareTo(rawPairs.get(j).getKey()) >= 0) {
        j++;
      }
    }

    return result;
  }

  public void commit() throws IOException {
    // Prepare
    for (Pair<byte[], byte[]> pair : putPairs) {
      kvBackend.put(pair.getKey(), pair.getValue(), true);
    }
    // Commit
    kvBackend.put(
        generateCommitKey(txId), SerializationUtils.serialize((Serializable) originalKeys), true);
  }

  public void rollback() throws IOException {
    // Delete the update value
    for (Pair<byte[], byte[]> pair : putPairs) {
      kvBackend.delete(pair.getKey());
    }
  }

  @Override
  public void close() throws IOException {}

  public static byte[] getRealValue(byte[] rawValue) {
    byte[] firstFourType = ArrayUtils.subarray(rawValue, 0, 4);
    int statusCode = ByteUtils.byteToInt(firstFourType);
    ValueStatusEnum statusEnum = ValueStatusEnum.fromCode(statusCode);
    if (statusEnum == ValueStatusEnum.DELETED) {
      // A deleted value is represented by a 4-byte integer with value 1
      return null;
    }
    return ArrayUtils.subarray(rawValue, VALUE_PREFIX_LENGTH, rawValue.length);
  }

  @VisibleForTesting
  byte[] constructValue(byte[] value, ValueStatusEnum status) {
    byte[] statusCode = ByteUtils.intToByte(status.getCode());
    byte[] prefix = new byte[VALUE_PREFIX_LENGTH];
    System.arraycopy(statusCode, 0, prefix, 0, statusCode.length);
    return Bytes.concat(prefix, value);
  }

  /**
   * Get the latest readable value of the key as we support multi-version concurrency control
   * mechanism to keep multiple version data.
   */
  private byte[] getNextReadableValue(byte[] key) throws IOException {
    List<Pair<byte[], byte[]>> pairs =
        kvBackend.scan(
            new KvRangeScan.KvRangeScanBuilder()
                .start(key)
                .startInclusive(false)
                .end(Bytes.increment(Bytes.wrap(key)).get())
                .endInclusive(false)
                .predicate(
                    (k, v) -> {
                      byte[] transactionId = getBinaryTransactionId(k);
                      return kvBackend.get(generateCommitKey(transactionId)) != null;
                    })
                .limit(1)
                .build());

    if (pairs.isEmpty()) {
      return null;
    }

    byte[] rawKey = pairs.get(0).getKey();
    byte[] transactionId = getBinaryTransactionId(rawKey);
    byte[] transactionFlag = generateCommitKey(transactionId);
    if (kvBackend.get(transactionFlag) != null) {
      // Commit flag exists, the value is readable
      return pairs.get(0).getValue();
    }
    return null;
  }

  /**
   * Revert the bytes, Why we need to revert the bytes? Because we use the transaction id to
   * construct a row key and need to place the latest version of the same key first. That is to say,
   * the latest version of a key is the smallest one in alphabetical order, in this case, we would
   * quickly locate the latest version of a key as key-value pair databases will sort keys in
   * ascending order.
   *
   * <p>Let's say we have a key "key1" and the transaction id is 1, 2, 3, 4, 5, 6, 7, 8, 9, 10. The
   * key-value pairs are:
   *
   * <pre>
   *   key1 10
   *   key1 9
   *   key1 8
   *   ...
   * </pre>
   *
   * Assuming we have two long values, a and b, a >= b, then we always have:
   * revert(ByteUtils.longToByte(a)) <= revert(ByteUtils.longToByte(b))
   *
   * <p>When we try to get the value of key1, we will first the value of key1 10 and can skip old
   * versions quickly.
   */
  private static byte[] revertByteArray(byte[] bytes) {
    for (int i = 0; i < bytes.length; i++) {
      bytes[i] = (byte) (bytes[i] ^ (byte) 0xff);
    }

    return bytes;
  }

  /** Generate a key of data for a specific transaction id. */
  @VisibleForTesting
  static byte[] generateKey(byte[] key, long transactionId) {
    return generateKey(key, revertByteArray(ByteUtils.longToByte(transactionId)));
  }

  static byte[] generateKey(byte[] key, byte[] binaryTransactionId) {
    return Bytes.concat(key, REAL_KEY_AND_TX_ID_SEPARATOR, binaryTransactionId);
  }

  /** Generate a commit key for a specific transaction id. */
  static byte[] generateCommitKey(long transactionId) {
    byte[] binaryTransactionId = ByteUtils.longToByte(transactionId);
    return generateCommitKey(revertByteArray(binaryTransactionId));
  }

  static byte[] generateCommitKey(byte[] transactionId) {
    return Bytes.concat(TRANSACTION_PREFIX.getBytes(StandardCharsets.UTF_8), transactionId);
  }

  /** Get the end of transaction id, we use this key to scan all commit marks. */
  static byte[] endOfTransactionId() {
    return Bytes.increment(Bytes.wrap(TRANSACTION_PREFIX.getBytes(StandardCharsets.UTF_8))).get();
  }

  /** Get the binary transaction id from the raw key. */
  static byte[] getBinaryTransactionId(byte[] rawKey) {
    return ArrayUtils.subarray(rawKey, rawKey.length - 8, rawKey.length);
  }
}
