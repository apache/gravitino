/*
 * Copyright 2023 Datastrato Pvt Ltd.
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
import java.util.List;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.commons.lang3.tuple.Pair;

/**
 * TransactionalKvBackendImpl is an implementation of {@link TransactionalKvBackend} that uses 2PC
 * (Two-Phase Commit) to support transaction.
 *
 * <p>Assuming we have a key-value pair (k1, v1) and a transaction id 1, the key-value pair will be
 * store as
 *
 * <pre>
 *       KEY                         VALUE
 *   key1 + separator + 1 --  status_code  + v1
 *   tx + separator + 1   --  binary that contains all keys involved in this tx
 * </pre>
 *
 * We use '0x1F' as the separator, '______tx' as the value of tx, key1 + separator + 1 as the key of
 * the value, tx + separator + 1 as the flag to indicate that the transaction 1 has been
 * successfully committed and key1 can be visible or not, if transaction 1 fails(fail to write tx +
 * separator + 1) and there is no tx + separator + 1, the key1 is not visible.
 *
 * <p>The status_code is a 20-byte integer that indicates the status of the value. The first four
 * bytes of status code can be one of the following values:
 *
 * <pre>
 *   0x00000000(Metrication: 0) -- NORMAL, the value is visible
 *   0x00000001(Metrication: 1) -- DELETED, the value is deleted and not visible
 * </pre>
 */
@ThreadSafe
public class TransactionalKvBackendImpl implements TransactionalKvBackend {
  private final KvBackend kvBackend;
  private final TransactionIdGenerator transactionIdGenerator;

  @VisibleForTesting
  final ThreadLocal<List<Pair<byte[], byte[]>>> putPairs =
      ThreadLocal.withInitial(Lists::newArrayList);

  private final ThreadLocal<List<byte[]>> originalKeys =
      ThreadLocal.withInitial(Lists::newArrayList);

  @VisibleForTesting final ThreadLocal<Long> txId = new ThreadLocal<>();

  // 0x1E is control character RS
  private static final byte[] TRANSACTION_PREFIX = {0x1E};

  // Why use 8? We use 8 bytes to represent the status code, the first byte is used to
  // identify the status of the value, the rest 7 bytes are for future use.
  private static final int LENGTH_OF_VALUE_PREFIX = 8;

  // Why use 0x1F, 0x1F is a control character that is used as a delimiter in the text.
  private static final byte[] SEPARATOR = new byte[] {0x1F};

  private static final int LENGTH_OF_TRANSACTION_ID = Long.BYTES;
  private static final int LENGTH_OF_SEPARATOR = SEPARATOR.length;
  private static final int LENGTH_OF_VALUE_STATUS = Byte.BYTES;

  public TransactionalKvBackendImpl(
      KvBackend kvBackend, TransactionIdGenerator transactionIdGenerator) {
    this.kvBackend = kvBackend;
    this.transactionIdGenerator = transactionIdGenerator;
  }

  @Override
  public void begin() {
    if (!putPairs.get().isEmpty()) {
      throw new IllegalStateException(
          "The transaction is has not committed or rollback yet, you should commit or rollback it first");
    }

    txId.set(transactionIdGenerator.nextId());
  }

  @Override
  public void commit() throws IOException {
    try {
      if (putPairs.get().isEmpty()) {
        return;
      }

      // Prepare
      for (Pair<byte[], byte[]> pair : putPairs.get()) {
        kvBackend.put(pair.getKey(), pair.getValue(), true);
      }

      // Commit
      kvBackend.put(
          generateCommitKey(txId.get()),
          SerializationUtils.serialize((Serializable) originalKeys.get()),
          true);
    } finally {
      putPairs.get().clear();
      originalKeys.get().clear();
      txId.remove();
    }
  }

  @Override
  public void rollback() throws IOException {
    // Delete the update value
    for (Pair<byte[], byte[]> pair : putPairs.get()) {
      kvBackend.delete(pair.getKey());
    }
  }

  @Override
  public void closeTransaction() {
    putPairs.remove();
    originalKeys.remove();
    txId.remove();
  }

  @Override
  public boolean inTransaction() {
    return txId.get() != null;
  }

  @Override
  public void initialize(Config config) throws IOException {}

  @Override
  public void put(byte[] key, byte[] value, boolean overwrite)
      throws IOException, EntityAlreadyExistsException {
    byte[] oldValue = get(key);
    if (oldValue != null && !overwrite) {
      throw new EntityAlreadyExistsException("Key already exists: " + Bytes.wrap(key));
    }
    putPairs
        .get()
        .add(Pair.of(generateKey(key, txId.get()), constructValue(value, ValueStatusEnum.NORMAL)));
    originalKeys.get().add(key);
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
    putPairs.get().add(Pair.of(generateKey(key, txId.get()), deletedValue));
    originalKeys.get().add(key);
    return true;
  }

  @Override
  public boolean deleteRange(KvRange kvRange) throws IOException {
    List<Pair<byte[], byte[]>> pairs = scan(kvRange);
    pairs.forEach(
        p ->
            putPairs
                .get()
                .add(
                    Pair.of(
                        generateKey(p.getKey(), txId.get()),
                        constructValue(p.getValue(), ValueStatusEnum.DELETED))));
    return true;
  }

  @Override
  public List<Pair<byte[], byte[]>> scan(KvRange scanRange) throws IOException {
    // Why we need to change the end key? Because we use the transaction id to construct a row key
    // Assuming the end key is 'a' and the value of endInclusive is true, if we want to scan the
    // value of key a, then we need to change the end key to 'b' and set the value of endInclusive
    // to false.
    byte[] end = scanRange.getEnd();
    boolean endInclude = scanRange.isEndInclusive();
    if (endInclude) {
      end = endOfKey(end);
      endInclude = false;
    }

    KvRange kvRange =
        new KvRange.KvRangeBuilder()
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

    List<Pair<byte[], byte[]>> rawPairs = kvBackend.scan(kvRange);
    List<Pair<byte[], byte[]>> result = Lists.newArrayList();
    int i = 0, j = 0;
    while (i < scanRange.getLimit() && j < rawPairs.size()) {
      Pair<byte[], byte[]> pair = rawPairs.get(j);
      byte[] rawKey = pair.getKey();
      byte[] realKey = getRealKey(rawKey);
      Bytes minNextKey = Bytes.increment(Bytes.wrap(Bytes.concat(realKey, SEPARATOR)));

      // If the start key is exclusive and the key is equal to the start key, we need to skip it.
      if (!scanRange.isStartInclusive()
          && Bytes.wrap(realKey).compareTo(scanRange.getStart()) == 0) {
        while (j < rawPairs.size() && minNextKey.compareTo(rawPairs.get(j).getKey()) >= 0) {
          j++;
        }
        continue;
      }

      // If the end key is exclusive and the key is equal to the end key, we need to skip it.
      if (!scanRange.isEndInclusive() && Bytes.wrap(realKey).compareTo(scanRange.getEnd()) == 0) {
        break;
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

  @Override
  public void close() throws IOException {}

  public static byte[] getRealValue(byte[] rawValue) {
    byte[] firstType = ArrayUtils.subarray(rawValue, 0, LENGTH_OF_VALUE_STATUS);
    ValueStatusEnum statusEnum = ValueStatusEnum.fromCode(firstType[0]);
    if (statusEnum == ValueStatusEnum.DELETED) {
      // A deleted value is represented by a 4-byte integer with value 1
      return null;
    }
    return ArrayUtils.subarray(rawValue, LENGTH_OF_VALUE_PREFIX, rawValue.length);
  }

  @VisibleForTesting
  byte[] constructValue(byte[] value, ValueStatusEnum status) {
    byte[] statusCode = new byte[] {status.getCode()};
    byte[] prefix = new byte[LENGTH_OF_VALUE_PREFIX];
    System.arraycopy(statusCode, 0, prefix, 0, statusCode.length);
    return Bytes.concat(prefix, value);
  }

  @VisibleForTesting
  byte[] constructKey(byte[] key) {
    return Bytes.concat(key, SEPARATOR, revertByteArray(ByteUtils.longToByte(txId.get())));
  }

  /**
   * Get the latest readable value of the key as we support multi-version concurrency control
   * mechanism to keep multiple version data.
   */
  private byte[] getNextReadableValue(byte[] key) throws IOException {
    List<Pair<byte[], byte[]>> pairs =
        kvBackend.scan(
            new KvRange.KvRangeBuilder()
                .start(key)
                .startInclusive(false)
                .end(endOfKey(key))
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

    return pairs.get(0).getValue();
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
  @VisibleForTesting
  static byte[] revertByteArray(byte[] bytes) {
    byte[] result = new byte[bytes.length];
    for (int i = 0; i < bytes.length; i++) {
      result[i] = (byte) (bytes[i] ^ (byte) 0xff);
    }

    return result;
  }

  /** Generate a key of data for a specific transaction id. */
  @VisibleForTesting
  static byte[] generateKey(byte[] key, long transactionId) {
    return generateKey(key, revertByteArray(ByteUtils.longToByte(transactionId)));
  }

  static byte[] generateKey(byte[] key, byte[] binaryTransactionId) {
    return Bytes.concat(key, SEPARATOR, binaryTransactionId);
  }

  /** Generate a commit key for a specific transaction id. */
  static byte[] generateCommitKey(long transactionId) {
    byte[] binaryTransactionId = ByteUtils.longToByte(transactionId);
    return generateCommitKey(revertByteArray(binaryTransactionId));
  }

  static byte[] generateCommitKey(byte[] transactionId) {
    return Bytes.concat(TRANSACTION_PREFIX, SEPARATOR, transactionId);
  }

  /** Get the end of transaction id, we use this key to scan all commit marks. */
  static byte[] endOfTransactionId() {
    // Why use 1? Because we use 1 to represent the smallest transaction id. The smaller id will
    // be larger than the bigger id when converting it to a byte array.
    return generateCommitKey(1L);
  }

  static byte[] endOfKey(byte[] key) {
    return generateKey(key, 1L);
  }

  static byte[] getRealKey(byte[] rawKey) {
    return ArrayUtils.subarray(
        rawKey, 0, rawKey.length - LENGTH_OF_TRANSACTION_ID - LENGTH_OF_SEPARATOR);
  }

  /** Get the binary transaction id from the raw key. */
  static byte[] getBinaryTransactionId(byte[] rawKey) {
    return ArrayUtils.subarray(rawKey, rawKey.length - LENGTH_OF_TRANSACTION_ID, rawKey.length);
  }

  static long getTransactionId(byte[] binaryTransactionId) {
    byte[] reverted = revertByteArray(binaryTransactionId);
    return ByteUtils.byteToLong(reverted);
  }
}
