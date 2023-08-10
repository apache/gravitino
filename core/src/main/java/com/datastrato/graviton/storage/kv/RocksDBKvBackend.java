/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.graviton.storage.kv;

import com.datastrato.graviton.Config;
import com.datastrato.graviton.Configs;
import com.datastrato.graviton.EntityAlreadyExistsException;
import com.datastrato.graviton.util.Bytes;
import com.datastrato.graviton.util.Executable;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import java.io.File;
import java.io.IOException;
import java.util.List;
import org.apache.commons.lang3.tuple.Pair;
import org.rocksdb.Options;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.Transaction;
import org.rocksdb.TransactionDB;
import org.rocksdb.TransactionDBOptions;
import org.rocksdb.WriteOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link RocksDBKvBackend} is a RocksDB implementation of the KvBackend interface. If we want to
 * use another kv implementation, We can just implement the {@link KvBackend} interface and use it
 * in Graviton.
 */
public class RocksDBKvBackend implements KvBackend {
  public static final Logger LOGGER = LoggerFactory.getLogger(RocksDBKvBackend.class);
  private TransactionDB db;

  public static final ThreadLocal<Transaction> TX_LOCAL = new ThreadLocal<>();

  /**
   * Initialize the RocksDB backend instance. We have used the {@link TransactionDB} to support
   * transaction instead of {@link RocksDB} instance.
   *
   * @param config The configuration for the backend.
   * @return The initialized RocksDB backend instance.
   * @throws RocksDBException If there's an issue initializing RocksDB.
   */
  private TransactionDB initRocksDB(Config config) throws RocksDBException {
    RocksDB.loadLibrary();
    final Options options = new Options();
    options.setCreateIfMissing(true);

    String dbPath = config.get(Configs.ENTRY_KV_ROCKSDB_BACKEND_PATH);
    File dbDir = new File(dbPath, "instance");
    try {
      if (!dbDir.exists() && !dbDir.mkdirs()) {
        throw new RocksDBException(
            String.format("Can't create RocksDB path '%s'", dbDir.getAbsolutePath()));
      }
      // TODO (yuqi), make options and transactionDBOptions configurable
      TransactionDBOptions transactionDBOptions = new TransactionDBOptions();
      return TransactionDB.open(options, transactionDBOptions, dbDir.getAbsolutePath());
    } catch (RocksDBException ex) {
      LOGGER.error(
          "Error initializing RocksDB, check configurations and permissions, exception: {}, message: {}, stackTrace: {}",
          ex.getCause(),
          ex.getMessage(),
          ex.getStackTrace());
      throw ex;
    }
  }

  /**
   * Initializes the RocksDB backend instance.
   *
   * @param config The configuration containing settings for the backend initialization.
   * @throws IOException If an I/O error occurs during the initialization process.
   */
  @Override
  public void initialize(Config config) throws IOException {
    try {
      db = initRocksDB(config);
    } catch (RocksDBException e) {
      throw new IOException(e);
    }
  }

  /**
   * Stores a key-value pair in the RocksDB database.
   *
   * @param key The key to store the value under.
   * @param value The value to be stored.
   * @param overwrite If true, overwrite the value if the key already exists.
   * @throws IOException If an I/O error occurs during the put operation.
   * @throws EntityAlreadyExistsException If the key already exists and overwrite is false.
   */
  @Override
  public void put(byte[] key, byte[] value, boolean overwrite) throws IOException {
    Transaction tx = TX_LOCAL.get();
    try {
      // Do without transaction if not in transaction
      if (tx == null) {
        handlePutWithoutTransaction(key, value, overwrite);
        return;
      }

      // Now try with transaction
      handlePutWithTransaction(key, value, overwrite, tx);
    } catch (EntityAlreadyExistsException e) {
      throw e;
    } catch (Throwable e) {
      throw new IOException(e);
    }
  }

  private void handlePutWithTransaction(byte[] key, byte[] value, boolean overwrite, Transaction tx)
      throws RocksDBException {
    if (overwrite) {
      tx.put(key, value);
      return;
    }

    byte[] existKey = tx.get(new ReadOptions(), key);
    if (existKey != null) {
      throw new EntityAlreadyExistsException(
          String.format(
              "Key %s already exists in the database, please use overwrite option to overwrite it",
              key));
    }
    tx.put(key, value);
  }

  private void handlePutWithoutTransaction(byte[] key, byte[] value, boolean overwrite)
      throws RocksDBException {
    if (overwrite) {
      db.put(key, value);
      return;
    }
    byte[] existKey = db.get(key);
    if (existKey != null) {
      throw new EntityAlreadyExistsException(
          String.format(
              "Key %s already exists in the database, please use overwrite option to overwrite it",
              key));
    }
    db.put(key, value);
  }

  /**
   * Retrieves the value associated with the given key from the RocksDB database.
   *
   * @param key The key for which to retrieve the value.
   * @return The value associated with the given key, or null if the key does not exist.
   * @throws IOException If an I/O error occurs during the get operation.
   */
  @Override
  public byte[] get(byte[] key) throws IOException {
    try {
      if (TX_LOCAL.get() != null) {
        return TX_LOCAL.get().get(new ReadOptions(), key);
      }

      return db.get(key);
    } catch (RocksDBException e) {
      throw new IOException(e);
    }
  }

  /**
   * Scans the range of key-value pairs within the specified {@link KvRangeScan} parameters.
   *
   * @param scanRange The {@link KvRangeScan} object specifying the scan range and other options.
   * @return A list of {@link Pair} objects representing key-value pairs within the scan range.
   * @throws IOException If an I/O error occurs during the scan operation.
   */
  @Override
  public List<Pair<byte[], byte[]>> scan(KvRangeScan scanRange) throws IOException {
    Transaction tx = TX_LOCAL.get();
    RocksIterator rocksIterator =
        TX_LOCAL.get() == null ? db.newIterator() : tx.getIterator(new ReadOptions());
    rocksIterator.seek(scanRange.getStart());

    List<Pair<byte[], byte[]>> result = Lists.newArrayList();
    int count = 0;
    while (count < scanRange.getLimit() && rocksIterator.isValid()) {
      byte[] key = rocksIterator.key();

      // Break if the key is out of the scan range
      if (Bytes.wrap(key).compareTo(scanRange.getEnd()) > 0) {
        break;
      }

      if (Bytes.wrap(key).compareTo(scanRange.getStart()) == 0) {
        if (scanRange.isStartInclusive()) {
          result.add(Pair.of(key, rocksIterator.value()));
          count++;
        }
      } else if (Bytes.wrap(key).compareTo(scanRange.getEnd()) == 0) {
        if (scanRange.isEndInclusive()) {
          result.add(Pair.of(key, rocksIterator.value()));
        }
        break;
      } else {
        result.add(Pair.of(key, rocksIterator.value()));
        count++;
      }

      rocksIterator.next();
    }

    rocksIterator.close();
    return result;
  }

  /**
   * Deletes the key-value pair associated with the given key from the RocksDB database.
   *
   * @param key The key of the key-value pair to delete.
   * @return True if the key-value pair was deleted, false if the key does not exist.
   * @throws IOException If an I/O error occurs during the delete operation.
   */
  @Override
  public boolean delete(byte[] key) throws IOException {
    try {
      if (TX_LOCAL.get() != null) {
        TX_LOCAL.get().delete(key);
        return true;
      }

      db.delete(key);
      return true;
    } catch (RocksDBException e) {
      throw new IOException(e);
    }
  }

  /**
   * Closes the RocksDB database.
   *
   * @throws IOException If an I/O error occurs during the closing process.
   */
  @Override
  public void close() throws IOException {
    db.close();
  }

  /**
   * Executes a transactional operation within the context of a RocksDB transaction.
   *
   * @param executable The {@link Executable} instance representing the transactional operation.
   * @param <R> The type of result returned by the executable.
   * @param <E> The type of exception that the executable might throw.
   * @return The result of the transactional operation.
   * @throws E If the executable throws an exception of type E.
   * @throws IOException If an I/O error occurs during the transactional operation.
   */
  @Override
  public <R, E extends Exception> R executeInTransaction(Executable<R, E> executable)
      throws E, IOException {
    // Already in tx, directly execute it without creating a new tx
    if (TX_LOCAL.get() != null) {
      return executable.execute();
    }

    Transaction tx = db.beginTransaction(new WriteOptions());
    LOGGER.info("Starting transaction: {}", tx);
    TX_LOCAL.set(tx);
    try {
      R r = executable.execute();
      tx.commit();
      return r;
    } catch (RocksDBException e) {
      rollback(tx, e);
      throw new IOException(e);
    } catch (Exception e) {
      rollback(tx, e);
      throw e;
    } finally {
      tx.close();
      LOGGER.info("Transaction close: {}", tx);
      TX_LOCAL.remove();
    }
  }

  private void rollback(Transaction tx, Exception e) {
    LOGGER.error(
        "Error executing transaction, exception: {}, message: {}, stackTrace: \n{}",
        e.getCause(),
        e.getMessage(),
        Throwables.getStackTraceAsString(e));

    try {
      tx.rollback();
    } catch (Exception e1) {
      LOGGER.error(
          "Error rolling back transaction, exception: {}, message: {}, stackTrace: \n{}",
          e1.getCause(),
          e1.getMessage(),
          Throwables.getStackTraceAsString(e));
    }
  }

  @Override
  public boolean isInTransaction() {
    // TODO (yuqi), check if the transaction is still valid and We do not allow transaction nesting
    //  That is, A transaction can not be started inside another transaction when outer transaction
    //  is still active
    return TX_LOCAL.get() != null;
  }
}
