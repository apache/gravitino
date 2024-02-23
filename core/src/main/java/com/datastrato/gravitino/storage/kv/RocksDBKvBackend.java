/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.storage.kv;

import com.datastrato.gravitino.Config;
import com.datastrato.gravitino.Configs;
import com.datastrato.gravitino.exceptions.AlreadyExistsException;
import com.datastrato.gravitino.utils.ByteUtils;
import com.datastrato.gravitino.utils.Bytes;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.TransactionDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link RocksDBKvBackend} is a RocksDB implementation of KvBackend interface. If we want to use
 * another kv implementation, We can just implement {@link KvBackend} interface and use it in the
 * Gravitino.
 */
public class RocksDBKvBackend implements KvBackend {
  public static final Logger LOGGER = LoggerFactory.getLogger(RocksDBKvBackend.class);
  private RocksDB db;

  /**
   * Initialize the RocksDB backend instance. We have used the {@link TransactionDB} to support
   * transaction instead of {@link RocksDB} instance.
   */
  private RocksDB initRocksDB(Config config) throws RocksDBException {
    RocksDB.loadLibrary();

    String dbPath = getStoragePath(config);
    File dbDir = new File(dbPath, "instance");
    try (final Options options = new Options()) {
      options.setCreateIfMissing(true);

      if (!dbDir.exists() && !dbDir.mkdirs()) {
        throw new RocksDBException(
            String.format("Can't create RocksDB path '%s'", dbDir.getAbsolutePath()));
      }
      LOGGER.info("Rocksdb storage directory:{}", dbDir);
      // TODO (yuqi), make options configurable
      return RocksDB.open(options, dbDir.getAbsolutePath());
    } catch (RocksDBException ex) {
      LOGGER.error(
          "Error initializing RocksDB, check configurations and permissions, exception: {}, message: {}, stackTrace: {}",
          ex.getCause(),
          ex.getMessage(),
          ex.getStackTrace());
      throw ex;
    }
  }

  @VisibleForTesting
  String getStoragePath(Config config) {
    String dbPath = config.get(Configs.ENTRY_KV_ROCKSDB_BACKEND_PATH);
    if (StringUtils.isBlank(dbPath)) {
      return Configs.DEFAULT_KV_ROCKSDB_BACKEND_PATH;
    }

    Path path = Paths.get(dbPath);
    // Relative Path
    if (!path.isAbsolute()) {
      path = Paths.get(System.getenv("GRAVITINO_HOME"), dbPath);
      return path.toString();
    }

    return dbPath;
  }

  @Override
  public void initialize(Config config) throws IOException {
    try {
      db = initRocksDB(config);
    } catch (RocksDBException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void put(byte[] key, byte[] value, boolean overwrite) throws IOException {
    try {
      handlePut(key, value, overwrite);
    } catch (AlreadyExistsException e) {
      throw e;
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @VisibleForTesting
  void handlePut(byte[] key, byte[] value, boolean overwrite) throws RocksDBException {
    if (overwrite) {
      db.put(key, value);
      return;
    }
    byte[] existKey = db.get(key);
    if (existKey != null) {
      throw new AlreadyExistsException(
          "Key %s already exists in the database, please use overwrite option to overwrite it",
          ByteUtils.formatByteArray(key));
    }
    db.put(key, value);
  }

  @Override
  public byte[] get(byte[] key) throws IOException {
    try {
      return db.get(key);
    } catch (RocksDBException e) {
      throw new IOException(e);
    }
  }

  @Override
  public List<Pair<byte[], byte[]>> scan(KvRange scanRange) throws IOException {
    RocksIterator rocksIterator = db.newIterator();
    try {
      rocksIterator.seek(scanRange.getStart());

      List<Pair<byte[], byte[]>> result = Lists.newArrayList();
      int count = 0;
      while (count < scanRange.getLimit() && rocksIterator.isValid()) {
        byte[] key = rocksIterator.key();

        // Break if the key is out of the scan range
        if (Bytes.wrap(key).compareTo(scanRange.getEnd()) > 0) {
          break;
        }

        if (!scanRange.getPredicate().test(key, rocksIterator.value())) {
          rocksIterator.next();
          continue;
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
      return result;
    } finally {
      rocksIterator.close();
    }
  }

  @Override
  public boolean delete(byte[] key) throws IOException {
    try {
      db.delete(key);
      return true;
    } catch (RocksDBException e) {
      throw new IOException(e);
    }
  }

  @Override
  public boolean deleteRange(KvRange deleteRange) throws IOException {
    RocksIterator rocksIterator = db.newIterator();

    try {
      rocksIterator.seek(deleteRange.getStart());

      while (rocksIterator.isValid()) {
        byte[] key = rocksIterator.key();
        // Break if the key is out of the scan range
        if (Bytes.wrap(key).compareTo(deleteRange.getEnd()) > 0) {
          break;
        }

        if (Bytes.wrap(key).compareTo(deleteRange.getStart()) == 0) {
          if (deleteRange.isStartInclusive()) {
            delete(key);
          }
        } else if (Bytes.wrap(key).compareTo(deleteRange.getEnd()) == 0) {
          if (deleteRange.isEndInclusive()) {
            delete(key);
          }
          break;
        } else {
          delete(key);
        }

        rocksIterator.next();
      }
      return true;
    } finally {
      rocksIterator.close();
    }
  }

  @Override
  public void close() throws IOException {
    db.close();
  }

  @VisibleForTesting
  public RocksDB getDb() {
    return db;
  }

  @VisibleForTesting
  public void setDb(RocksDB db) {
    this.db = db;
  }
}
