/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.graviton.storage.kv;


import com.datastrato.graviton.Config;
import com.datastrato.graviton.Configs;
import com.datastrato.graviton.storage.utils.Bytes;
import com.google.common.collect.Lists;
import java.io.File;
import java.io.IOException;
import java.util.List;
import org.apache.commons.lang3.tuple.Pair;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link RocksDBKvBackend} is a RocksDB implementatioxn of KvBackend interface. If we want to use
 * other kv implementation, We can just implement {@link KvBackend} interface and use it in the
 * Graviton.
 */
public class RocksDBKvBackend implements KvBackend {
  public static final Logger LOGGER = LoggerFactory.getLogger(RocksDBKvBackend.class);
  private RocksDB db;

  private RocksDB initRocksDB(Config config) throws RocksDBException {
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
      // TODO (yuqi), Use transaction db to do Transaction operation NOT Lock
      return RocksDB.open(options, dbDir.getAbsolutePath());
    } catch (RocksDBException ex) {
      LOGGER.error(
          "Error initializng RocksDB, check configurations and permissions, exception: {}, message: {}, stackTrace: {}",
          ex.getCause(),
          ex.getMessage(),
          ex.getStackTrace());
      throw ex;
    }
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
  public void put(byte[] key, byte[] value) throws IOException {
    try {
      db.put(key, value);
    } catch (RocksDBException e) {
      throw new IOException(e);
    }
  }

  @Override
  public byte[] get(byte[] key) throws IOException {
    byte[] value;
    try {
      value = db.get(key);
    } catch (RocksDBException e) {
      throw new IOException(e);
    }
    return value;
  }

  @Override
  public List<Pair<byte[], byte[]>> scan(KvRangeScan scanRange) throws IOException {

    RocksIterator rocksIterator = db.newIterator();
    rocksIterator.seek(scanRange.getStart());

    List<Pair<byte[], byte[]>> result = Lists.newArrayList();
    int count = 0;
    while (count < scanRange.getLimit() && rocksIterator.isValid()) {
      byte[] key = rocksIterator.key();
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

  @Override
  public boolean delete(byte[] key) throws IOException {
    try {
      db.delete(key);
    } catch (RocksDBException e) {
      throw new IOException(e);
    }
    return true;
  }

  @Override
  public void close() throws IOException {
    db.close();
  }

}
