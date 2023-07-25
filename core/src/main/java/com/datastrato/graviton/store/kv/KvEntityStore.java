/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.store.kv;

import static com.datastrato.graviton.Configs.DEFUALT_ENTRY_KV_BACKEND;
import static com.datastrato.graviton.Configs.ENTRY_KV_BACKEND_TYPE;

import com.datastrato.graviton.Config;
import com.datastrato.graviton.Configs;
import com.datastrato.graviton.Entity;
import com.datastrato.graviton.EntityAlreadyExistsException;
import com.datastrato.graviton.EntitySerDe;
import com.datastrato.graviton.EntitySerDeFactory;
import com.datastrato.graviton.EntityStore;
import com.datastrato.graviton.HasIdentifier;
import com.datastrato.graviton.NameIdentifier;
import com.datastrato.graviton.Namespace;
import com.datastrato.graviton.NoSuchEntityException;
import com.datastrato.graviton.util.Bytes;
import com.datastrato.graviton.util.Executable;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.commons.lang3.tuple.Pair;

/**
 * KV store to store entities. This means we can store entities in a key value store. i.e. RocksDB,
 * Cassandra, etc. If you want to use a different backend, you can implement the {@link KvBackend}
 * interface
 */
public class KvEntityStore implements EntityStore {
  private KvBackend backend;
  private EntityKeyEncoder entityKeyEncoder;
  private EntitySerDe serDe;

  // TODO replaced with rocksdb transaction
  private Lock lock;

  @Override
  public void initialize(Config config) throws RuntimeException {
    try {
      if (DEFUALT_ENTRY_KV_BACKEND.equals(config.get(ENTRY_KV_BACKEND_TYPE))) {
        backend = new RocksDBKvBackend();
        backend.initialize(config);
      } else {
        throw new RuntimeException("Unsupported backend type...");
      }
      serDe = EntitySerDeFactory.createEntitySerDe(config.get(Configs.ENTITY_SERDE));
      this.setSerDe(serDe);
      lock = new ReentrantLock();
      this.entityKeyEncoder = new CustomEntityKeyEncoder();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void setSerDe(EntitySerDe entitySerDe) {
    this.serDe = entitySerDe;
  }

  @Override
  public <E extends Entity & HasIdentifier> List<E> list(Namespace namespace, Class<E> e)
      throws IOException {
    byte[] startKey = entityKeyEncoder.encode(namespace);
    byte[] endKey = Bytes.increment(Bytes.wrap(startKey)).get();
    List<Pair<byte[], byte[]>> kvs =
        backend.scan(
            new RockDBHelper.ScanRange.ScanRangeBuilder()
                .start(startKey)
                .end(endKey)
                .startInclusive(true)
                .endInclusive(false)
                .limit(Integer.MAX_VALUE)
                .build());

    List<E> entities = Lists.newArrayList();
    for (Pair<byte[], byte[]> pairs : kvs) {
      entities.add(serDe.deserialize(pairs.getRight(), e));
    }
    return entities;
  }

  @Override
  public boolean exists(NameIdentifier ident) throws IOException {
    return false;
  }

  @Override
  public <E extends Entity & HasIdentifier> void put(NameIdentifier ident, E e, boolean overwritten)
      throws IOException, EntityAlreadyExistsException {
    // Simple implementation, just use the entity's identifier as the key
    byte[] key = entityKeyEncoder.encode(ident);
    byte[] value = serDe.serialize(e);

    executeInTransaction(
        () -> {
          if (overwritten) {
            backend.put(key, value);
          } else {
            byte[] origin = backend.get(key);
            if (origin == null) {
              backend.put(key, value);
            } else {
              throw new EntityAlreadyExistsException(ident.toString());
            }
          }
          return null;
        });
  }

  @Override
  public <E extends Entity & HasIdentifier> E get(NameIdentifier ident, Class<E> type)
      throws NoSuchEntityException, IOException {
    byte[] key = entityKeyEncoder.encode(ident);
    byte[] value = backend.get(key);
    if (value == null) {
      throw new NoSuchEntityException(ident.toString());
    }
    return serDe.deserialize(value, type);
  }

  @Override
  public boolean delete(NameIdentifier ident) throws IOException {
    return executeInTransaction(
        () -> {
          byte[] key = entityKeyEncoder.encode(ident);
          return backend.delete(key);
        });
  }

  @Override
  public <R> R executeInTransaction(Executable<R> executable) throws IOException {
    lock.lock();
    try {
      return executable.execute();
    } finally {
      lock.unlock();
    }
  }

  @Override
  public void close() throws IOException {
    backend.close();
  }
}
