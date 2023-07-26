/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.graviton.storage.kv;

import com.datastrato.graviton.Config;
import com.datastrato.graviton.Entity;
import com.datastrato.graviton.EntityAlreadyExistsException;
import com.datastrato.graviton.EntitySerDe;
import com.datastrato.graviton.EntityStore;
import com.datastrato.graviton.HasIdentifier;
import com.datastrato.graviton.NameIdentifier;
import com.datastrato.graviton.Namespace;
import com.datastrato.graviton.NoSuchEntityException;
import com.datastrato.graviton.util.Executable;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.locks.Lock;

/**
 * KV store to store entities. This means we can store entities in a key value store. i.e. RocksDB,
 * Cassandra, etc. If you want to use a different backend, you can implement the {@link
 * com.datastrato.graviton.storage.kv.KvBackend} interface
 */
public class KvEntityStore implements EntityStore {
  private KvBackend backend;
  private EntityKeyEncoder entityKeyEncoder;
  private EntitySerDe serDe;

  // TODO replaced with rocksdb transaction
  private Lock lock;

  @Override
  public void initialize(Config config) throws RuntimeException {
    // TODO
  }

  @Override
  public void setSerDe(EntitySerDe entitySerDe) {
    this.serDe = entitySerDe;
  }

  @Override
  public <E extends Entity & HasIdentifier> List<E> list(Namespace namespace, Class<E> e)
      throws IOException {
    // TODO
    return null;
  }

  @Override
  public boolean exists(NameIdentifier ident) throws IOException {
    return false;
  }

  @Override
  public <E extends Entity & HasIdentifier> void put(NameIdentifier ident, E e, boolean overwritten)
      throws IOException, EntityAlreadyExistsException {
    // TODO
  }

  @Override
  public <E extends Entity & HasIdentifier> E get(NameIdentifier ident, Class<E> type)
      throws NoSuchEntityException, IOException {
    // TODO
    return null;
  }

  @Override
  public boolean delete(NameIdentifier ident) throws IOException {
    return false;
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
