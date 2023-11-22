/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.storage.kv;

import com.datastrato.gravitino.storage.IdGenerator;
import com.datastrato.gravitino.storage.NameMappingService;
import com.datastrato.gravitino.storage.RandomIdGenerator;
import com.datastrato.gravitino.storage.TransactionIdGenerator;
import com.datastrato.gravitino.utils.ByteUtils;
import com.datastrato.gravitino.utils.Bytes;
import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import javax.annotation.concurrent.ThreadSafe;

/**
 * {@link KvNameMappingService} is an implementation that use {@link KvBackend} to store name to id
 * mapping.
 */
@ThreadSafe
public class KvNameMappingService implements NameMappingService {

  // TODO(yuqi) Make this configurable
  @VisibleForTesting final KvBackend backend;
  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
  @VisibleForTesting final IdGenerator idGenerator = new RandomIdGenerator();

  // name prefix of name in name to id mapping,
  // e.g., name_metalake1 -> 1
  //       name_metalake2 -> 2
  private static final byte[] NAME_PREFIX = "name_".getBytes(StandardCharsets.UTF_8);

  // id prefix of id in name to id mapping,
  // e.g., id_1 -> metalake1
  //       id_2 -> metalake2
  private static final byte[] ID_PREFIX = "id_".getBytes(StandardCharsets.UTF_8);

  private final TransactionalKvBackend transactionalKvBackend;

  public KvNameMappingService(KvBackend backend, TransactionIdGenerator txIdGenerator) {
    this.backend = backend;
    transactionalKvBackend = new TransactionalKvBackendImpl(backend, txIdGenerator);
  }

  @Override
  public Long getIdByName(String name) throws IOException {
    lock.readLock().lock();
    try {
      transactionalKvBackend.begin();
      byte[] nameByte = Bytes.concat(NAME_PREFIX, name.getBytes(StandardCharsets.UTF_8));
      byte[] idByte = transactionalKvBackend.get(nameByte);
      return idByte == null ? null : ByteUtils.byteToLong(idByte);
    } finally {
      transactionalKvBackend.closeTransaction();
      lock.readLock().unlock();
    }
  }

  private long bindNameAndId(String name) throws IOException {
    byte[] nameByte = Bytes.concat(NAME_PREFIX, name.getBytes(StandardCharsets.UTF_8));
    long id = idGenerator.nextId();
    lock.writeLock().lock();
    try {
      transactionalKvBackend.begin();
      transactionalKvBackend.put(nameByte, ByteUtils.longToByte(id), false);
      byte[] idByte = Bytes.concat(ID_PREFIX, ByteUtils.longToByte(id));
      transactionalKvBackend.put(idByte, name.getBytes(StandardCharsets.UTF_8), false);
      transactionalKvBackend.commit();
      return id;
    } finally {
      transactionalKvBackend.closeTransaction();
      lock.writeLock().unlock();
    }
  }

  @Override
  public boolean updateName(String oldName, String newName) throws IOException {
    lock.writeLock().lock();
    try {
      transactionalKvBackend.begin();
      byte[] nameByte = Bytes.concat(NAME_PREFIX, oldName.getBytes(StandardCharsets.UTF_8));
      byte[] oldIdValue = transactionalKvBackend.get(nameByte);

      // Old mapping has been deleted, no need to do it;
      if (oldIdValue == null) {
        return false;
      }
      // Delete old name --> id mapping
      transactionalKvBackend.delete(nameByte);

      transactionalKvBackend.put(
          Bytes.concat(NAME_PREFIX, newName.getBytes(StandardCharsets.UTF_8)), oldIdValue, false);
      transactionalKvBackend.put(
          Bytes.concat(ID_PREFIX, oldIdValue), newName.getBytes(StandardCharsets.UTF_8), true);
      transactionalKvBackend.commit();
      return true;
    } finally {
      transactionalKvBackend.closeTransaction();
      lock.writeLock().unlock();
    }
  }

  @Override
  public boolean unbindNameAndId(String name) throws IOException {
    byte[] nameByte = Bytes.concat(NAME_PREFIX, name.getBytes(StandardCharsets.UTF_8));

    lock.writeLock().lock();
    try {
      transactionalKvBackend.begin();
      boolean r = transactionalKvBackend.delete(nameByte);
      transactionalKvBackend.commit();
      return r;
    } finally {
      transactionalKvBackend.closeTransaction();
      lock.writeLock().unlock();
    }
  }

  @Override
  public void close() throws Exception {}

  @Override
  public long getOrCreateIdFromName(String name) throws IOException {
    Long id = getIdByName(name);
    if (id == null) {
      synchronized (this) {
        if ((id = getIdByName(name)) == null) {
          id = bindNameAndId(name);
        }
      }
    }

    return id;
  }
}
