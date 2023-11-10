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
  private final TransactionIdGenerator txIdGenerator;

  // name prefix of name in name to id mapping,
  // e.g., name_metalake1 -> 1
  //       name_metalake2 -> 2
  private static final byte[] NAME_PREFIX = "name_".getBytes();

  // id prefix of id in name to id mapping,
  // e.g., id_1 -> metalake1
  //       id_2 -> metalake2
  private static final byte[] ID_PREFIX = "id_".getBytes();

  public KvNameMappingService(KvBackend backend, TransactionIdGenerator txIdGenerator) {
    this.backend = backend;
    this.txIdGenerator = txIdGenerator;
  }

  @Override
  public Long getIdByName(String name) throws IOException {
    lock.readLock().lock();
    try (TransactionalKvBackend transactionalKvBackend =
        new TransactionalKvBackendImpl(backend, txIdGenerator)) {
      transactionalKvBackend.begin();
      byte[] nameByte = Bytes.concat(NAME_PREFIX, name.getBytes());
      byte[] idByte = transactionalKvBackend.get(nameByte);
      return idByte == null ? null : ByteUtils.byteToLong(idByte);
    } finally {
      lock.readLock().unlock();
    }
  }

  private long bindNameAndId(String name) throws IOException {
    byte[] nameByte = Bytes.concat(NAME_PREFIX, name.getBytes());
    long id = idGenerator.nextId();
    lock.writeLock().lock();
    try (TransactionalKvBackendImpl kvTransactionManager =
        new TransactionalKvBackendImpl(backend, txIdGenerator)) {
      kvTransactionManager.begin();
      kvTransactionManager.put(nameByte, ByteUtils.longToByte(id), false);
      byte[] idByte = Bytes.concat(ID_PREFIX, ByteUtils.longToByte(id));
      kvTransactionManager.put(idByte, name.getBytes(), false);
      kvTransactionManager.commit();
      return id;
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public boolean updateName(String oldName, String newName) throws IOException {
    lock.writeLock().lock();
    try (TransactionalKvBackendImpl kvTransactionManager =
        new TransactionalKvBackendImpl(backend, txIdGenerator)) {
      kvTransactionManager.begin();
      byte[] nameByte = Bytes.concat(NAME_PREFIX, oldName.getBytes());
      byte[] oldIdValue = kvTransactionManager.get(nameByte);

      // Old mapping has been deleted, no need to do it;
      if (oldIdValue == null) {
        return false;
      }
      // Delete old name --> id mapping
      kvTransactionManager.delete(nameByte);

      kvTransactionManager.put(Bytes.concat(NAME_PREFIX, newName.getBytes()), oldIdValue, false);
      kvTransactionManager.put(Bytes.concat(ID_PREFIX, oldIdValue), newName.getBytes(), true);
      kvTransactionManager.commit();
      return true;
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public boolean unbindNameAndId(String name) throws IOException {
    byte[] nameByte = Bytes.concat(NAME_PREFIX, name.getBytes());

    lock.writeLock().lock();
    try (TransactionalKvBackendImpl kvTransactionManager =
        new TransactionalKvBackendImpl(backend, txIdGenerator)) {
      kvTransactionManager.begin();
      boolean r = kvTransactionManager.delete(nameByte);
      kvTransactionManager.commit();
      return r;
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public void close() throws Exception {
    backend.close();
  }

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
