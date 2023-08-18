/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.graviton.storage.kv;

import com.datastrato.graviton.storage.IdGenerator;
import com.datastrato.graviton.storage.NameMappingService;
import com.datastrato.graviton.storage.RandomIdGenerator;
import com.datastrato.graviton.util.ByteUtils;
import com.datastrato.graviton.util.Bytes;
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

  // name prefix of name in name to id mapping,
  // e.g., name_metalake1 -> 1
  //       name_metalake2 -> 2
  private static final byte[] NAME_PREFIX = "name_".getBytes();

  // id prefix of id in name to id mapping,
  // e.g., id_1 -> metalake1
  //       id_2 -> metalake2
  private static final byte[] ID_PREFIX = "id_".getBytes();

  public KvNameMappingService(KvBackend backend) {
    this.backend = backend;
  }

  @Override
  public Long getIdByName(String name) throws IOException {
    lock.readLock().lock();
    try {
      byte[] nameByte = Bytes.concat(NAME_PREFIX, name.getBytes());
      byte[] idByte = backend.get(nameByte);
      return idByte == null ? null : ByteUtils.byteToLong(idByte);
    } finally {
      lock.readLock().unlock();
    }
  }

  public long bindNameAndId(String name) throws IOException {
    byte[] nameByte = Bytes.concat(NAME_PREFIX, name.getBytes());
    long id = idGenerator.nextId();
    lock.writeLock().lock();
    try {
      return backend.executeInTransaction(
          () -> {
            backend.put(nameByte, ByteUtils.longToByte(id), false);
            byte[] idByte = Bytes.concat(ID_PREFIX, ByteUtils.longToByte(id));
            backend.put(idByte, name.getBytes(), false);
            return id;
          });
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public boolean updateName(String oldName, String newName) throws IOException {
    lock.writeLock().lock();
    try {
      return backend.executeInTransaction(
          () -> {
            byte[] nameByte = Bytes.concat(NAME_PREFIX, oldName.getBytes());
            byte[] oldIdValue = backend.get(nameByte);

            // Old mapping has been deleted, no need to do it;
            if (oldIdValue == null) {
              return false;
            }
            // Delete old name --> id mapping
            backend.delete(nameByte);

            backend.put(Bytes.concat(NAME_PREFIX, newName.getBytes()), oldIdValue, false);
            backend.put(Bytes.concat(ID_PREFIX, oldIdValue), newName.getBytes(), true);
            return true;
          });
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public boolean unbindNameAndId(String name) throws IOException {
    byte[] nameByte = Bytes.concat(NAME_PREFIX, name.getBytes());

    lock.writeLock().lock();
    try {
      return backend.delete(nameByte);
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
