/*
 * Copyright 2023 DATASTRATO Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.storage.kv;

import com.datastrato.gravitino.storage.FunctionUtils;
import com.datastrato.gravitino.storage.IdGenerator;
import com.datastrato.gravitino.storage.NameMappingService;
import com.datastrato.gravitino.storage.RandomIdGenerator;
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

  @VisibleForTesting final ReentrantReadWriteLock lock;
  @VisibleForTesting final IdGenerator idGenerator = new RandomIdGenerator();

  // To separate it from user keys, we will add three control flag 0x1D, 0x00, 0x00 as the prefix.
  private static final byte[] GENERAL_NAME_MAPPING_PREFIX = new byte[] {0x1D, 0x00, 0x00};

  // Name prefix of name in name to id mapping,
  // e.g., name_metalake1 -> 1
  //       name_metalake2 -> 2
  private static final byte[] NAME_PREFIX =
      Bytes.concat(GENERAL_NAME_MAPPING_PREFIX, "name_".getBytes(StandardCharsets.UTF_8));

  // Id prefix of id in name to id mapping,
  // e.g., id_1 -> metalake1
  //       id_2 -> metalake2
  private static final byte[] ID_PREFIX =
      Bytes.concat(GENERAL_NAME_MAPPING_PREFIX, "id_".getBytes(StandardCharsets.UTF_8));

  @VisibleForTesting final TransactionalKvBackend transactionalKvBackend;

  public KvNameMappingService(
      TransactionalKvBackend transactionalKvBackend,
      ReentrantReadWriteLock reentrantReadWriteLock) {
    this.transactionalKvBackend = transactionalKvBackend;
    this.lock = reentrantReadWriteLock;
  }

  @Override
  public Long getIdByName(String name) throws IOException {
    byte[] nameByte = getNameKey(name);
    return FunctionUtils.executeWithReadLock(
        () ->
            FunctionUtils.executeInTransaction(
                () -> {
                  byte[] idByte = transactionalKvBackend.get(nameByte);
                  return idByte == null ? null : ByteUtils.byteToLong(idByte);
                },
                transactionalKvBackend),
        lock);
  }

  private long bindNameAndId(String name) throws IOException {
    byte[] nameByte = getNameKey(name);
    long id = idGenerator.nextId();
    byte[] idByte = getIdKey(id);
    return FunctionUtils.executeWithWriteLock(
        () ->
            FunctionUtils.executeInTransaction(
                () -> {
                  transactionalKvBackend.put(nameByte, ByteUtils.longToByte(id), false);
                  transactionalKvBackend.put(idByte, name.getBytes(StandardCharsets.UTF_8), false);
                  return id;
                },
                transactionalKvBackend),
        lock);
  }

  @Override
  public boolean updateName(String oldName, String newName) throws IOException {
    return FunctionUtils.executeWithWriteLock(
        () ->
            FunctionUtils.executeInTransaction(
                () -> {
                  byte[] nameByte = getNameKey(oldName);
                  byte[] oldIdValue = transactionalKvBackend.get(nameByte);
                  // Old mapping has been deleted, no need to do it;
                  if (oldIdValue == null) {
                    return false;
                  }

                  // Delete old name --> id mapping
                  transactionalKvBackend.delete(nameByte);

                  transactionalKvBackend.put(getNameKey(newName), oldIdValue, false);
                  transactionalKvBackend.put(
                      oldIdValue, newName.getBytes(StandardCharsets.UTF_8), true);
                  return true;
                },
                transactionalKvBackend),
        lock);
  }

  @Override
  public boolean unbindNameAndId(String name) throws IOException {
    byte[] nameByte = Bytes.concat(NAME_PREFIX, name.getBytes(StandardCharsets.UTF_8));
    return FunctionUtils.executeWithWriteLock(
        () ->
            FunctionUtils.executeInTransaction(
                () -> {
                  byte[] idByte = transactionalKvBackend.get(nameByte);
                  if (idByte == null) {
                    return false;
                  }
                  transactionalKvBackend.delete(nameByte);
                  transactionalKvBackend.delete(Bytes.concat(ID_PREFIX, idByte));
                  return true;
                },
                transactionalKvBackend),
        lock);
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

  /** Generate key to store id to name mapping. */
  private static byte[] getIdKey(long id) {
    return Bytes.concat(ID_PREFIX, ByteUtils.longToByte(id));
  }

  /** Generate key to store name to id mapping. */
  private static byte[] getNameKey(String name) {
    return Bytes.concat(NAME_PREFIX, name.getBytes(StandardCharsets.UTF_8));
  }
}
