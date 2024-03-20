/*
 * Copyright 2023 Datastrato Pvt Ltd.
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
  static final byte[] GENERAL_NAME_MAPPING_PREFIX = new byte[] {0x1D, 0x00, 0x00};

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
    return FunctionUtils.executeInTransaction(
        () -> {
          byte[] idByte = transactionalKvBackend.get(nameByte);
          return idByte == null ? null : ByteUtils.byteToLong(idByte);
        },
        transactionalKvBackend);
  }

  @Override
  public String getNameById(long id) throws IOException {
    byte[] idByte = getIdKey(id);
    return FunctionUtils.executeInTransaction(
        () -> {
          byte[] name = transactionalKvBackend.get(idByte);
          return name == null ? null : new String(name, StandardCharsets.UTF_8);
        },
        transactionalKvBackend);
  }

  private long bindNameAndId(String name) throws IOException {
    byte[] nameByte = getNameKey(name);
    long id = idGenerator.nextId();
    byte[] idByte = getIdKey(id);
    return FunctionUtils.executeInTransaction(
        () -> {
          transactionalKvBackend.put(nameByte, ByteUtils.longToByte(id), false);
          transactionalKvBackend.put(idByte, name.getBytes(StandardCharsets.UTF_8), false);
          return id;
        },
        transactionalKvBackend);
  }

  @Override
  public boolean updateName(String oldName, String newName) throws IOException {
    return FunctionUtils.executeInTransaction(
        () -> {
          byte[] nameByte = getNameKey(oldName);
          byte[] oldIdValue = transactionalKvBackend.get(nameByte);
          // Old mapping has been deleted, no need to do it;
          if (oldIdValue == null) {
            return false;
          }

          // Delete old name --> id mapping
          transactionalKvBackend.delete(nameByte);
          // In case there exists the mapping of new_name --> id, so we should use
          // the overwritten strategy. In the following scenario, we should use the
          // overwritten strategy:
          // 1. Create name1
          // 2. Delete name1
          // 3. Create name2
          // 4. Rename name2 -> name1
          transactionalKvBackend.put(getNameKey(newName), oldIdValue, true);
          transactionalKvBackend.put(oldIdValue, newName.getBytes(StandardCharsets.UTF_8), true);
          return true;
        },
        transactionalKvBackend);
  }

  @Override
  public boolean unbindNameAndId(String name) throws IOException {
    byte[] nameByte = Bytes.concat(NAME_PREFIX, name.getBytes(StandardCharsets.UTF_8));
    return FunctionUtils.executeInTransaction(
        () -> {
          byte[] idByte = transactionalKvBackend.get(nameByte);
          if (idByte == null) {
            return false;
          }
          transactionalKvBackend.delete(nameByte);
          transactionalKvBackend.delete(Bytes.concat(ID_PREFIX, idByte));
          return true;
        },
        transactionalKvBackend);
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
