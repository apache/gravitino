/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.storage.kv;

import com.datastrato.gravitino.storage.TransactionIdGenerator;
import com.datastrato.gravitino.utils.ByteUtils;
import java.io.IOException;

public class TransactionIdGeneratorImpl implements TransactionIdGenerator {
  private final KvBackend kvBackend;
  private static final String LAST_ID = "last_id";

  public TransactionIdGeneratorImpl(KvBackend kvBackend) {
    this.kvBackend = kvBackend;
  }

  @Override
  public synchronized long nextId() {
    try {
      byte[] oldIdBytes = kvBackend.get(LAST_ID.getBytes());
      long id = oldIdBytes == null ? 0 : ByteUtils.byteToLong(oldIdBytes) + 1;
      kvBackend.put(LAST_ID.getBytes(), ByteUtils.longToByte(id), true);
      return id;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
