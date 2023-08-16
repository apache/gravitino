/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.graviton.storage.kv;

import com.datastrato.graviton.Entity.EntityIdentifier;
import com.datastrato.graviton.Entity.EntityType;
import com.datastrato.graviton.NameIdentifier;
import java.io.IOException;

/**
 * Interface for encoding entity key for KV backend, e.g., RocksDB. The key is used to store the
 * entity in the backend.
 */
public interface EntityKeyEncoder {
  /**
   * Construct the key for key-value store from the entity NameIdentifier and EntityType.
   *
   * @param entityIdentifier entity identifier to encode
   * @param createIdIfNotExists create the id mapping for name if not exists
   * @return encoded key for key-value store
   * @throws IOException
   */
  byte[] encode(EntityIdentifier entityIdentifier, boolean createIdIfNotExists) throws IOException;

  /**
   * Construct the key for key-value store from the entity NameIdentifier and EntityType. Note, This
   * method is the replacement of {@link #encode(EntityIdentifier, boolean)}. When we implement this
   * method, we should mark {@link #encode(EntityIdentifier, boolean)} as deprecated and remove it
   * later.
   *
   * @param ident entity identifier to encode
   * @param type entity type to encode
   * @return encoded key for key-value store
   * @throws IOException, Exception if error occurs
   */
  default byte[] encode(NameIdentifier ident, EntityType type) throws IOException {
    return new byte[0];
  }
}
