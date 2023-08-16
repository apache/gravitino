/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.graviton.storage;

import com.datastrato.graviton.Entity.EntityIdentifier;
import com.datastrato.graviton.Entity.EntityType;
import com.datastrato.graviton.NameIdentifier;
import java.io.IOException;

/** Interface for encoding entity to storage it in underlying storage. E.g. RocksDB. */
public interface EntityKeyEncoder<T> {
  /**
   * Construct the key for key-value store from the entity NameIdentifier and EntityType.
   *
   * @param entityIdentifier entity identifier to encode
   * @param createIdIfNotExists create the id mapping for name if not exists
   * @return encoded key for key-value store
   * @throws IOException Exception if error occurs
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
  default T encode(NameIdentifier ident, EntityType type) throws IOException {
    throw new UnsupportedOperationException("Not implemented yet");
  }
}
