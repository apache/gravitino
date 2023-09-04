/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.graviton.storage;

import com.datastrato.graviton.Entity.EntityType;
import com.datastrato.graviton.NameIdentifier;
import java.io.IOException;

/** Interface for encoding entity to storage it in underlying storage. E.g. RocksDB. */
public interface EntityKeyEncoder<T> {
  /**
   * Construct the key for key-value store from the entity NameIdentifier and EntityType.
   *
   * @param ident entity identifier to encode
   * @param type entity type to encode
   * @return encoded key for key-value stored
   * @throws IOException, Exception if error occurs
   */
  default T encode(NameIdentifier ident, EntityType type) throws IOException {
    return encode(ident, type, false);
  }

  /**
   * Construct the key for key-value store from the entity NameIdentifier and EntityType.
   *
   * @param nullIfMissing return null if the specific entity no found
   * @return encoded key for key-value stored
   * @throws IOException, Exception if error occurs
   */
  T encode(NameIdentifier ident, EntityType type, boolean nullIfMissing) throws IOException;
}
