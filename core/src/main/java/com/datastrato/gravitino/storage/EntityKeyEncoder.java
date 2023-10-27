/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.storage;

import com.datastrato.gravitino.Entity.EntityType;
import com.datastrato.gravitino.NameIdentifier;
import java.io.IOException;
import org.apache.commons.lang3.tuple.Pair;

/** Interface for encoding entity to storage it in underlying storage. */
public interface EntityKeyEncoder<T> {
  /**
   * Construct the key for key-value store from the entity NameIdentifier and EntityType.
   *
   * @param ident entity identifier to encode
   * @param type entity type to encode
   * @return encoded key for key-value stored
   * @throws IOException Exception if error occurs
   */
  default T encode(NameIdentifier ident, EntityType type) throws IOException {
    return encode(ident, type, false);
  }

  /**
   * Construct the key for key-value store from the entity NameIdentifier and EntityType.
   *
   * @param nullIfMissing return null if the specific entity no found
   * @return encoded key for key-value stored
   * @throws IOException Exception if error occurs
   */
  T encode(NameIdentifier ident, EntityType type, boolean nullIfMissing) throws IOException;

  /** Decode the key from key-value store to entity NameIdentifier and EntityType. */
  default Pair<NameIdentifier, EntityType> decode(T key) throws IOException {
    throw new UnsupportedOperationException("Currently not supported");
  }
}
