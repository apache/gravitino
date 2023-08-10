/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.graviton.storage.kv;

import com.datastrato.graviton.Entity.EntityIdentifer;
import java.io.IOException;

/** Interface for encoding entity keys for use with KV backends, such as RocksDB. */
public interface EntityKeyEncoder {
  /**
   * Construct the key for key-value store from the entity NameIdentifier and EntityType.
   *
   * @param entityIdentifer entity identifier
   * @param createIdIfNotExists create the id mapping for name if not exists
   * @return encoded key for key-value store
   * @throws IOException
   */
  byte[] encode(EntityIdentifer entityIdentifer, boolean createIdIfNotExists) throws IOException;
}
