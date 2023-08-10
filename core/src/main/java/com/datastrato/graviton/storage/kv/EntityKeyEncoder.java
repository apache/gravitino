/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.graviton.storage.kv;

import com.datastrato.graviton.NameIdentifier;
import com.datastrato.graviton.Namespace;

/** Interface for encoding entity keys for use with KV backends, such as RocksDB. */
public interface EntityKeyEncoder {

  /**
   * Encodes a NameIdentifier into a byte array representing the entity key.
   *
   * @param identifier The NameIdentifier to be encoded.
   * @return The byte array representing the encoded key.
   */
  byte[] encode(NameIdentifier identifier);

  /**
   * Encodes a Namespace into a byte array representing the key.
   *
   * @param namespace The Namespace to be encoded.
   * @return The byte array representing the encoded key.
   */
  byte[] encode(Namespace namespace);
}
