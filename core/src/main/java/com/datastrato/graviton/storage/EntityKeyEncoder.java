/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.graviton.storage;

import com.datastrato.graviton.Entity.EntityType;
import com.datastrato.graviton.NameIdentifier;
import java.io.IOException;
import java.util.List;

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

  /**
   * Get key prefix of all sub-entities under a specific entities. For example, if we use {@link
   * com.datastrato.graviton.storage.kv.BinaryEntityKeyEncoder} as the default coder, as a metalake
   * will start with `ml_{metalake_id}`, sub-entities under this metalake will have the prefix
   *
   * <pre>
   *   catalog: ca_{metalake_id}
   *   schema:  sc_{metalake_id}
   *   table:   ta_{metalake_id}
   * </pre>
   *
   * @param identifier
   * @param type
   * @return
   * @throws IOException
   */
  List<T> encodeSubEntityPrefix(NameIdentifier identifier, EntityType type) throws IOException;

  /**
   * Generate the key of name-id mapping. If we use {@link
   * com.datastrato.graviton.storage.kv.BinaryEntityKeyEncoder} Assuming we have a name identifier
   * 'a.b.c.d' that represent a table, the default key-value encoding are as followings
   *
   * <pre>
   *              key      value
   *   (metalake)     a   --> 1
   *   (catalog)    1/b   --> 2
   *   (schema)   1/2/c   --> 3
   *   (table)  1/2/3/d   --> 4
   * </pre>
   *
   * @param nameIdentifier
   * @return
   * @throws IOException
   */
  String generateIdNameMappingKey(NameIdentifier nameIdentifier) throws IOException;
}
