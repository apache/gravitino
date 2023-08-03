/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.graviton.storage.kv;

import com.datastrato.graviton.Entity.EntityType;
import com.datastrato.graviton.NameIdentifier;
import com.datastrato.graviton.Namespace;
import java.io.IOException;

/**
 * Interface for encoding entity key for KV backend, e.g., RocksDB. The key is used to store the
 * entity in the backend.
 */
public interface EntityKeyEncoder {

  /**
   * Construct the key for key-value store from the entity NameIdentifier and EntityType.
   *
   * @param identifier entity identifier
   * @param type entity type, e.g., metalake, catalog, schema, table, topic
   * @return encoded key for key-value store
   * @throws IOException
   */
  byte[] encode(NameIdentifier identifier, EntityType type) throws IOException;

  /**
   * Encode entity key, This is for range scan. For example, if we want to scan all the metalakes.
   * Then the value of type is EntityType.METALAKE and namespace are empty. If we want to scan all
   * catalogs in a metalake, then the value of type is EntityType.CATALOG and namespace are metalake
   * name.
   *
   * @param namespace namespace of current entity
   * @param type type of entity under the current namespace to scan, if the namespace is catalog and
   *     type is schema, we will scan all schemas under the catalog
   */
  byte[] encode(Namespace namespace, EntityType type) throws IOException;
}
