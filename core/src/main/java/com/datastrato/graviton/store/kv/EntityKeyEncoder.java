/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.graviton.store.kv;

import com.datastrato.graviton.NameIdentifier;
import com.datastrato.graviton.Namespace;

/**
 * Interface for encoding entity key for KV backend, e.g. RocksDB. The key is used to store the
 * entity in the backend.
 */
public interface EntityKeyEncoder {
  byte[] encode(NameIdentifier identifier);

  byte[] encode(Namespace namespace);
}
