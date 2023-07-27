/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.graviton.storage.kv;

import com.datastrato.graviton.Config;
import com.datastrato.graviton.EntityAlreadyExistsException;
import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import org.apache.commons.lang3.tuple.Pair;

public interface KvBackend extends Closeable {
  /**
   * Init KvBackend environment
   *
   * @param config configuration for the backend
   */
  void initialize(Config config) throws IOException;

  /**
   * Store key value pair ignoring any existing value
   *
   * @param key key of the pair
   * @param value value of the pair
   * @param overwrite if true, overwrite existing value
   */
  void put(byte[] key, byte[] value, boolean overwrite)
      throws IOException, EntityAlreadyExistsException;

  /** Get value pair for key, Null if the key does not exist */
  byte[] get(byte[] key) throws IOException;

  /** Delete key value pair */
  default boolean delete(byte[] key) throws IOException {
    return false;
  }

  /**
   * Scan the range represented by {@link KvRangeScan} and return the list of key value pairs
   *
   * @param scanRange range to scan
   * @return List of key value pairs
   * @throws IOException if exectiopn occurs
   */
  List<Pair<byte[], byte[]>> scan(KvRangeScan scanRange) throws IOException;
}
