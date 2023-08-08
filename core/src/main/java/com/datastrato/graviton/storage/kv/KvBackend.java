/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.graviton.storage.kv;

import com.datastrato.graviton.Config;
import com.datastrato.graviton.EntityAlreadyExistsException;
import com.datastrato.graviton.util.Executable;
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
   * Store a key value pair ignoring any existing value if overwrite is true. Once the value is
   * false, it will throw {@link EntityAlreadyExistsException} if the key already exists
   *
   * @param key key of the pair
   * @param value value of the pair
   * @param overwrite if true, overwrite existing value
   */
  void put(byte[] key, byte[] value, boolean overwrite)
      throws IOException, EntityAlreadyExistsException;

  /** Get a value pair for a key, Null if the key does not exist */
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

  /**
   * Do a transactional operation on the backend
   *
   * @param executable
   * @throws IOException
   */
  <R, E extends Exception> R executeInTransaction(Executable<R, E> executable)
      throws E, IOException;
}
