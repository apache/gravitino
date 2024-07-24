/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.gravitino.storage.kv;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.gravitino.Config;
import org.apache.gravitino.EntityAlreadyExistsException;

/** Interface defining the operations for a Key-Value (KV) Backend. */
public interface KvBackend extends Closeable {
  /**
   * Initializes the KV Backend environment with the provided configuration.
   *
   * @param config The configuration for the backend.
   * @throws IOException If an I/O exception occurs during initialization.
   */
  void initialize(Config config) throws IOException;

  /**
   * Stores a key-value pair, possibly overwriting an existing value if specified.
   *
   * @param key The key of the pair.
   * @param value The value of the pair.
   * @param overwrite If true, overwrites the existing value.
   * @throws IOException If an I/O exception occurs during the operation.
   * @throws EntityAlreadyExistsException If the key already exists and overwrite is false.
   */
  void put(byte[] key, byte[] value, boolean overwrite)
      throws IOException, EntityAlreadyExistsException;

  /**
   * Retrieves the value associated with a given key.
   *
   * @param key The key to retrieve the value for.
   * @return The value associated with the key, or null if the key does not exist.
   * @throws IOException If an I/O exception occurs during retrieval.
   */
  byte[] get(byte[] key) throws IOException;

  /**
   * Deletes the key-value pair associated with the given key.
   *
   * @param key The key to delete.
   * @return True, if the key-value pair was successfully deleted, else throw exception.
   * @throws IOException If an exception occurs during deletion.
   */
  boolean delete(byte[] key) throws IOException;

  /**
   * Delete the key-value pair associated with the given {@link KvRange}
   *
   * @param kvRange kv range to to delete
   * @return True, if the key-value pair was successfully deleted, else throw exception.
   * @throws IOException If an I/O exception occurs during deletion.
   */
  boolean deleteRange(KvRange kvRange) throws IOException;

  /**
   * Scans the specified range using the provided KvRangeScan and returns a list of key-value pairs.
   *
   * @param scanRange The range to scan.
   * @return A list of key-value pairs within the specified range.
   * @throws IOException If an I/O exception occurs during scanning.
   */
  List<Pair<byte[], byte[]>> scan(KvRange scanRange) throws IOException;
}
