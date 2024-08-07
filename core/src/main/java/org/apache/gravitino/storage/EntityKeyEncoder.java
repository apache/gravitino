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

package org.apache.gravitino.storage;

import java.io.IOException;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.gravitino.Entity.EntityType;
import org.apache.gravitino.NameIdentifier;

/** Interface for encoding entity to storage it in underlying storage. E.g., RocksDB. */
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
   * @param type type of the ident that represents
   * @param ident entity identifier to encode
   * @return encoded key for key-value stored
   * @throws IOException Exception if error occurs
   */
  T encode(NameIdentifier ident, EntityType type, boolean nullIfMissing) throws IOException;

  /**
   * Decode the key to NameIdentifier and EntityType.
   *
   * @param key the key to decode
   * @return the pair of NameIdentifier and EntityType
   * @throws IOException Exception if error occurs
   */
  default Pair<NameIdentifier, EntityType> decode(T key) throws IOException {
    throw new UnsupportedOperationException("Not implemented yet");
  }
}
