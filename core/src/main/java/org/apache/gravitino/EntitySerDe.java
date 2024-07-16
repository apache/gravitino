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
package org.apache.gravitino;

import java.io.IOException;
import java.util.Optional;

public interface EntitySerDe {

  /**
   * Serializes the entity to a byte array.
   *
   * @param t the entity to serialize
   * @return the serialized byte array of the entity
   * @param <T> The type of entity
   * @throws IOException if the serialization fails
   */
  <T extends Entity> byte[] serialize(T t) throws IOException;

  /**
   * Deserializes the entity from a byte array.
   *
   * @param bytes the byte array to deserialize
   * @param clazz the class of the entity
   * @param namespace the namespace to use when deserializing the entity
   * @return the deserialized entity
   * @param <T> The type of entity
   * @throws IOException if the deserialization fails
   */
  default <T extends Entity> T deserialize(byte[] bytes, Class<T> clazz, Namespace namespace)
      throws IOException {
    ClassLoader loader =
        Optional.ofNullable(Thread.currentThread().getContextClassLoader())
            .orElse(getClass().getClassLoader());
    return deserialize(bytes, clazz, loader, namespace);
  }

  /**
   * Deserializes the entity from a byte array.
   *
   * @param bytes the byte array to deserialize
   * @param clazz the class of the entity
   * @param classLoader the class loader to use
   * @param namespace the namespace to use when deserializing the entity
   * @return the deserialized entity
   * @param <T> The type of entity
   * @throws IOException if the deserialization fails
   */
  <T extends Entity> T deserialize(
      byte[] bytes, Class<T> clazz, ClassLoader classLoader, Namespace namespace)
      throws IOException;
}
