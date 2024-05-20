/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino;

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
