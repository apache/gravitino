/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.graviton;

import java.io.IOException;
import java.util.Optional;

/**
 * EntitySerDeFacade is a facade of {@link EntitySerDe}. It is used to serialize and deserialize
 * entites in Graviton.
 *
 * <p>Underlying implementation of {@link EntitySerDeFacade} can be changed in the future, now the
 * default implementation is {@link com.datastrato.graviton.proto.ProtoEntitySerDe}
 */
public interface EntitySerDeFacade {

  /**
   * Set the underlying entity ser/de implementation. {@link EntitySerDe} will be used to serialize
   * and deserialize
   *
   * @param serDe The detailed implementation of {@link EntitySerDe}
   */
  void setEntitySeDe(EntitySerDe serDe);

  /**
   * Serializes the entity to a byte array. Compare to {@link EntitySerDe#serialize(Entity)}, this
   * method will add some extra message to the serialized byte array, such as the entity class name.
   * Other information can also be added in the future.
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
   * @param <T> The type of entity
   * @return the deserialized entity
   * @throws IOException if the deserialization fails
   */
  default <T extends Entity> T deserialize(byte[] bytes) throws IOException {
    ClassLoader loader =
        Optional.ofNullable(Thread.currentThread().getContextClassLoader())
            .orElse(getClass().getClassLoader());
    return deserialize(bytes, loader);
  }

  /**
   * Deserializes the entity from a byte array.
   *
   * @param bytes the byte array to deserialize
   * @param classLoader the class loader to use
   * @param <T> The type of entity
   * @return the deserialized entity
   * @throws IOException if the deserialization fails
   */
  <T extends Entity> T deserialize(byte[] bytes, ClassLoader classLoader) throws IOException;
}
