/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.proto;

import com.datastrato.gravitino.Namespace;
import com.google.protobuf.Message;

/**
 * This interface defines the contract for a Protocol Buffer Serializer and Deserializer (SerDe).
 *
 * @param <T> The entity type to be serialized and deserialized.
 * @param <M> The Protocol Buffer message type representing the entity.
 */
public interface ProtoSerDe<T, M extends Message> {

  /**
   * Serializes the provided entity into its corresponding Protocol Buffer message representation.
   *
   * @param t The entity to be serialized.
   * @return The Protocol Buffer message representing the serialized entity.
   */
  M serialize(T t);

  /**
   * Deserializes the provided Protocol Buffer message into its corresponding entity representation.
   *
   * @param p The Protocol Buffer message to be deserialized.
   * @param namespace The namespace to be specified for entity deserialization.
   * @return The entity representing the deserialized Protocol Buffer message.
   */
  T deserialize(M p, Namespace namespace);
}
