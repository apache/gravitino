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
package org.apache.gravitino.proto;

import com.google.protobuf.Message;
import org.apache.gravitino.Namespace;

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
