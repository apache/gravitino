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

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.gravitino.proto.ProtoEntitySerDe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is responsible for creating instances of EntitySerDe implementations. EntitySerDe
 * (Entity Serialization/Deserialization) implementations are used to serialize and deserialize
 * entities within the Apache Gravitino framework.
 */
public class EntitySerDeFactory {

  private static final Logger LOG = LoggerFactory.getLogger(EntitySerDeFactory.class);

  // Register EntitySerDe's short name to its full qualified class name in the map. So that user
  // don't need to specify the full qualified class name when creating an EntitySerDe.
  private static final Map<String, String> ENTITY_SERDES =
      ImmutableMap.of("proto", ProtoEntitySerDe.class.getCanonicalName());

  private EntitySerDeFactory() {}

  /**
   * Creates an instance of EntitySerDe.
   *
   * @param config The configuration object containing settings for EntitySerDe.
   * @return An instance of EntitySerDe.
   */
  public static EntitySerDe createEntitySerDe(Config config) {
    String name = config.get(Configs.ENTITY_SERDE);
    return createEntitySerDe(name);
  }

  /**
   * Creates an instance of EntitySerDe.
   *
   * @param name The short name identifying the EntitySerDe implementation.
   * @return An instance of EntitySerDe.
   * @throws RuntimeException If the EntitySerDe creation fails.
   */
  public static EntitySerDe createEntitySerDe(String name) {
    String className = ENTITY_SERDES.getOrDefault(name, name);

    try {
      return (EntitySerDe) Class.forName(className).getDeclaredConstructor().newInstance();
    } catch (Exception e) {
      LOG.error("Failed to create EntitySerDe by name {}.", name, e);
      throw new RuntimeException("Failed to create EntitySerDe: " + name, e);
    }
  }
}
