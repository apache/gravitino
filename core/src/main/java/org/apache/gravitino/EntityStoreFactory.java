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

import static org.apache.gravitino.Configs.KV_STORE_KEY;

import com.google.common.collect.ImmutableMap;
import org.apache.gravitino.storage.kv.KvEntityStore;
import org.apache.gravitino.storage.relational.RelationalEntityStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is responsible for creating instances of EntityStore implementations. EntityStore
 * implementations are used to store and manage entities within the Apache Gravitino framework.
 */
public class EntityStoreFactory {

  private static final Logger LOG = LoggerFactory.getLogger(EntityStoreFactory.class);

  // Register EntityStore's short name to its full qualified class name in the map. So that user
  // doesn't need to specify the full qualified class name when creating an EntityStore.
  public static final ImmutableMap<String, String> ENTITY_STORES =
      ImmutableMap.of(
          KV_STORE_KEY,
          KvEntityStore.class.getCanonicalName(),
          Configs.RELATIONAL_ENTITY_STORE,
          RelationalEntityStore.class.getCanonicalName());

  // Private constructor to prevent instantiation of this factory class.
  private EntityStoreFactory() {}

  /**
   * Creates an instance of EntityStore based on the configuration settings.
   *
   * @param config The configuration object containing settings for EntityStore.
   * @return An instance of EntityStore.
   */
  public static EntityStore createEntityStore(Config config) {
    String name = config.get(Configs.ENTITY_STORE);
    String className = ENTITY_STORES.getOrDefault(name, name);

    if (KV_STORE_KEY.equals(name)) {
      throw new UnsupportedOperationException(
          "KvEntityStore is not supported since version 0.6.0. Please use RelationalEntityStore instead.");
    }

    try {
      return (EntityStore) Class.forName(className).getDeclaredConstructor().newInstance();
    } catch (Exception e) {
      LOG.error("Failed to create and initialize EntityStore by name {}.", name, e);
      throw new RuntimeException("Failed to create and initialize EntityStore: " + name, e);
    }
  }
}
