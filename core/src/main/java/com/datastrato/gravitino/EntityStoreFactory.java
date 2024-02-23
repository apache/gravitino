/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino;

import com.datastrato.gravitino.storage.kv.KvEntityStore;
import com.datastrato.gravitino.storage.relational.RelationalEntityStore;
import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is responsible for creating instances of EntityStore implementations. EntityStore
 * implementations are used to store and manage entities within the Gravitino framework.
 */
public class EntityStoreFactory {

  private static final Logger LOG = LoggerFactory.getLogger(EntityStoreFactory.class);

  // Register EntityStore's short name to its full qualified class name in the map. So that user
  // doesn't need to specify the full qualified class name when creating an EntityStore.
  public static final ImmutableMap<String, String> ENTITY_STORES =
      ImmutableMap.of(
          Configs.DEFAULT_ENTITY_STORE,
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

    try {
      return (EntityStore) Class.forName(className).getDeclaredConstructor().newInstance();
    } catch (Exception e) {
      LOG.error("Failed to create and initialize EntityStore by name {}.", name, e);
      throw new RuntimeException("Failed to create and initialize EntityStore: " + name, e);
    }
  }
}
