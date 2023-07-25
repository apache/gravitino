/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.storage;

import com.datastrato.graviton.Config;
import com.datastrato.graviton.Configs;
import com.datastrato.graviton.EntityStore;
import com.datastrato.graviton.storage.kv.KvEntityStore;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EntityStoreFactory {

  private static final Logger LOG = LoggerFactory.getLogger(EntityStore.class);

  // Register EntityStore's short name to its full qualified class name in the map. So that user
  // don't need to specify the full qualified class name when creating an EntityStore.
  private static final Map<String, String> ENTITY_STORES =
      new HashMap<String, String>() {
        {
          put("kv", KvEntityStore.class.getCanonicalName());
        }
      };

  private EntityStoreFactory() {}

  public static EntityStore createEntityStore(Config config) {
    String name = config.get(Configs.ENTITY_STORE);
    String className = ENTITY_STORES.getOrDefault(name, name);

    try {
      EntityStore store = (EntityStore) Class.forName(className).newInstance();
      store.initialize(config);
      return store;
    } catch (Exception e) {
      LOG.error("Failed to create and initialize EntityStore by name {}.", name, e);
      throw new RuntimeException("Failed to create and initialize EntityStore: " + name, e);
    }
  }
}
