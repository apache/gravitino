package com.datastrato.graviton;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EntityStoreFactory {

  private static final Logger LOG = LoggerFactory.getLogger(EntityStoreFactory.class);

  // Register EntityStore's short name to its full qualified class name in the map. So that user
  // don't need to specify the full qualified class name when creating an EntityStore.
  private static final Map<String, String> ENTITY_STORES = ImmutableMap.of();

  private EntityStoreFactory() {}

  public static EntityStore createEntityStore(Config config) {
    String name = config.get(configs.ENTITY_STORE);
    return createEntityStore(name);
  }

  public static EntityStore createEntityStore(String name) {
    String className = ENTITY_STORES.getOrDefault(name, name);

    try {
      return (EntityStore) Class.forName(className).newInstance();
    } catch (Exception e) {
      LOG.error("Failed to create EntityStore by name {}.", name, e);
      throw new RuntimeException("Failed to create EntityStore: " + name, e);
    }
  }
}
