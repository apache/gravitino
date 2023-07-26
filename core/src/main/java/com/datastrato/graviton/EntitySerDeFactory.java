/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton;

import com.datastrato.graviton.proto.ProtoEntitySerDe;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EntitySerDeFactory {

  private static final Logger LOG = LoggerFactory.getLogger(EntitySerDe.class);

  // Register EntitySerDe's short name to its full qualified class name in the map. So that user
  // don't need to specify the full qualified class name when creating an EntitySerDe.
  private static final Map<String, String> ENTITY_SERDES =
      ImmutableMap.of("proto", ProtoEntitySerDe.class.getCanonicalName());

  private EntitySerDeFactory() {}

  public static EntitySerDe createEntitySerDe(Config config) {
    String name = config.get(Configs.ENTITY_SERDE);
    return createEntitySerDe(name);
  }

  public static EntitySerDe createEntitySerDe(String name) {
    String className = ENTITY_SERDES.getOrDefault(name, name);

    try {
      return (EntitySerDe) Class.forName(className).newInstance();
    } catch (Exception e) {
      LOG.error("Failed to create EntitySerDe by name {}.", name, e);
      throw new RuntimeException("Failed to create EntitySerDe: " + name, e);
    }
  }
}
