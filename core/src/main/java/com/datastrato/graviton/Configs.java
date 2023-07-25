/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton;

import com.datastrato.graviton.config.ConfigBuilder;
import com.datastrato.graviton.config.ConfigEntry;

public interface Configs {
  String ENTRY_KV_BACKEND_KEY_NAME = "graviton.entity.backend.kv";
  String DEFUALT_ENTRY_KV_BACKEND = "rocksdb";

  String KV_BACKEND_PATH_KEY_NAME = "graviton.entity.backend.kv.path";
  String DEFAULT_KV_BACKEND_PATH = "/tmp/graviton";

  ConfigEntry<String> ENTITY_STORE =
      new ConfigBuilder("graviton.entity.store")
          .doc("The entity store to use")
          .version("0.1.0")
          .stringConf()
          // TODO. Change this when we have a EntityStore implementation. @Jerry
          .createWithDefault("in-memory");

  ConfigEntry<String> ENTITY_SERDE =
      new ConfigBuilder("graviton.entity.serde")
          .doc("The entity serde to use")
          .version("0.1.0")
          .stringConf()
          .createWithDefault("proto");

  ConfigEntry<Long> CATALOG_CACHE_EVICTION_INTERVAL_MS =
      new ConfigBuilder("graviton.catalog.cache.evictionIntervalMs")
          .doc("The interval in milliseconds to evict the catalog cache")
          .version("0.1.0")
          .longConf()
          .createWithDefault(60 * 60 * 1000L);

  ConfigEntry<String> ENTRY_KV_BACKEND_TYPE =
      new ConfigBuilder(ENTRY_KV_BACKEND_KEY_NAME)
          .doc("KV backend to use for entity store")
          .version("0.1.0")
          .stringConf()
          .createWithDefault("rocksdb");

  ConfigEntry<String> ENTRY_KV_BACKEND_PATH =
      new ConfigBuilder(ENTRY_KV_BACKEND_KEY_NAME)
          .doc("KV backend path for entity store")
          .version("0.1.0")
          .stringConf()
          .createWithDefault("/tmp/graviton");
}
