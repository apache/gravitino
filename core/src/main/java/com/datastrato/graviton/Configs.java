/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton;

import com.datastrato.graviton.config.ConfigBuilder;
import com.datastrato.graviton.config.ConfigEntry;

public interface Configs {
  String DEFUALT_ENTITY_STORE = "kv";
  String ENTITY_STORE_KEY = "graviton.entity.store";

  String DEFUALT_ENTITY_KV_STORE = "RocksDBKvBackend";
  String ENTITY_KV_STORE_KEY = "graviton.entity.store.kv";

  String ENTITY_KV_ROCKSDB_BACKEND_PATH_KEY = "graviton.entity.store.kv.rocskdb.path";
  String DEFAULT_KV_ROCKSDB_BACKEND_PATH = "/tmp/graviton";

  ConfigEntry<String> ENTITY_STORE =
      new ConfigBuilder(ENTITY_STORE_KEY)
          .doc("The entity store to use")
          .version("0.1.0")
          .stringConf()
          .createWithDefault(DEFUALT_ENTITY_STORE);

  ConfigEntry<String> ENTITY_KV_STORE =
      new ConfigBuilder(ENTITY_KV_STORE_KEY)
          .doc("The kv entity store to use")
          .version("0.1.0")
          .stringConf()
          .createWithDefault(DEFUALT_ENTITY_KV_STORE);

  ConfigEntry<String> ENTRY_KV_ROCKSDB_BACKEND_PATH =
      new ConfigBuilder(ENTITY_KV_ROCKSDB_BACKEND_PATH_KEY)
          .doc("The RocksDB backend path for entity store")
          .version("0.1.0")
          .stringConf()
          .createWithDefault(DEFAULT_KV_ROCKSDB_BACKEND_PATH);

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

  ConfigEntry<Boolean> CATALOG_LOAD_ISOLATED =
      new ConfigBuilder("graviton.catalog.classloader.isolated")
          .doc("Whether to load the catalog in an isolated classloader")
          .version("0.1.0")
          .booleanConf()
          .createWithDefault(true);
}
