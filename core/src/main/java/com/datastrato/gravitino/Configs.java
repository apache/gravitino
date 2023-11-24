/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino;

import com.datastrato.gravitino.config.ConfigBuilder;
import com.datastrato.gravitino.config.ConfigEntry;
import java.io.File;

public interface Configs {

  String DEFAULT_ENTITY_STORE = "kv";
  String ENTITY_STORE_KEY = "gravitino.entity.store";

  String DEFAULT_ENTITY_KV_STORE = "RocksDBKvBackend";
  String ENTITY_KV_STORE_KEY = "gravitino.entity.store.kv";

  String ENTITY_KV_ROCKSDB_BACKEND_PATH_KEY = "gravitino.entity.store.kv.rocksdbPath";

  Long DEFAULT_ENTITY_KV_TTL = 7L; // 7 days
  String ENTITY_KV_TTL_KEY_KEY = "graviton.entity.store.kv.ttl";

  // Default path for RocksDB backend is "${GRAVITINO_HOME}/data/rocksdb"
  String DEFAULT_KV_ROCKSDB_BACKEND_PATH =
      String.join(File.separator, System.getenv("GRAVITINO_HOME"), "data", "rocksdb");

  ConfigEntry<String> ENTITY_STORE =
      new ConfigBuilder(ENTITY_STORE_KEY)
          .doc("The entity store to use")
          .version("0.1.0")
          .stringConf()
          .createWithDefault(DEFAULT_ENTITY_STORE);

  ConfigEntry<String> ENTITY_KV_STORE =
      new ConfigBuilder(ENTITY_KV_STORE_KEY)
          .doc("The kv entity store to use")
          .version("0.1.0")
          .stringConf()
          .createWithDefault(DEFAULT_ENTITY_KV_STORE);

  ConfigEntry<String> ENTRY_KV_ROCKSDB_BACKEND_PATH =
      new ConfigBuilder(ENTITY_KV_ROCKSDB_BACKEND_PATH_KEY)
          .doc("The RocksDB backend path for entity store")
          .version("0.1.0")
          .stringConf()
          .createWithDefault(DEFAULT_KV_ROCKSDB_BACKEND_PATH);

  ConfigEntry<String> ENTITY_SERDE =
      new ConfigBuilder("gravitino.entity.serde")
          .doc("The entity SerDe to use")
          .version("0.1.0")
          .stringConf()
          .createWithDefault("proto");

  ConfigEntry<Long> CATALOG_CACHE_EVICTION_INTERVAL_MS =
      new ConfigBuilder("gravitino.catalog.cache.evictionIntervalMs")
          .doc("The interval in milliseconds to evict the catalog cache")
          .version("0.1.0")
          .longConf()
          .createWithDefault(60 * 60 * 1000L);

  ConfigEntry<Boolean> CATALOG_LOAD_ISOLATED =
      new ConfigBuilder("gravitino.catalog.classloader.isolated")
          .doc("Whether to load the catalog in an isolated classloader")
          .version("0.1.0")
          .booleanConf()
          .createWithDefault(true);

  ConfigEntry<String> AUTHENTICATOR =
      new ConfigBuilder("gravitino.authenticator")
          .doc("The authenticator which Gravitino uses")
          .version("0.3.0")
          .stringConf()
          .createWithDefault("simple");

  ConfigEntry<Long> STORE_TRANSACTION_MAX_SKEW_TIME =
      new ConfigBuilder("gravitino.entity.store.maxTransactionSkewTimeMs")
          .doc("Max time skew allowed for transaction, Unit: millisecond")
          .version("0.3.0")
          .longConf()
          .createWithDefault(2000L);

  ConfigEntry<Long> ENTITY_KV_TTL =
      new ConfigBuilder(ENTITY_KV_TTL_KEY_KEY)
          .doc("The TTL of old version kv entity store, unit: days")
          .version("0.3.0")
          .longConf()
          .createWithDefault(DEFAULT_ENTITY_KV_TTL);
}
