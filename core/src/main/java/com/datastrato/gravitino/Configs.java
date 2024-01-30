/*
 * Copyright 2023 Datastrato Pvt Ltd.
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

  Long DEFAULT_KV_DELETE_AFTER_TIME = 604800000L; // 7 days
  String KV_DELETE_AFTER_TIME_KEY = "gravitino.entity.store.kv.deleteAfterTimeMs";

  // Default path for RocksDB backend is "${GRAVITINO_HOME}/data/rocksdb"
  String DEFAULT_KV_ROCKSDB_BACKEND_PATH =
      String.join(File.separator, System.getenv("GRAVITINO_HOME"), "data", "rocksdb");

  ConfigEntry<String> ENTITY_STORE =
      new ConfigBuilder(ENTITY_STORE_KEY)
          .doc("Which storage implementation to use")
          .version("0.1.0")
          .stringConf()
          .createWithDefault(DEFAULT_ENTITY_STORE);

  ConfigEntry<String> ENTITY_KV_STORE =
      new ConfigBuilder(ENTITY_KV_STORE_KEY)
          .doc("Detailed implementation of Kv storage")
          .version("0.1.0")
          .stringConf()
          .createWithDefault(DEFAULT_ENTITY_KV_STORE);

  ConfigEntry<String> ENTRY_KV_ROCKSDB_BACKEND_PATH =
      new ConfigBuilder(ENTITY_KV_ROCKSDB_BACKEND_PATH_KEY)
          .doc(
              "The storage path for RocksDB storage implementation. It supports both absolute and relative path, if the value is a relative path, the final path is `${GRAVITINO_HOME}/${PATH_YOU_HAVA_SET}`, default value is `${GRAVITINO_HOME}/data/rocksdb`")
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
          .doc("The maximum skew time of transactions in milliseconds")
          .version("0.3.0")
          .longConf()
          .createWithDefault(2000L);

  ConfigEntry<Long> KV_DELETE_AFTER_TIME =
      new ConfigBuilder(KV_DELETE_AFTER_TIME_KEY)
          .doc(
              "The maximum time in milliseconds that the deleted data and old version data is kept")
          .version("0.3.0")
          .longConf()
          .createWithDefault(DEFAULT_KV_DELETE_AFTER_TIME);
}
