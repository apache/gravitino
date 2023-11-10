/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino;

import com.datastrato.gravitino.config.ConfigBuilder;
import com.datastrato.gravitino.config.ConfigEntry;
import io.jsonwebtoken.SignatureAlgorithm;
import java.io.File;

public interface Configs {

  String DEFAULT_ENTITY_STORE = "kv";
  String ENTITY_STORE_KEY = "gravitino.entity.store";

  String DEFAULT_ENTITY_KV_STORE = "RocksDBKvBackend";
  String ENTITY_KV_STORE_KEY = "gravitino.entity.store.kv";

  String ENTITY_KV_ROCKSDB_BACKEND_PATH_KEY = "gravitino.entity.store.kv.rocksdbPath";

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

  ConfigEntry<String> SERVICE_AUDIENCE =
      new ConfigBuilder("gravitino.authenicator.oauth.service.audience")
          .doc("The audience name when Gravitino uses oauth as the authenticator")
          .version("0.3.0")
          .stringConf()
          .createWithDefault("");

  ConfigEntry<Long> ALLOW_SKEW_SECONDS =
      new ConfigBuilder("gravitino.authenticator.oauth.allow.skew.seconds")
          .doc("The jwt allows skew seconds when Gravitino uses oauth as the authenticator")
          .version("0.3.0")
          .longConf()
          .createWithDefault(0L);

  ConfigEntry<String> DEFAULT_SIGN_KEY =
      new ConfigBuilder("gravitino.authenticator.oauth.default.sign.key")
          .doc("The sign key of jwt when Gravitino uses oauth as the authenticator")
          .version("0.3.0")
          .stringConf()
          .createWithDefault(null);

  ConfigEntry<String> ALG_TYPE =
      new ConfigBuilder("gravitino.authenticator.oauth.algorithm.type")
          .doc("The encryption algorithm when Gravitino uses oauth as the authenticator")
          .version("0.3.0")
          .stringConf()
          .createWithDefault(SignatureAlgorithm.RS256.name());
}
