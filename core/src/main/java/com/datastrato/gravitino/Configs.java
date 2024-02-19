/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino;

import com.datastrato.gravitino.config.ConfigBuilder;
import com.datastrato.gravitino.config.ConfigConstants;
import com.datastrato.gravitino.config.ConfigEntry;
import java.io.File;
import org.apache.commons.lang3.StringUtils;

public interface Configs {

  String DEFAULT_ENTITY_STORE = "kv";
  String RELATIONAL_ENTITY_STORE = "relational";
  String ENTITY_STORE_KEY = "gravitino.entity.store";

  String DEFAULT_ENTITY_KV_STORE = "RocksDBKvBackend";
  String ENTITY_KV_STORE_KEY = "gravitino.entity.store.kv";

  String DEFAULT_ENTITY_RELATIONAL_STORE = "JDBCBackend";
  String ENTITY_RELATIONAL_STORE_KEY = "gravitino.entity.store.relational";

  String ENTITY_RELATIONAL_JDBC_BACKEND_URL_KEY = "gravitino.entity.store.relational.jdbcUrl";
  String ENTITY_RELATIONAL_JDBC_BACKEND_DRIVER_KEY = "gravitino.entity.store.relational.jdbcDriver";
  String ENTITY_RELATIONAL_JDBC_BACKEND_USER_KEY = "gravitino.entity.store.relational.jdbcUser";
  String ENTITY_RELATIONAL_JDBC_BACKEND_PASSWORD_KEY =
      "gravitino.entity.store.relational.jdbcPassword";

  String ENTITY_KV_ROCKSDB_BACKEND_PATH_KEY = "gravitino.entity.store.kv.rocksdbPath";

  Long DEFAULT_KV_DELETE_AFTER_TIME = 604800000L; // 7 days
  String KV_DELETE_AFTER_TIME_KEY = "gravitino.entity.store.kv.deleteAfterTimeMs";

  // Default path for RocksDB backend is "${GRAVITINO_HOME}/data/rocksdb"
  String DEFAULT_KV_ROCKSDB_BACKEND_PATH =
      String.join(File.separator, System.getenv("GRAVITINO_HOME"), "data", "rocksdb");

  long MAX_NODE_IN_MEMORY = 100000L;

  long MIN_NODE_IN_MEMORY = 1000L;

  long CLEAN_INTERVAL_IN_SECS = 60L;

  ConfigEntry<String> ENTITY_STORE =
      new ConfigBuilder(ENTITY_STORE_KEY)
          .doc("Which storage implementation to use")
          .version(ConfigConstants.VERSION_0_1_0)
          .stringConf()
          .createWithDefault(DEFAULT_ENTITY_STORE);

  ConfigEntry<String> ENTITY_KV_STORE =
      new ConfigBuilder(ENTITY_KV_STORE_KEY)
          .doc("Detailed implementation of Kv storage")
          .version(ConfigConstants.VERSION_0_1_0)
          .stringConf()
          .createWithDefault(DEFAULT_ENTITY_KV_STORE);

  ConfigEntry<String> ENTITY_RELATIONAL_STORE =
      new ConfigBuilder(ENTITY_RELATIONAL_STORE_KEY)
          .doc("Detailed implementation of relational storage")
          .version(ConfigConstants.VERSION_0_5_0)
          .stringConf()
          .checkValue(StringUtils::isNotBlank, ConfigConstants.NOT_BLANK_ERROR_MSG)
          .createWithDefault(DEFAULT_ENTITY_RELATIONAL_STORE);

  ConfigEntry<String> ENTITY_RELATIONAL_JDBC_BACKEND_URL =
      new ConfigBuilder(ENTITY_RELATIONAL_JDBC_BACKEND_URL_KEY)
          .doc("Connection URL of `JDBCBackend`")
          .version(ConfigConstants.VERSION_0_5_0)
          .stringConf()
          .checkValue(StringUtils::isNotBlank, ConfigConstants.NOT_BLANK_ERROR_MSG)
          .create();

  ConfigEntry<String> ENTITY_RELATIONAL_JDBC_BACKEND_DRIVER =
      new ConfigBuilder(ENTITY_RELATIONAL_JDBC_BACKEND_DRIVER_KEY)
          .doc("Driver Name of `JDBCBackend`")
          .version(ConfigConstants.VERSION_0_5_0)
          .stringConf()
          .checkValue(StringUtils::isNotBlank, ConfigConstants.NOT_BLANK_ERROR_MSG)
          .create();

  ConfigEntry<String> ENTITY_RELATIONAL_JDBC_BACKEND_USER =
      new ConfigBuilder(ENTITY_RELATIONAL_JDBC_BACKEND_USER_KEY)
          .doc("Username of `JDBCBackend`")
          .version(ConfigConstants.VERSION_0_5_0)
          .stringConf()
          .checkValue(StringUtils::isNotBlank, ConfigConstants.NOT_BLANK_ERROR_MSG)
          .create();

  ConfigEntry<String> ENTITY_RELATIONAL_JDBC_BACKEND_PASSWORD =
      new ConfigBuilder(ENTITY_RELATIONAL_JDBC_BACKEND_PASSWORD_KEY)
          .doc("Password of `JDBCBackend`")
          .version(ConfigConstants.VERSION_0_5_0)
          .stringConf()
          .checkValue(StringUtils::isNotBlank, ConfigConstants.NOT_BLANK_ERROR_MSG)
          .create();

  ConfigEntry<String> ENTRY_KV_ROCKSDB_BACKEND_PATH =
      new ConfigBuilder(ENTITY_KV_ROCKSDB_BACKEND_PATH_KEY)
          .doc(
              "The storage path for RocksDB storage implementation. It supports both absolute and"
                  + " relative path, if the value is a relative path, the final path is "
                  + "`${GRAVITINO_HOME}/${PATH_YOU_HAVA_SET}`, default value is "
                  + "`${GRAVITINO_HOME}/data/rocksdb`")
          .version(ConfigConstants.VERSION_0_1_0)
          .stringConf()
          .createWithDefault(DEFAULT_KV_ROCKSDB_BACKEND_PATH);

  ConfigEntry<String> ENTITY_SERDE =
      new ConfigBuilder("gravitino.entity.serde")
          .doc("The entity SerDe to use")
          .version(ConfigConstants.VERSION_0_1_0)
          .stringConf()
          .createWithDefault("proto");

  ConfigEntry<Long> CATALOG_CACHE_EVICTION_INTERVAL_MS =
      new ConfigBuilder("gravitino.catalog.cache.evictionIntervalMs")
          .doc("The interval in milliseconds to evict the catalog cache")
          .version(ConfigConstants.VERSION_0_1_0)
          .longConf()
          .createWithDefault(60 * 60 * 1000L);

  ConfigEntry<Boolean> CATALOG_LOAD_ISOLATED =
      new ConfigBuilder("gravitino.catalog.classloader.isolated")
          .doc("Whether to load the catalog in an isolated classloader")
          .version(ConfigConstants.VERSION_0_1_0)
          .booleanConf()
          .createWithDefault(true);

  ConfigEntry<String> AUTHENTICATOR =
      new ConfigBuilder("gravitino.authenticator")
          .doc("The authenticator which Gravitino uses")
          .version(ConfigConstants.VERSION_0_3_0)
          .stringConf()
          .createWithDefault("simple");

  ConfigEntry<Long> STORE_TRANSACTION_MAX_SKEW_TIME =
      new ConfigBuilder("gravitino.entity.store.maxTransactionSkewTimeMs")
          .doc("The maximum skew time of transactions in milliseconds")
          .version(ConfigConstants.VERSION_0_3_0)
          .longConf()
          .createWithDefault(2000L);

  ConfigEntry<Long> KV_DELETE_AFTER_TIME =
      new ConfigBuilder(KV_DELETE_AFTER_TIME_KEY)
          .doc(
              "The maximum time in milliseconds that the deleted data and old version data is kept")
          .version(ConfigConstants.VERSION_0_3_0)
          .longConf()
          .createWithDefault(DEFAULT_KV_DELETE_AFTER_TIME);

  // The followings are configurations for tree lock

  ConfigEntry<Long> TREE_LOCK_MAX_NODE_IN_MEMORY =
      new ConfigBuilder("gravitino.lock.maxNodes")
          .doc("The maximum number of tree lock nodes to keep in memory")
          .version(ConfigConstants.VERSION_0_4_0)
          .longConf()
          .createWithDefault(MAX_NODE_IN_MEMORY);

  ConfigEntry<Long> TREE_LOCK_MIN_NODE_IN_MEMORY =
      new ConfigBuilder("gravitino.lock.minNodes")
          .doc("The minimum number of tree lock nodes to keep in memory")
          .version(ConfigConstants.VERSION_0_4_0)
          .longConf()
          .createWithDefault(MIN_NODE_IN_MEMORY);

  ConfigEntry<Long> TREE_LOCK_CLEAN_INTERVAL =
      new ConfigBuilder("gravitino.lock.cleanIntervalInSecs")
          .doc("The interval in seconds to clean up the stale tree lock nodes")
          .version(ConfigConstants.VERSION_0_4_0)
          .longConf()
          .createWithDefault(CLEAN_INTERVAL_IN_SECS);
}
