/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino;

import com.google.common.collect.Lists;
import java.io.File;
import java.util.Collections;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.config.ConfigBuilder;
import org.apache.gravitino.config.ConfigConstants;
import org.apache.gravitino.config.ConfigEntry;

public class Configs {

  private Configs() {}

  public static final String KV_STORE_KEY = "kv";
  public static final String RELATIONAL_ENTITY_STORE = "relational";
  public static final String ENTITY_STORE_KEY = "gravitino.entity.store";

  public static final String DEFAULT_ENTITY_KV_STORE = "RocksDBKvBackend";
  public static final String ENTITY_KV_STORE_KEY = "gravitino.entity.store.kv";

  public static final String DEFAULT_ENTITY_RELATIONAL_STORE = "JDBCBackend";
  public static final String ENTITY_RELATIONAL_STORE_KEY = "gravitino.entity.store.relational";

  public static final String ENTITY_RELATIONAL_JDBC_BACKEND_URL_KEY =
      "gravitino.entity.store.relational.jdbcUrl";
  public static final String ENTITY_RELATIONAL_JDBC_BACKEND_DRIVER_KEY =
      "gravitino.entity.store.relational.jdbcDriver";
  public static final String ENTITY_RELATIONAL_JDBC_BACKEND_USER_KEY =
      "gravitino.entity.store.relational.jdbcUser";
  public static final String ENTITY_RELATIONAL_JDBC_BACKEND_PASSWORD_KEY =
      "gravitino.entity.store.relational.jdbcPassword";

  public static final String ENTITY_RELATIONAL_JDBC_BACKEND_STORAGE_PATH_KEY =
      "gravitino.entity.store.relational.storagePath";

  public static final String ENTITY_KV_ROCKSDB_BACKEND_PATH_KEY =
      "gravitino.entity.store.kv.rocksdbPath";

  public static final Long DEFAULT_KV_DELETE_AFTER_TIME = 604800000L; // 7 days
  public static final String KV_DELETE_AFTER_TIME_KEY =
      "gravitino.entity.store.kv.deleteAfterTimeMs";

  // Config for data keep time after soft deletion, in milliseconds.
  public static final String STORE_DELETE_AFTER_TIME_KEY =
      "gravitino.entity.store.deleteAfterTimeMs";
  // using the fallback default value
  public static final Long DEFAULT_STORE_DELETE_AFTER_TIME = DEFAULT_KV_DELETE_AFTER_TIME;
  // The maximum allowed keep time for data deletion, in milliseconds. Equivalent to 30 days.
  public static final Long MAX_DELETE_TIME_ALLOW = 1000 * 60 * 60 * 24 * 30L;
  // The minimum allowed keep time for data deletion, in milliseconds. Equivalent to 10 minutes.
  public static final Long MIN_DELETE_TIME_ALLOW = 1000 * 60 * 10L;

  // Count of versions allowed to be retained, including the current version, used to delete old
  // versions data.
  public static final String VERSION_RETENTION_COUNT_KEY =
      "gravitino.entity.store.versionRetentionCount";
  public static final Long DEFAULT_VERSION_RETENTION_COUNT = 1L;
  // The maximum allowed count of versions to be retained
  public static final Long MAX_VERSION_RETENTION_COUNT = 10L;
  // The minimum allowed count of versions to be retained
  public static final Long MIN_VERSION_RETENTION_COUNT = 1L;

  // Default path for RocksDB backend is "${GRAVITINO_HOME}/data/rocksdb"
  public static final String DEFAULT_KV_ROCKSDB_BACKEND_PATH =
      String.join(File.separator, System.getenv("GRAVITINO_HOME"), "data", "rocksdb");

  public static final String DEFAULT_RELATIONAL_JDBC_BACKEND_PATH =
      String.join(File.separator, System.getenv("GRAVITINO_HOME"), "data", "jdbc");

  public static final String DEFAULT_RELATIONAL_JDBC_BACKEND_URL = "jdbc:h2";

  public static final String DEFAULT_RELATIONAL_JDBC_BACKEND_DRIVER = "org.h2.Driver";

  public static final String DEFAULT_RELATIONAL_JDBC_BACKEND_USERNAME = "gravitino";

  public static final String DEFAULT_RELATIONAL_JDBC_BACKEND_PASSWORD = "gravitino";

  public static final int GARBAGE_COLLECTOR_SINGLE_DELETION_LIMIT = 100;
  public static final long MAX_NODE_IN_MEMORY = 100000L;

  public static final long MIN_NODE_IN_MEMORY = 1000L;

  public static final long CLEAN_INTERVAL_IN_SECS = 60L;

  public static final ConfigEntry<String> ENTITY_STORE =
      new ConfigBuilder(ENTITY_STORE_KEY)
          .doc("Which storage implementation to use")
          .version(ConfigConstants.VERSION_0_1_0)
          .stringConf()
          .createWithDefault(RELATIONAL_ENTITY_STORE);

  public static final ConfigEntry<String> ENTITY_KV_STORE =
      new ConfigBuilder(ENTITY_KV_STORE_KEY)
          .doc("Detailed implementation of Kv storage")
          .version(ConfigConstants.VERSION_0_1_0)
          .stringConf()
          .createWithDefault(DEFAULT_ENTITY_KV_STORE);

  public static final ConfigEntry<String> ENTITY_RELATIONAL_STORE =
      new ConfigBuilder(ENTITY_RELATIONAL_STORE_KEY)
          .doc("Detailed implementation of relational storage")
          .version(ConfigConstants.VERSION_0_5_0)
          .stringConf()
          .checkValue(StringUtils::isNotBlank, ConfigConstants.NOT_BLANK_ERROR_MSG)
          .createWithDefault(DEFAULT_ENTITY_RELATIONAL_STORE);

  public static final ConfigEntry<String> ENTITY_RELATIONAL_JDBC_BACKEND_URL =
      new ConfigBuilder(ENTITY_RELATIONAL_JDBC_BACKEND_URL_KEY)
          .doc("Connection URL of `JDBCBackend`")
          .version(ConfigConstants.VERSION_0_5_0)
          .stringConf()
          .checkValue(StringUtils::isNotBlank, ConfigConstants.NOT_BLANK_ERROR_MSG)
          .createWithDefault(DEFAULT_RELATIONAL_JDBC_BACKEND_URL);

  public static final ConfigEntry<String> ENTITY_RELATIONAL_JDBC_BACKEND_DRIVER =
      new ConfigBuilder(ENTITY_RELATIONAL_JDBC_BACKEND_DRIVER_KEY)
          .doc("Driver Name of `JDBCBackend`")
          .version(ConfigConstants.VERSION_0_5_0)
          .stringConf()
          .checkValue(StringUtils::isNotBlank, ConfigConstants.NOT_BLANK_ERROR_MSG)
          .createWithDefault(DEFAULT_RELATIONAL_JDBC_BACKEND_DRIVER);

  public static final ConfigEntry<String> ENTITY_RELATIONAL_JDBC_BACKEND_USER =
      new ConfigBuilder(ENTITY_RELATIONAL_JDBC_BACKEND_USER_KEY)
          .doc("Username of `JDBCBackend`")
          .version(ConfigConstants.VERSION_0_5_0)
          .stringConf()
          .checkValue(StringUtils::isNotBlank, ConfigConstants.NOT_BLANK_ERROR_MSG)
          .createWithDefault(DEFAULT_RELATIONAL_JDBC_BACKEND_USERNAME);

  public static final ConfigEntry<String> ENTITY_RELATIONAL_JDBC_BACKEND_PASSWORD =
      new ConfigBuilder(ENTITY_RELATIONAL_JDBC_BACKEND_PASSWORD_KEY)
          .doc("Password of `JDBCBackend`")
          .version(ConfigConstants.VERSION_0_5_0)
          .stringConf()
          .createWithDefault(DEFAULT_RELATIONAL_JDBC_BACKEND_PASSWORD);

  public static final ConfigEntry<String> ENTITY_RELATIONAL_JDBC_BACKEND_PATH =
      new ConfigBuilder(ENTITY_RELATIONAL_JDBC_BACKEND_STORAGE_PATH_KEY)
          .doc(
              "The storage path for JDBC storage implementation. It supports both absolute and"
                  + " relative path, if the value is a relative path, the final path is "
                  + "`${GRAVITINO_HOME}/${PATH_YOU_HAVA_SET}`, default value is "
                  + "`${GRAVITINO_HOME}/data/jdbc`")
          .version(ConfigConstants.VERSION_0_6_0)
          .stringConf()
          .createWithDefault(DEFAULT_RELATIONAL_JDBC_BACKEND_PATH);

  public static final ConfigEntry<String> ENTITY_KV_ROCKSDB_BACKEND_PATH =
      new ConfigBuilder(ENTITY_KV_ROCKSDB_BACKEND_PATH_KEY)
          .doc(
              "The storage path for RocksDB storage implementation. It supports both absolute and"
                  + " relative path, if the value is a relative path, the final path is "
                  + "`${GRAVITINO_HOME}/${PATH_YOU_HAVA_SET}`, default value is "
                  + "`${GRAVITINO_HOME}/data/rocksdb`")
          .version(ConfigConstants.VERSION_0_1_0)
          .stringConf()
          .createWithDefault(DEFAULT_KV_ROCKSDB_BACKEND_PATH);

  public static final ConfigEntry<String> ENTITY_SERDE =
      new ConfigBuilder("gravitino.entity.serde")
          .doc("The entity SerDe to use")
          .version(ConfigConstants.VERSION_0_1_0)
          .stringConf()
          .createWithDefault("proto");

  public static final ConfigEntry<Long> CATALOG_CACHE_EVICTION_INTERVAL_MS =
      new ConfigBuilder("gravitino.catalog.cache.evictionIntervalMs")
          .doc("The interval in milliseconds to evict the catalog cache")
          .version(ConfigConstants.VERSION_0_1_0)
          .longConf()
          .createWithDefault(60 * 60 * 1000L);

  public static final ConfigEntry<Boolean> CATALOG_LOAD_ISOLATED =
      new ConfigBuilder("gravitino.catalog.classloader.isolated")
          .doc("Whether to load the catalog in an isolated classloader")
          .version(ConfigConstants.VERSION_0_1_0)
          .booleanConf()
          .createWithDefault(true);

  public static final ConfigEntry<String> AUTHENTICATOR =
      new ConfigBuilder("gravitino.authenticator")
          .doc(
              "The authenticator which Gravitino uses. Multiple authenticators "
                  + "separated by commas")
          .version(ConfigConstants.VERSION_0_3_0)
          .deprecated()
          .stringConf()
          .createWithDefault("simple");

  public static final ConfigEntry<List<String>> AUTHENTICATORS =
      new ConfigBuilder("gravitino.authenticators")
          .doc(
              "The authenticators which Gravitino uses. Multiple authenticators "
                  + "separated by commas")
          .version(ConfigConstants.VERSION_0_6_0)
          .alternatives(Lists.newArrayList("gravitino.authenticator"))
          .stringConf()
          .toSequence()
          .checkValue(
              valueList ->
                  valueList != null && valueList.stream().allMatch(StringUtils::isNotBlank),
              ConfigConstants.NOT_BLANK_ERROR_MSG)
          .createWithDefault(Lists.newArrayList("simple"));

  public static final ConfigEntry<Long> STORE_TRANSACTION_MAX_SKEW_TIME =
      new ConfigBuilder("gravitino.entity.store.maxTransactionSkewTimeMs")
          .doc("The maximum skew time of transactions in milliseconds")
          .version(ConfigConstants.VERSION_0_3_0)
          .longConf()
          .createWithDefault(2000L);

  public static final ConfigEntry<Long> KV_DELETE_AFTER_TIME =
      new ConfigBuilder(KV_DELETE_AFTER_TIME_KEY)
          .doc(
              String.format(
                  "The maximum time in milliseconds that the deleted data and old version data is kept, "
                      + "max delete time allow is %s ms(30 days), "
                      + "min delete time allow is %s ms(10 minutes)",
                  MAX_DELETE_TIME_ALLOW, MIN_DELETE_TIME_ALLOW))
          .version(ConfigConstants.VERSION_0_5_0)
          .deprecated()
          .longConf()
          .checkValue(
              v -> v >= MIN_DELETE_TIME_ALLOW && v <= MAX_DELETE_TIME_ALLOW,
              String.format(
                  "The value of %s is out of range, which must be between %s and %s",
                  KV_DELETE_AFTER_TIME_KEY, MIN_DELETE_TIME_ALLOW, MAX_DELETE_TIME_ALLOW))
          .createWithDefault(DEFAULT_KV_DELETE_AFTER_TIME);

  public static final ConfigEntry<Long> STORE_DELETE_AFTER_TIME =
      new ConfigBuilder(STORE_DELETE_AFTER_TIME_KEY)
          .doc(
              String.format(
                  "The maximum time in milliseconds that the deleted data and old version data is kept, "
                      + "max delete time allow is %s ms(30 days), "
                      + "min delete time allow is %s ms(10 minutes)",
                  MAX_DELETE_TIME_ALLOW, MIN_DELETE_TIME_ALLOW))
          .version(ConfigConstants.VERSION_0_5_0)
          .alternatives(Lists.newArrayList(KV_DELETE_AFTER_TIME_KEY))
          .longConf()
          .checkValue(
              v -> v >= MIN_DELETE_TIME_ALLOW && v <= MAX_DELETE_TIME_ALLOW,
              String.format(
                  "The value of %s is out of range, which must be between %s and %s",
                  STORE_DELETE_AFTER_TIME_KEY, MIN_DELETE_TIME_ALLOW, MAX_DELETE_TIME_ALLOW))
          .createWithDefault(DEFAULT_STORE_DELETE_AFTER_TIME);

  public static final ConfigEntry<Long> VERSION_RETENTION_COUNT =
      new ConfigBuilder(VERSION_RETENTION_COUNT_KEY)
          .doc(
              String.format(
                  "The count of versions allowed to be retained, including the current version, "
                      + "max version retention count is %s, "
                      + "min version retention count is %s",
                  MAX_VERSION_RETENTION_COUNT, MIN_VERSION_RETENTION_COUNT))
          .version(ConfigConstants.VERSION_0_5_0)
          .longConf()
          .checkValue(
              v -> v >= MIN_VERSION_RETENTION_COUNT && v <= MAX_VERSION_RETENTION_COUNT,
              String.format(
                  "The value of %s is out of range, which must be between %s and %s",
                  VERSION_RETENTION_COUNT_KEY,
                  MIN_VERSION_RETENTION_COUNT,
                  MAX_VERSION_RETENTION_COUNT))
          .createWithDefault(DEFAULT_VERSION_RETENTION_COUNT);

  // The followings are configurations for tree lock

  public static final ConfigEntry<Long> TREE_LOCK_MAX_NODE_IN_MEMORY =
      new ConfigBuilder("gravitino.lock.maxNodes")
          .doc("The maximum number of tree lock nodes to keep in memory")
          .version(ConfigConstants.VERSION_0_4_0)
          .longConf()
          .createWithDefault(MAX_NODE_IN_MEMORY);

  public static final ConfigEntry<Long> TREE_LOCK_MIN_NODE_IN_MEMORY =
      new ConfigBuilder("gravitino.lock.minNodes")
          .doc("The minimum number of tree lock nodes to keep in memory")
          .version(ConfigConstants.VERSION_0_4_0)
          .longConf()
          .createWithDefault(MIN_NODE_IN_MEMORY);

  public static final ConfigEntry<Long> TREE_LOCK_CLEAN_INTERVAL =
      new ConfigBuilder("gravitino.lock.cleanIntervalInSecs")
          .doc("The interval in seconds to clean up the stale tree lock nodes")
          .version(ConfigConstants.VERSION_0_4_0)
          .longConf()
          .createWithDefault(CLEAN_INTERVAL_IN_SECS);

  public static final ConfigEntry<Boolean> ENABLE_AUTHORIZATION =
      new ConfigBuilder("gravitino.authorization.enable")
          .doc("Enable the authorization")
          .version(ConfigConstants.VERSION_0_5_0)
          .booleanConf()
          .createWithDefault(false);

  public static final ConfigEntry<List<String>> SERVICE_ADMINS =
      new ConfigBuilder("gravitino.authorization.serviceAdmins")
          .doc("The admins of Gravitino service")
          .version(ConfigConstants.VERSION_0_5_0)
          .stringConf()
          .toSequence()
          .checkValue(
              valueList ->
                  valueList != null && valueList.stream().allMatch(StringUtils::isNotBlank),
              ConfigConstants.NOT_BLANK_ERROR_MSG)
          .create();

  public static final ConfigEntry<Long> ROLE_CACHE_EVICTION_INTERVAL_MS =
      new ConfigBuilder("gravitino.authorization.roleCacheEvictionIntervalMs")
          .doc("The interval in milliseconds to evict the role cache")
          .version(ConfigConstants.VERSION_0_5_0)
          .longConf()
          .createWithDefault(60 * 60 * 1000L);

  public static final int DEFAULT_METRICS_TIME_SLIDING_WINDOW_SECONDS = 60;
  public static final ConfigEntry<Integer> METRICS_TIME_SLIDING_WINDOW_SECONDS =
      new ConfigBuilder("gravitino.metrics.timeSlidingWindowSecs")
          .doc("The seconds of Gravitino metrics time sliding window")
          .version(ConfigConstants.VERSION_0_5_1)
          .intConf()
          .createWithDefault(DEFAULT_METRICS_TIME_SLIDING_WINDOW_SECONDS);

  public static final ConfigEntry<List<String>> REST_API_EXTENSION_PACKAGES =
      new ConfigBuilder("gravitino.server.rest.extensionPackages")
          .doc("Comma-separated list of REST API packages to expand")
          .version(ConfigConstants.VERSION_0_6_0)
          .stringConf()
          .toSequence()
          .createWithDefault(Collections.emptyList());
}
