/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.catalog.jdbc.config;

import com.datastrato.gravitino.Config;
import com.datastrato.gravitino.config.ConfigBuilder;
import com.datastrato.gravitino.config.ConfigEntry;
import java.util.Map;

public class JdbcConfig extends Config {

  public static final ConfigEntry<String> JDBC_URL =
      new ConfigBuilder("jdbc-url")
          .doc("The url of the Jdbc connection")
          .version("0.3.0")
          .stringConf()
          .createWithDefault(null);

  public static final ConfigEntry<String> USERNAME =
      new ConfigBuilder("jdbc-user")
          .doc("The username of the Jdbc connection")
          .version("0.3.0")
          .stringConf()
          .createWithDefault(null);

  public static final ConfigEntry<String> PASSWORD =
      new ConfigBuilder("jdbc-password")
          .doc("The password of the Jdbc connection")
          .version("0.3.0")
          .stringConf()
          .createWithDefault(null);

  public static final ConfigEntry<Integer> POOL_MIN_IDLE =
      new ConfigBuilder("jdbc.connect-pool.min-idle")
          .doc("The minimum number of connections in the pool")
          .version("0.3.0")
          .intConf()
          .createWithDefault(2);

  public static final ConfigEntry<Integer> POOL_MAX_SIZE =
      new ConfigBuilder("jdbc.connect-pool.max-size")
          .doc("The maximum number of connections in the pool")
          .version("0.3.0")
          .intConf()
          .createWithDefault(10);

  public String getJdbcUrl() {
    return get(JDBC_URL);
  }

  public String getUsername() {
    return get(USERNAME);
  }

  public String getPassword() {
    return get(PASSWORD);
  }

  public int getPoolMinIdle() {
    return get(POOL_MIN_IDLE);
  }

  public int getPoolMaxSize() {
    return get(POOL_MAX_SIZE);
  }

  public JdbcConfig(Map<String, String> properties) {
    super(false);
    loadFromMap(properties, k -> true);
    assert null != getJdbcUrl();
  }
}
