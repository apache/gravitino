/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.catalog.jdbc.config;

import com.datastrato.gravitino.Config;
import com.datastrato.gravitino.config.ConfigBuilder;
import com.datastrato.gravitino.config.ConfigConstants;
import com.datastrato.gravitino.config.ConfigEntry;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;

public class JdbcConfig extends Config {

  public static final ConfigEntry<String> JDBC_URL =
      new ConfigBuilder("jdbc-url")
          .doc("The url of the Jdbc connection")
          .version(ConfigConstants.VERSION_0_3_0)
          .stringConf()
          .checkValue(StringUtils::isNotBlank, ConfigConstants.NOT_BLANK_ERROR_MSG)
          .create();

  public static final ConfigEntry<String> JDBC_DATABASE =
      new ConfigBuilder("jdbc-database")
          .doc("The database of the jdbc connection")
          .version(ConfigConstants.VERSION_0_3_0)
          .stringConf()
          .checkValue(StringUtils::isNotBlank, ConfigConstants.NOT_BLANK_ERROR_MSG)
          .create();

  public static final ConfigEntry<String> JDBC_DRIVER =
      new ConfigBuilder("jdbc-driver")
          .doc("The driver of the jdbc connection")
          .version(ConfigConstants.VERSION_0_3_0)
          .stringConf()
          .checkValue(StringUtils::isNotBlank, ConfigConstants.NOT_BLANK_ERROR_MSG)
          .create();

  public static final ConfigEntry<String> USERNAME =
      new ConfigBuilder("jdbc-user")
          .doc("The username of the Jdbc connection")
          .version(ConfigConstants.VERSION_0_3_0)
          .stringConf()
          .checkValue(StringUtils::isNotBlank, ConfigConstants.NOT_BLANK_ERROR_MSG)
          .create();

  public static final ConfigEntry<String> PASSWORD =
      new ConfigBuilder("jdbc-password")
          .doc("The password of the Jdbc connection")
          .version(ConfigConstants.VERSION_0_3_0)
          .stringConf()
          .checkValue(StringUtils::isNotBlank, ConfigConstants.NOT_BLANK_ERROR_MSG)
          .create();

  public static final ConfigEntry<Integer> POOL_MIN_SIZE =
      new ConfigBuilder("jdbc.pool.min-size")
          .doc("The minimum number of connections in the pool")
          .version(ConfigConstants.VERSION_0_3_0)
          .intConf()
          .checkValue(value -> value > 0, ConfigConstants.POSITIVE_NUMBER_ERROR_MSG)
          .createWithDefault(2);

  public static final ConfigEntry<Integer> POOL_MAX_SIZE =
      new ConfigBuilder("jdbc.pool.max-size")
          .doc("The maximum number of connections in the pool")
          .version(ConfigConstants.VERSION_0_3_0)
          .intConf()
          .checkValue(value -> value > 0, ConfigConstants.POSITIVE_NUMBER_ERROR_MSG)
          .createWithDefault(10);

  public String getJdbcUrl() {
    return get(JDBC_URL);
  }

  public String getJdbcDriver() {
    return get(JDBC_DRIVER);
  }

  public String getUsername() {
    return get(USERNAME);
  }

  public String getPassword() {
    return get(PASSWORD);
  }

  public int getPoolMinSize() {
    return get(POOL_MIN_SIZE);
  }

  public int getPoolMaxSize() {
    return get(POOL_MAX_SIZE);
  }

  public String getJdbcDatabase() {
    return get(JDBC_DATABASE);
  }

  public JdbcConfig(Map<String, String> properties) {
    super(false);
    loadFromMap(properties, k -> true);
  }
}
