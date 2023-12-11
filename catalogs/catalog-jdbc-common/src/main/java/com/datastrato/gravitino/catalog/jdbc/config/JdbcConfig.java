/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.catalog.jdbc.config;

import com.datastrato.gravitino.Config;
import com.datastrato.gravitino.config.ConfigBuilder;
import com.datastrato.gravitino.config.ConfigEntry;
import com.datastrato.gravitino.exceptions.GravitinoRuntimeException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.Optional;
import org.apache.commons.lang3.StringUtils;

public class JdbcConfig extends Config {

  public static final ConfigEntry<String> JDBC_URL =
      new ConfigBuilder("jdbc-url")
          .doc("The url of the Jdbc connection")
          .version("0.3.0")
          .stringConf()
          .createWithDefault(null);

  public static final ConfigEntry<Optional<String>> JDBC_DRIVER =
      new ConfigBuilder("jdbc-driver")
          .doc("The driver of the jdbc connection")
          .version("0.3.0")
          .stringConf()
          .createWithOptional();

  public static final ConfigEntry<Optional<String>> USERNAME =
      new ConfigBuilder("jdbc-user")
          .doc("The username of the Jdbc connection")
          .version("0.3.0")
          .stringConf()
          .createWithOptional();

  public static final ConfigEntry<Optional<String>> PASSWORD =
      new ConfigBuilder("jdbc-password")
          .doc("The password of the Jdbc connection")
          .version("0.3.0")
          .stringConf()
          .createWithOptional();

  public static final ConfigEntry<Integer> POOL_MIN_SIZE =
      new ConfigBuilder("jdbc.pool.min-size")
          .doc("The minimum number of connections in the pool")
          .version("0.3.0")
          .intConf()
          .createWithDefault(2);

  public static final ConfigEntry<Integer> POOL_MAX_SIZE =
      new ConfigBuilder("jdbc.pool.max-size")
          .doc("The maximum number of connections in the pool")
          .version("0.3.0")
          .intConf()
          .createWithDefault(10);

  public String getJdbcUrl() {
    return get(JDBC_URL);
  }

  public Optional<String> getJdbcDriverOptional() {
    return get(JDBC_DRIVER);
  }

  public Optional<String> getUsernameOptional() {
    return get(USERNAME);
  }

  public Optional<String> getPasswordOptional() {
    return get(PASSWORD);
  }

  public int getPoolMinSize() {
    return get(POOL_MIN_SIZE);
  }

  public int getPoolMaxSize() {
    return get(POOL_MAX_SIZE);
  }

  public String getJdbcDatabaseOrElseThrow(String errorMessage) {
    try {
      String jdbcUrl = getJdbcUrl();
      URI uri = new URI(jdbcUrl.replace("jdbc:", ""));
      String database = uri.getPath();
      if (StringUtils.startsWith(database, "/")) {
        // Remove leading slash
        database = database.substring(1);
      }
      if (StringUtils.isEmpty(database)) {
        throw new IllegalArgumentException(errorMessage);
      }
      return database;
    } catch (URISyntaxException e) {
      throw new GravitinoRuntimeException(e.getMessage(), e);
    }
  }

  public JdbcConfig(Map<String, String> properties) {
    super(false);
    loadFromMap(properties, k -> true);
  }
}
