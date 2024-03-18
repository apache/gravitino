/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.mysql;

import static com.datastrato.gravitino.connector.PropertyEntry.enumImmutablePropertyEntry;
import static com.datastrato.gravitino.connector.PropertyEntry.integerOptionalPropertyEntry;
import static com.datastrato.gravitino.connector.PropertyEntry.stringReservedPropertyEntry;

import com.datastrato.gravitino.catalog.jdbc.JdbcTablePropertiesMetadata;
import com.datastrato.gravitino.connector.PropertyEntry;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.collections4.BidiMap;
import org.apache.commons.collections4.bidimap.TreeBidiMap;

public class MysqlTablePropertiesMetadata extends JdbcTablePropertiesMetadata {
  public static final String GRAVITINO_ENGINE_KEY = "engine";
  public static final String MYSQL_ENGINE_KEY = "ENGINE";
  public static final String GRAVITINO_AUTO_INCREMENT_OFFSET_KEY = "auto-increment-offset";
  public static final String MYSQL_AUTO_INCREMENT_OFFSET_KEY = "AUTO_INCREMENT";
  private static final Map<String, PropertyEntry<?>> PROPERTIES_METADATA =
      createPropertiesMetadata();

  public static final BidiMap<String, String> GRAVITINO_CONFIG_TO_MYSQL =
      createGravitinoConfigToMysql();

  private static BidiMap<String, String> createGravitinoConfigToMysql() {
    BidiMap<String, String> map = new TreeBidiMap<>();
    map.put(GRAVITINO_ENGINE_KEY, MYSQL_ENGINE_KEY);
    map.put(GRAVITINO_AUTO_INCREMENT_OFFSET_KEY, MYSQL_AUTO_INCREMENT_OFFSET_KEY);
    return map;
  }

  private static Map<String, PropertyEntry<?>> createPropertiesMetadata() {
    Map<String, PropertyEntry<?>> map = new HashMap<>();
    map.put(COMMENT_KEY, stringReservedPropertyEntry(COMMENT_KEY, "The table comment", true));
    map.put(
        GRAVITINO_ENGINE_KEY,
        enumImmutablePropertyEntry(
            GRAVITINO_ENGINE_KEY,
            "The table engine",
            false,
            ENGINE.class,
            ENGINE.INNODB,
            false,
            false));
    // auto_increment properties can only be specified when creating and cannot be
    // modified.
    map.put(
        GRAVITINO_AUTO_INCREMENT_OFFSET_KEY,
        integerOptionalPropertyEntry(
            GRAVITINO_AUTO_INCREMENT_OFFSET_KEY,
            "The table auto increment offset",
            true,
            null,
            false));
    return Collections.unmodifiableMap(map);
  }

  public enum ENGINE {
    NDBCLUSTER("ndbcluster"),
    FEDERATED("FEDERATED"),
    MEMORY("MEMORY"),
    INNODB("InnoDB"),
    PERFORMANCE_SCHEMA("PERFORMANCE_SCHEMA"),
    MYISAM("MyISAM"),
    NDBINFO("ndbinfo"),
    MRG_MYISAM("MRG_MYISAM"),
    BLACKHOLE("BLACKHOLE"),
    CSV("CSV"),
    ARCHIVE("ARCHIVE");

    private final String value;

    ENGINE(String value) {
      this.value = value;
    }
  }

  @Override
  protected Map<String, PropertyEntry<?>> specificPropertyEntries() {
    return PROPERTIES_METADATA;
  }

  @Override
  public Map<String, String> transformToJdbcProperties(Map<String, String> properties) {
    return Collections.unmodifiableMap(
        new HashMap<String, String>() {
          {
            properties.forEach(
                (key, value) -> {
                  if (GRAVITINO_CONFIG_TO_MYSQL.containsKey(key)) {
                    put(GRAVITINO_CONFIG_TO_MYSQL.get(key), value);
                  }
                });
          }
        });
  }

  @Override
  protected Map<String, String> convertFromJdbcProperties(Map<String, String> properties) {
    BidiMap<String, String> mysqlConfigToGravitino = GRAVITINO_CONFIG_TO_MYSQL.inverseBidiMap();
    return Collections.unmodifiableMap(
        new HashMap<String, String>() {
          {
            properties.forEach(
                (key, value) -> {
                  if (mysqlConfigToGravitino.containsKey(key)) {
                    put(mysqlConfigToGravitino.get(key), value);
                  }
                });
          }
        });
  }
}
