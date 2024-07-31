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
package org.apache.gravitino.catalog.mysql;

import static org.apache.gravitino.connector.PropertyEntry.enumImmutablePropertyEntry;
import static org.apache.gravitino.connector.PropertyEntry.integerOptionalPropertyEntry;
import static org.apache.gravitino.connector.PropertyEntry.stringReservedPropertyEntry;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.collections4.BidiMap;
import org.apache.commons.collections4.bidimap.TreeBidiMap;
import org.apache.gravitino.catalog.jdbc.JdbcTablePropertiesMetadata;
import org.apache.gravitino.connector.PropertyEntry;

public class MysqlTablePropertiesMetadata extends JdbcTablePropertiesMetadata {
  public static final String GRAVITINO_ENGINE_KEY = MysqlConstants.GRAVITINO_ENGINE_KEY;
  public static final String MYSQL_ENGINE_KEY = MysqlConstants.MYSQL_ENGINE_KEY;
  public static final String GRAVITINO_AUTO_INCREMENT_OFFSET_KEY =
      MysqlConstants.GRAVITINO_AUTO_INCREMENT_OFFSET_KEY;
  public static final String MYSQL_AUTO_INCREMENT_OFFSET_KEY =
      MysqlConstants.MYSQL_AUTO_INCREMENT_OFFSET_KEY;
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

    public String getValue() {
      return this.value;
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
  public Map<String, String> convertFromJdbcProperties(Map<String, String> properties) {
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
