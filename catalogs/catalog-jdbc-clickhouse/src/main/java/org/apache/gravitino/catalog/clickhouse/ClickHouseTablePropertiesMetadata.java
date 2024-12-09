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
package org.apache.gravitino.catalog.clickhouse;

import static org.apache.gravitino.connector.PropertyEntry.enumImmutablePropertyEntry;
import static org.apache.gravitino.connector.PropertyEntry.stringReservedPropertyEntry;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.collections4.BidiMap;
import org.apache.commons.collections4.bidimap.TreeBidiMap;
import org.apache.gravitino.catalog.jdbc.JdbcTablePropertiesMetadata;
import org.apache.gravitino.connector.PropertyEntry;

public class ClickHouseTablePropertiesMetadata extends JdbcTablePropertiesMetadata {
  public static final String GRAVITINO_ENGINE_KEY = ClickHouseConstants.GRAVITINO_ENGINE_KEY;
  public static final String CLICKHOUSE_ENGINE_KEY = ClickHouseConstants.CLICKHOUSE_ENGINE_KEY;
  private static final Map<String, PropertyEntry<?>> PROPERTIES_METADATA =
      createPropertiesMetadata();

  public static final BidiMap<String, String> GRAVITINO_CONFIG_TO_CLICKHOUSE =
      createGravitinoConfigToClickhouse();

  private static BidiMap<String, String> createGravitinoConfigToClickhouse() {
    BidiMap<String, String> map = new TreeBidiMap<>();
    map.put(GRAVITINO_ENGINE_KEY, CLICKHOUSE_ENGINE_KEY);
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
            ENGINE.MERGETREE,
            false,
            false));
    return Collections.unmodifiableMap(map);
  }

  /** refer https://clickhouse.com/docs/en/engines/table-engines */
  public enum ENGINE {
    // MergeTree
    MERGETREE("MergeTree"),
    REPLACINGMERGETREE("ReplacingMergeTree"),
    SUMMINGMERGETREE("SummingMergeTree"),
    AGGREGATINGMERGETREE("AggregatingMergeTree"),
    COLLAPSINGMERGETREE("CollapsingMergeTree"),
    VERSIONEDCOLLAPSINGMERGETREE("VersionedCollapsingMergeTree"),
    GRAPHITEMERGETREE("GraphiteMergeTree"),

    // Log
    TINYLOG("TinyLog"),
    STRIPELOG("StripeLog"),
    LOG("Log"),

    // Integration Engines
    ODBC("ODBC"),
    JDBC("JDBC"),
    MySQL("MySQL"),
    MONGODB("MongoDB"),
    Redis("Redis"),
    HDFS("HDFS"),
    S3("S3"),
    KAFKA("Kafka"),
    EMBEDDEDROCKSDB("EmbeddedRocksDB"),
    RABBITMQ("RabbitMQ"),
    POSTGRESQL("PostgreSQL"),
    S3QUEUE("S3Queue"),
    TIMESERIES("TimeSeries"),

    // Special Engines
    DISTRIBUTED("Distributed"),
    DICTIONARY("Dictionary"),
    MERGE("Merge"),
    FILE("File"),
    NULL("Null"),
    SET("Set"),
    JOIN("Join"),
    URL("URL"),
    VIEW("View"),
    MEMORY("Memory"),
    BUFFER("Buffer"),
    KEEPER_MAP("KeeperMap");

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
                  if (GRAVITINO_CONFIG_TO_CLICKHOUSE.containsKey(key)) {
                    put(GRAVITINO_CONFIG_TO_CLICKHOUSE.get(key), value);
                  }
                });
          }
        });
  }

  @Override
  public Map<String, String> convertFromJdbcProperties(Map<String, String> properties) {
    BidiMap<String, String> clickhouseConfigToGravitino =
        GRAVITINO_CONFIG_TO_CLICKHOUSE.inverseBidiMap();
    return Collections.unmodifiableMap(
        new HashMap<String, String>() {
          {
            properties.forEach(
                (key, value) -> {
                  if (clickhouseConfigToGravitino.containsKey(key)) {
                    put(clickhouseConfigToGravitino.get(key), value);
                  }
                });
          }
        });
  }
}
