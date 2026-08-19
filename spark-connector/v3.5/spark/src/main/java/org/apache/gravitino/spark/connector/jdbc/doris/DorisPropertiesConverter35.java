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
package org.apache.gravitino.spark.connector.jdbc.doris;

import com.google.common.collect.ImmutableSet;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.gravitino.spark.connector.PropertiesConverter;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/** Converts Gravitino Doris properties into protected Spark native and JDBC options. */
final class DorisPropertiesConverter35 implements PropertiesConverter {

  private static final Pattern FE_ENDPOINT = Pattern.compile("[A-Za-z0-9._-]+:(\\d+)");
  private static final Set<String> ALLOWED_OPTIONS =
      ImmutableSet.of(
          "doris.request.retries",
          "doris.request.connect.timeout.ms",
          "doris.request.read.timeout.ms",
          "doris.request.query.timeout.s",
          "doris.request.tablet.size",
          "doris.batch.size",
          "doris.exec.mem.limit",
          "doris.filter.query.in.max.count",
          "doris.thrift.max.message.size");
  private static final Set<String> PROTECTED_OPTIONS =
      ImmutableSet.of(
          "doris.fenodes",
          "doris.query.port",
          "doris.user",
          "doris.password",
          "doris.table.identifier",
          "url",
          "driver",
          "user",
          "password",
          "dbtable");

  private static final DorisPropertiesConverter35 INSTANCE = new DorisPropertiesConverter35();

  private DorisPropertiesConverter35() {}

  static DorisPropertiesConverter35 getInstance() {
    return INSTANCE;
  }

  @Override
  public Map<String, String> toSparkCatalogProperties(
      CaseInsensitiveStringMap options, Map<String, String> properties) {
    Map<String, String> result = toSparkCatalogProperties(properties);
    if (properties != null) {
      properties.forEach(
          (key, value) -> {
            if (key.startsWith(SPARK_PROPERTY_PREFIX)) {
              result.put(validateOptionKey(key.substring(SPARK_PROPERTY_PREFIX.length())), value);
            }
          });
    }
    if (options != null) {
      options
          .asCaseSensitiveMap()
          .forEach((key, value) -> result.put(validateOptionKey(key), value));
    }
    return result;
  }

  @Override
  public Map<String, String> toSparkCatalogProperties(Map<String, String> properties) {
    if (properties == null) {
      throw new IllegalArgumentException("Doris catalog properties must not be null");
    }
    Map<String, String> result = new HashMap<>();
    String feNodes = properties.get(DorisConnectorConstants35.GRAVITINO_FE_NODES);
    if (feNodes != null) {
      result.put(DorisConnectorConstants35.DORIS_FE_NODES, normalizeFeNodes(feNodes));
    }
    String queryPort = properties.get(DorisConnectorConstants35.GRAVITINO_QUERY_PORT);
    if (queryPort != null) {
      result.put(DorisConnectorConstants35.DORIS_QUERY_PORT, normalizePort(queryPort));
    }
    putIfPresent(result, "url", properties.get(DorisConnectorConstants35.JDBC_URL));
    putIfPresent(result, "driver", properties.get(DorisConnectorConstants35.JDBC_DRIVER));
    putJdbcOption(
        result, "partitionColumn", properties, DorisConnectorConstants35.JDBC_PARTITION_COLUMN);
    putJdbcOption(result, "lowerBound", properties, DorisConnectorConstants35.JDBC_LOWER_BOUND);
    putJdbcOption(result, "upperBound", properties, DorisConnectorConstants35.JDBC_UPPER_BOUND);
    putJdbcOption(
        result, "numPartitions", properties, DorisConnectorConstants35.JDBC_NUM_PARTITIONS);
    putJdbcOption(result, "fetchsize", properties, DorisConnectorConstants35.JDBC_FETCH_SIZE);
    properties.forEach(
        (key, value) -> {
          String canonicalKey = key.toLowerCase(Locale.ROOT);
          if (ALLOWED_OPTIONS.contains(canonicalKey)) {
            result.put(canonicalKey, value);
          }
        });
    return result;
  }

  @Override
  public Map<String, String> toGravitinoTableProperties(Map<String, String> properties) {
    return new HashMap<>(properties);
  }

  @Override
  public Map<String, String> toSparkTableProperties(Map<String, String> properties) {
    return new HashMap<>(properties);
  }

  private static String validateOptionKey(String key) {
    String canonicalKey = key.toLowerCase(Locale.ROOT);
    if (PROTECTED_OPTIONS.contains(canonicalKey)) {
      throw new IllegalArgumentException(
          "Doris Spark options cannot override protected properties");
    }
    if (!ALLOWED_OPTIONS.contains(canonicalKey)) {
      throw new IllegalArgumentException("Unsupported Doris Spark read option: " + key);
    }
    return canonicalKey;
  }

  private static void putJdbcOption(
      Map<String, String> result, String sparkKey, Map<String, String> properties, String key) {
    putIfPresent(result, sparkKey, properties.get(key));
  }

  private static void putIfPresent(Map<String, String> result, String key, String value) {
    if (value != null) {
      result.put(key, value);
    }
  }

  private static String normalizeFeNodes(String value) {
    if (value == null || value.trim().isEmpty()) {
      throw new IllegalArgumentException(
          DorisConnectorConstants35.GRAVITINO_FE_NODES + " must not be blank");
    }
    StringBuilder normalized = new StringBuilder();
    for (String rawEndpoint : value.split(",", -1)) {
      String endpoint = rawEndpoint.trim();
      Matcher matcher = FE_ENDPOINT.matcher(endpoint);
      if (!matcher.matches()) {
        throw new IllegalArgumentException(
            DorisConnectorConstants35.GRAVITINO_FE_NODES
                + " must use host:port[,host2:port] format");
      }
      validatePort(DorisConnectorConstants35.GRAVITINO_FE_NODES, matcher.group(1));
      if (normalized.length() > 0) {
        normalized.append(',');
      }
      normalized.append(endpoint);
    }
    return normalized.toString();
  }

  private static String normalizePort(String value) {
    validatePort(DorisConnectorConstants35.GRAVITINO_QUERY_PORT, value);
    return Integer.toString(Integer.parseInt(value));
  }

  private static void validatePort(String property, String value) {
    try {
      if (value == null || !value.equals(value.trim())) {
        throw new NumberFormatException("port contains whitespace");
      }
      int port = Integer.parseInt(value);
      if (port < 1 || port > 65535) {
        throw new NumberFormatException("port out of range");
      }
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(property + " must contain a port between 1 and 65535");
    }
  }
}
