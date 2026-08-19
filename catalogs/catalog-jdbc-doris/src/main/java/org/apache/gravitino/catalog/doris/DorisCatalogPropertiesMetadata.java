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
package org.apache.gravitino.catalog.doris;

import static org.apache.gravitino.connector.PropertyEntry.integerOptionalPropertyEntry;
import static org.apache.gravitino.connector.PropertyEntry.stringOptionalPropertyEntry;

import com.google.common.collect.ImmutableMap;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.gravitino.catalog.jdbc.JdbcCatalogPropertiesMetadata;
import org.apache.gravitino.connector.PropertyEntry;

/** Catalog property metadata for Apache Doris-specific Spark read configuration. */
public class DorisCatalogPropertiesMetadata extends JdbcCatalogPropertiesMetadata {

  /** The optional comma-separated Doris FE endpoint property. */
  public static final String DORIS_FE_NODES = "doris-fenodes";

  /** The optional Doris MySQL-protocol query port property. */
  public static final String DORIS_QUERY_PORT = "doris-query-port";

  private static final Pattern FE_ENDPOINT = Pattern.compile("[A-Za-z0-9._-]+:(\\d+)");

  private static final Map<String, PropertyEntry<?>> PROPERTIES_METADATA =
      ImmutableMap.of(
          DORIS_FE_NODES,
          stringOptionalPropertyEntry(
              DORIS_FE_NODES,
              "Comma-separated Doris FE endpoints in host:port format",
              true /* immutable */,
              null,
              false /* hidden */),
          DORIS_QUERY_PORT,
          integerOptionalPropertyEntry(
              DORIS_QUERY_PORT,
              "Doris MySQL-protocol query port",
              true /* immutable */,
              null,
              false /* hidden */));

  /**
   * Returns the JDBC property entries together with the Doris Spark read entries.
   *
   * @return merged catalog property entries
   */
  @Override
  protected Map<String, PropertyEntry<?>> specificPropertyEntries() {
    Map<String, PropertyEntry<?>> entries = new HashMap<>(super.specificPropertyEntries());
    entries.putAll(PROPERTIES_METADATA);
    return entries;
  }

  /**
   * Validates and normalizes JDBC and Doris catalog properties.
   *
   * @param properties catalog properties to transform
   * @return transformed catalog properties
   */
  @Override
  public Map<String, String> transformProperties(Map<String, String> properties) {
    Map<String, String> result = new HashMap<>(super.transformProperties(properties));
    if (properties.containsKey(DORIS_FE_NODES)) {
      result.put(DORIS_FE_NODES, normalizeFeNodes(properties.get(DORIS_FE_NODES)));
    }
    if (properties.containsKey(DORIS_QUERY_PORT)) {
      result.put(DORIS_QUERY_PORT, normalizePort(properties.get(DORIS_QUERY_PORT)));
    }
    return result;
  }

  static String normalizeFeNodes(String value) {
    if (value == null || value.trim().isEmpty()) {
      throw new IllegalArgumentException(DORIS_FE_NODES + " must not be blank");
    }

    StringBuilder normalized = new StringBuilder();
    for (String rawEndpoint : value.split(",", -1)) {
      String endpoint = rawEndpoint.trim();
      Matcher matcher = FE_ENDPOINT.matcher(endpoint);
      if (!matcher.matches()) {
        throw new IllegalArgumentException(
            DORIS_FE_NODES + " must use host:port[,host2:port] format");
      }
      validatePort(DORIS_FE_NODES, matcher.group(1));
      if (normalized.length() > 0) {
        normalized.append(',');
      }
      normalized.append(endpoint);
    }
    return normalized.toString();
  }

  static String normalizePort(String value) {
    validatePort(DORIS_QUERY_PORT, value);
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
