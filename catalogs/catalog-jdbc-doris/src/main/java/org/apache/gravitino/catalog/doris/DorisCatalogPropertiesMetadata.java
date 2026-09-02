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
import static org.apache.gravitino.connector.PropertyEntry.stringImmutablePropertyEntry;
import static org.apache.gravitino.connector.PropertyEntry.stringOptionalPropertyEntry;

import com.google.common.collect.ImmutableMap;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.gravitino.catalog.jdbc.JdbcCatalogPropertiesMetadata;
import org.apache.gravitino.connector.PropertyEntry;

/** Catalog property metadata for Apache Doris-specific Spark read/write configuration. */
public class DorisCatalogPropertiesMetadata extends JdbcCatalogPropertiesMetadata {

  /** The optional comma-separated Doris FE endpoint property. */
  public static final String DORIS_FE_NODES = "doris-fenodes";

  /** The optional Doris MySQL-protocol query port property. */
  public static final String DORIS_QUERY_PORT = "doris-query-port";

  /** The governed Doris Spark write mode property. */
  public static final String DORIS_WRITE_MODE = "doris-write-mode";

  /** The governed Doris Spark overwrite mode property. */
  public static final String DORIS_WRITE_OVERWRITE_MODE = "doris-write-overwrite-mode";

  /** The default value that keeps governed Doris Spark writes disabled. */
  public static final String WRITE_DISABLED = "disabled";

  /** The opt-in governed Doris Spark batch-write mode. */
  public static final String WRITE_BATCH = "batch";

  /** The default value that rejects governed Doris Spark overwrite. */
  public static final String WRITE_OVERWRITE_REJECT = "reject";

  /** The opt-in non-atomic truncate-then-load overwrite mode. */
  public static final String WRITE_OVERWRITE_TRUNCATE = "truncate";

  private static final Pattern FE_ENDPOINT = Pattern.compile("[A-Za-z0-9._-]+:(\\d+)");

  private static final Map<String, PropertyEntry<?>> PROPERTIES_METADATA =
      ImmutableMap.<String, PropertyEntry<?>>builder()
          .put(
              DORIS_FE_NODES,
              stringOptionalPropertyEntry(
                  DORIS_FE_NODES,
                  "Comma-separated Doris FE endpoints in host:port format",
                  true /* immutable */,
                  null,
                  false /* hidden */))
          .put(
              DORIS_QUERY_PORT,
              integerOptionalPropertyEntry(
                  DORIS_QUERY_PORT,
                  "Doris MySQL-protocol query port",
                  true /* immutable */,
                  null,
                  false /* hidden */))
          .put(
              DORIS_WRITE_MODE,
              stringImmutablePropertyEntry(
                  DORIS_WRITE_MODE,
                  "Governed Doris Spark write mode: disabled or batch",
                  false /* required */,
                  WRITE_DISABLED,
                  false /* hidden */,
                  false /* reserved */))
          .put(
              DORIS_WRITE_OVERWRITE_MODE,
              stringImmutablePropertyEntry(
                  DORIS_WRITE_OVERWRITE_MODE,
                  "Governed Doris Spark overwrite mode: reject or truncate",
                  false /* required */,
                  WRITE_OVERWRITE_REJECT,
                  false /* hidden */,
                  false /* reserved */))
          .build();

  /**
   * Returns the JDBC property entries together with the Doris Spark read/write entries.
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
    String writeMode =
        normalizeWriteMode(properties.getOrDefault(DORIS_WRITE_MODE, WRITE_DISABLED));
    String overwriteMode =
        normalizeOverwriteMode(
            properties.getOrDefault(DORIS_WRITE_OVERWRITE_MODE, WRITE_OVERWRITE_REJECT));
    if (WRITE_DISABLED.equals(writeMode) && WRITE_OVERWRITE_TRUNCATE.equals(overwriteMode)) {
      throw new IllegalArgumentException(
          DORIS_WRITE_OVERWRITE_MODE + "=truncate requires " + DORIS_WRITE_MODE + "=batch");
    }
    if (properties.containsKey(DORIS_WRITE_MODE)) {
      result.put(DORIS_WRITE_MODE, writeMode);
    }
    if (properties.containsKey(DORIS_WRITE_OVERWRITE_MODE)) {
      result.put(DORIS_WRITE_OVERWRITE_MODE, overwriteMode);
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

  static String normalizeWriteMode(String value) {
    if (WRITE_DISABLED.equals(value) || WRITE_BATCH.equals(value)) {
      return value;
    }
    throw new IllegalArgumentException(DORIS_WRITE_MODE + " must be disabled or batch");
  }

  static String normalizeOverwriteMode(String value) {
    if (WRITE_OVERWRITE_REJECT.equals(value) || WRITE_OVERWRITE_TRUNCATE.equals(value)) {
      return value;
    }
    throw new IllegalArgumentException(DORIS_WRITE_OVERWRITE_MODE + " must be reject or truncate");
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
