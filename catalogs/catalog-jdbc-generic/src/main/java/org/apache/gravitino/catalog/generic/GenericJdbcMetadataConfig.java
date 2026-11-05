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
package org.apache.gravitino.catalog.generic;

import java.util.Arrays;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;

/** Metadata discovery configuration for the generic JDBC catalog. */
public class GenericJdbcMetadataConfig {

  /** Optional JDBC catalog pattern passed to {@link java.sql.DatabaseMetaData}. */
  public static final String CATALOG_PATTERN = "jdbc.metadata.catalog-pattern";

  /** Optional JDBC schema pattern passed to {@link java.sql.DatabaseMetaData}. */
  public static final String SCHEMA_PATTERN = "jdbc.metadata.schema-pattern";

  /** Comma-separated table types passed to {@link java.sql.DatabaseMetaData#getTables}. */
  public static final String TABLE_TYPES = "jdbc.metadata.table-types";

  /** Comma-separated schemas to include in metadata discovery. */
  public static final String INCLUDE_SCHEMAS = "jdbc.metadata.include-schemas";

  /** Comma-separated schemas to exclude from metadata discovery. */
  public static final String EXCLUDE_SCHEMAS = "jdbc.metadata.exclude-schemas";

  private static final String DEFAULT_TABLE_TYPES = "TABLE,VIEW";

  private static final Set<String> DEFAULT_EXCLUDED_SCHEMAS =
      Set.of(
          "information_schema",
          "mysql",
          "performance_schema",
          "pg_catalog",
          "pg_toast",
          "sys",
          "system");

  private final String catalogPattern;
  private final String schemaPattern;
  private final String[] tableTypes;
  private final Set<String> includedSchemas;
  private final Set<String> excludedSchemas;

  /** Creates a metadata discovery config from catalog properties. */
  public GenericJdbcMetadataConfig(Map<String, String> properties) {
    this.catalogPattern = normalizePattern(properties.get(CATALOG_PATTERN));
    this.schemaPattern = normalizePattern(properties.get(SCHEMA_PATTERN));
    this.tableTypes =
        split(properties.getOrDefault(TABLE_TYPES, DEFAULT_TABLE_TYPES)).stream()
            .map(type -> type.toUpperCase(Locale.ROOT))
            .toArray(String[]::new);
    this.includedSchemas = split(properties.get(INCLUDE_SCHEMAS));
    this.excludedSchemas =
        properties.containsKey(EXCLUDE_SCHEMAS)
            ? split(properties.get(EXCLUDE_SCHEMAS))
            : DEFAULT_EXCLUDED_SCHEMAS;
  }

  /** Returns the JDBC catalog pattern to pass into metadata calls. */
  public String catalogPattern() {
    return catalogPattern;
  }

  /** Returns the JDBC schema pattern to pass into metadata calls. */
  public String schemaPattern() {
    return schemaPattern;
  }

  /** Returns table types to include in table metadata discovery. */
  public String[] tableTypes() {
    return tableTypes.clone();
  }

  /** Returns whether the schema should be exposed by the generic catalog. */
  public boolean includesSchema(String schemaName) {
    String normalized = normalizeName(schemaName);
    return (includedSchemas.isEmpty() || includedSchemas.contains(normalized))
        && !excludedSchemas.contains(normalized);
  }

  private static String normalizePattern(String pattern) {
    if (StringUtils.isBlank(pattern)) {
      return null;
    }
    return pattern.replace('*', '%');
  }

  private static Set<String> split(String value) {
    if (StringUtils.isBlank(value)) {
      return Set.of();
    }
    return Arrays.stream(value.split(","))
        .map(String::trim)
        .filter(StringUtils::isNotEmpty)
        .map(GenericJdbcMetadataConfig::normalizeName)
        .collect(Collectors.toUnmodifiableSet());
  }

  private static String normalizeName(String value) {
    return value.toLowerCase(Locale.ROOT);
  }
}
