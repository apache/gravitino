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

package org.apache.gravitino.maintenance.optimizer.common.util;

import com.google.common.base.Preconditions;
import java.util.Arrays;
import java.util.Optional;
import java.util.regex.Pattern;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;

/** Utilities for working with fully qualified table identifiers. */
public class IdentifierUtils {
  private static final String NORMALIZED_IDENTIFIER_MESSAGE =
      "Identifier must be catalog.schema.table";

  /**
   * Server default for {@code gravitino.schema.separator}. Production TMS paths must load the live
   * value from {@code GET /configs}; this constant is only for tests that pass an explicit
   * separator.
   */
  public static final String DEFAULT_SCHEMA_SEPARATOR = ":";

  /**
   * Removes the catalog level from a catalog.schema.table identifier.
   *
   * @param tableIdentifier fully qualified table identifier
   * @return schema.table identifier
   * @throws IllegalArgumentException if the identifier is not catalog.schema.table
   */
  public static NameIdentifier removeCatalogFromIdentifier(NameIdentifier tableIdentifier) {
    Preconditions.checkArgument(tableIdentifier != null, "tableIdentifier must not be null");
    Namespace namespace = tableIdentifier.namespace();
    Preconditions.checkArgument(
        namespace != null && namespace.levels().length == 2, NORMALIZED_IDENTIFIER_MESSAGE);
    return NameIdentifier.of(namespace.levels()[1], tableIdentifier.name());
  }

  /**
   * Returns the catalog name from a catalog.schema.table identifier.
   *
   * @param tableIdentifier fully qualified table identifier
   * @return catalog name
   * @throws IllegalArgumentException if the identifier is not catalog.schema.table
   */
  public static String getCatalogNameFromTableIdentifier(NameIdentifier tableIdentifier) {
    Preconditions.checkArgument(tableIdentifier != null, "tableIdentifier must not be null");
    Namespace namespace = tableIdentifier.namespace();
    Preconditions.checkArgument(
        namespace != null && namespace.levels().length == 2, NORMALIZED_IDENTIFIER_MESSAGE);
    return namespace.levels()[0];
  }

  /**
   * Validates that a table identifier is normalized as catalog.schema.table.
   *
   * @param tableIdentifier identifier to validate
   * @throws IllegalArgumentException if the identifier is not catalog.schema.table
   */
  public static void requireTableIdentifierNormalized(NameIdentifier tableIdentifier) {
    Preconditions.checkArgument(tableIdentifier != null, "tableIdentifier must not be null");
    Namespace namespace = tableIdentifier.namespace();
    Preconditions.checkArgument(
        namespace != null && namespace.levels().length == 2, NORMALIZED_IDENTIFIER_MESSAGE);
  }

  /**
   * Builds a Spark/Iceberg table identifier from {@code catalog.schema.table}, splitting a
   * hierarchical Gravitino schema (e.g. {@code a:b:c}) into dotted namespace levels ({@code
   * a.b.c.events}).
   *
   * <p>Callers must pass the server's configured separator (from {@code GET /configs}); do not
   * invent a default.
   *
   * @param tableIdentifier catalog.schema.table identifier
   * @param schemaSeparator external hierarchical schema separator (must not contain {@code .})
   * @return Spark table identifier
   */
  public static String toSparkTableIdentifier(
      NameIdentifier tableIdentifier, String schemaSeparator) {
    requireTableIdentifierNormalized(tableIdentifier);
    Preconditions.checkArgument(
        StringUtils.isNotBlank(schemaSeparator), "schemaSeparator must not be blank");
    Preconditions.checkArgument(
        !schemaSeparator.contains("."), "schemaSeparator must not contain '.'");
    String schema = tableIdentifier.namespace().level(1);
    String table = tableIdentifier.name();
    Preconditions.checkArgument(StringUtils.isNotBlank(table), "table name must not be blank");
    String[] segments = schema.split(Pattern.quote(schemaSeparator), -1);
    for (String segment : segments) {
      Preconditions.checkArgument(
          StringUtils.isNotBlank(segment),
          "hierarchical schema '%s' contains an empty segment for separator '%s'",
          schema,
          schemaSeparator);
    }
    return String.join(".", segments) + "." + table;
  }

  /**
   * Converts a Spark/Iceberg dotted table identifier back to {@code catalog.schema.table}, joining
   * all but the last level with {@code schemaSeparator} as the Gravitino schema name.
   *
   * @param catalogName catalog name
   * @param sparkTableIdentifier Spark table id (e.g. {@code a.b.c.events} or {@code db.t})
   * @param schemaSeparator external hierarchical schema separator
   * @return catalog.schema.table identifier
   */
  public static NameIdentifier fromSparkTableIdentifier(
      String catalogName, String sparkTableIdentifier, String schemaSeparator) {
    Preconditions.checkArgument(
        StringUtils.isNotBlank(catalogName), "catalogName must not be blank");
    Preconditions.checkArgument(
        StringUtils.isNotBlank(sparkTableIdentifier), "sparkTableIdentifier must not be blank");
    Preconditions.checkArgument(
        StringUtils.isNotBlank(schemaSeparator), "schemaSeparator must not be blank");
    Preconditions.checkArgument(
        !schemaSeparator.contains("."), "schemaSeparator must not contain '.'");
    String[] levels = sparkTableIdentifierLevels(sparkTableIdentifier);
    String table = levels[levels.length - 1];
    String schema = String.join(schemaSeparator, Arrays.copyOf(levels, levels.length - 1));
    return NameIdentifier.of(catalogName, schema, table);
  }

  /**
   * Splits a Spark table identifier into namespace and table levels.
   *
   * @param sparkTableIdentifier dotted Spark table id
   * @return at least two non-blank levels
   */
  public static String[] sparkTableIdentifierLevels(String sparkTableIdentifier) {
    Preconditions.checkArgument(
        StringUtils.isNotBlank(sparkTableIdentifier), "sparkTableIdentifier must not be blank");
    String[] levels = sparkTableIdentifier.split("\\.", -1);
    Preconditions.checkArgument(
        levels.length >= 2,
        "spark table identifier must have at least schema.table, but got: %s",
        sparkTableIdentifier);
    for (String level : levels) {
      Preconditions.checkArgument(
          StringUtils.isNotBlank(level),
          "spark table identifier contains an empty level: %s",
          sparkTableIdentifier);
    }
    return levels;
  }

  /**
   * Parse table identifier text.
   *
   * <p>Accepted forms:
   *
   * <ul>
   *   <li>{@code schema.table}, when default catalog is configured
   *   <li>{@code catalog.schema.table}
   * </ul>
   *
   * @param identifierText raw identifier text
   * @param defaultCatalogName optional default catalog for {@code schema.table}
   * @return parsed identifier
   */
  public static Optional<NameIdentifier> parseTableIdentifier(
      String identifierText, String defaultCatalogName) {
    if (StringUtils.isBlank(identifierText)) {
      return Optional.empty();
    }

    try {
      NameIdentifier parsed = NameIdentifier.parse(identifierText);
      int levels = parsed.namespace().levels().length;
      if (levels == 0) {
        return Optional.empty();
      }
      if (levels == 1) {
        if (StringUtils.isNotBlank(defaultCatalogName)) {
          return Optional.of(
              NameIdentifier.of(defaultCatalogName, parsed.namespace().levels()[0], parsed.name()));
        }
        return Optional.empty();
      }
      if (levels == 2) {
        return Optional.of(parsed);
      }
      return Optional.empty();
    } catch (Exception e) {
      return Optional.empty();
    }
  }

  /**
   * Parse job identifier text.
   *
   * @param identifierText raw identifier text
   * @return parsed identifier
   */
  public static Optional<NameIdentifier> parseJobIdentifier(String identifierText) {
    if (StringUtils.isBlank(identifierText)) {
      return Optional.empty();
    }

    try {
      return Optional.of(NameIdentifier.parse(identifierText));
    } catch (Exception e) {
      return Optional.empty();
    }
  }
}
