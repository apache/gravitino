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
package org.apache.gravitino.catalog;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.commons.lang3.StringUtils;

/**
 * Utility class for hierarchical schema name conversions.
 *
 * <p>Gravitino supports nested namespace semantics where a logical schema name like {@code A:B:C}
 * (using a configurable external separator, default {@code :}) is mapped to a physical schema name
 * {@code A.B.C} stored in {@code EntityStore} using {@code .} as the internal separator.
 *
 * <p>Schema names in {@code EntityStore} never contain the external separator; the external
 * separator is only used at the API boundary and in catalog capability validation.
 */
public final class HierarchicalSchemaUtil {

  /** The internal separator used in EntityStore for nested schema names. */
  private static final String PHYSICAL_SEPARATOR = ".";

  private HierarchicalSchemaUtil() {}

  /**
   * Converts a logical schema path (using the external separator) to a physical schema name (using
   * {@code .} as separator) suitable for storage in EntityStore.
   *
   * <p>Example: {@code "A:B:C"} with separator {@code ":"} → {@code "A.B.C"}
   *
   * @param logicalPath the logical schema path using the external separator
   * @param separator the external separator configured on the server
   * @return the physical schema name using {@code .} as separator
   */
  public static String logicalToPhysical(String logicalPath, String separator) {
    Preconditions.checkArgument(StringUtils.isNotBlank(logicalPath), "logicalPath must not be blank");
    Preconditions.checkArgument(StringUtils.isNotBlank(separator), "separator must not be blank");
    return logicalPath.replace(separator, PHYSICAL_SEPARATOR);
  }

  /**
   * Converts a physical schema name (using {@code .} as separator) back to the logical schema path
   * using the configured external separator.
   *
   * <p>Example: {@code "A.B.C"} with separator {@code ":"} → {@code "A:B:C"}
   *
   * @param physicalName the physical schema name using {@code .} as separator
   * @param separator the external separator configured on the server
   * @return the logical schema path using the external separator
   */
  public static String physicalToLogical(String physicalName, String separator) {
    Preconditions.checkArgument(StringUtils.isNotBlank(physicalName), "physicalName must not be blank");
    Preconditions.checkArgument(StringUtils.isNotBlank(separator), "separator must not be blank");
    return physicalName.replace(PHYSICAL_SEPARATOR, separator);
  }

  /**
   * Returns whether a schema name is a nested path (contains the external separator).
   *
   * @param name the schema name to test
   * @param separator the external separator
   * @return {@code true} if the name contains the external separator
   */
  public static boolean isNested(String name, String separator) {
    return StringUtils.isNotBlank(name) && name.contains(separator);
  }

  /**
   * Returns all ancestor schema names of the given schema name, ordered from outermost to innermost
   * (but excluding the name itself). Returns an empty list for top-level (non-nested) schemas.
   *
   * <p>Example: {@code "A:B:C"} with separator {@code ":"} → {@code ["A", "A:B"]}
   *
   * @param schemaName the schema name to find ancestors for
   * @param separator the external separator
   * @return ancestor names from outermost to innermost, or empty list if not nested
   */
  public static List<String> getAncestorNames(String schemaName, String separator) {
    Preconditions.checkArgument(StringUtils.isNotBlank(schemaName), "schemaName must not be blank");
    Preconditions.checkArgument(StringUtils.isNotBlank(separator), "separator must not be blank");
    String[] parts = schemaName.split(java.util.regex.Pattern.quote(separator), -1);
    List<String> ancestors = new ArrayList<>();
    for (int i = 1; i < parts.length; i++) {
      ancestors.add(String.join(separator, java.util.Arrays.copyOf(parts, i)));
    }
    return ancestors;
  }

  /**
   * Returns the schema name and all its ancestor schema names, ordered from the schema itself to
   * the outermost ancestor. Returns a single-element list for top-level (non-nested) schemas.
   *
   * <p>Example: {@code "A:B:C"} with separator {@code ":"} → {@code ["A:B:C", "A:B", "A"]}
   *
   * <p>This is used for privilege inheritance: a privilege on an ancestor schema is inherited by
   * all descendant schemas.
   *
   * @param schemaName the schema name to compute scopes for
   * @param separator the external separator
   * @return the schema name and all ancestor names, from most specific to outermost
   */
  public static List<String> allScopes(String schemaName, String separator) {
    Preconditions.checkArgument(StringUtils.isNotBlank(schemaName), "schemaName must not be blank");
    Preconditions.checkArgument(StringUtils.isNotBlank(separator), "separator must not be blank");
    List<String> result = new ArrayList<>();
    result.add(schemaName);
    String current = schemaName;
    while (current.contains(separator)) {
      int lastIdx = current.lastIndexOf(separator);
      current = current.substring(0, lastIdx);
      result.add(current);
    }
    return result;
  }

  /**
   * Given a list of schema names and an optional parent, returns only the direct children of that
   * parent (one level deeper). When {@code parent} is empty, returns top-level schemas (names that
   * do not contain the separator).
   *
   * <p>Example: schemas {@code ["A", "A:B", "A:B:C", "B"]}, separator {@code ":"}, parent {@code
   * Optional.of("A:B")} → {@code ["A:B:C"]}
   *
   * @param allNames all schema names in the catalog
   * @param parent the parent schema name, or {@link Optional#empty()} for top-level
   * @param separator the separator used in the schema names
   * @return direct children names
   */
  public static List<String> filterDirectChildren(
      List<String> allNames, Optional<String> parent, String separator) {
    List<String> result = new ArrayList<>();
    if (!parent.isPresent()) {
      for (String name : allNames) {
        if (!name.contains(separator)) {
          result.add(name);
        }
      }
    } else {
      String prefix = parent.get() + separator;
      for (String name : allNames) {
        if (name.startsWith(prefix)) {
          String remainder = name.substring(prefix.length());
          if (!remainder.contains(separator)) {
            result.add(name);
          }
        }
      }
    }
    return result;
  }
}
