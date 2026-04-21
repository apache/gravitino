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
  public static final String PHYSICAL_SEPARATOR = ".";

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
    if (!logicalPath.contains(separator)) {
      return logicalPath;
    }
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
    if (!physicalName.contains(PHYSICAL_SEPARATOR)) {
      return physicalName;
    }
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
   * Returns whether a schema name represents a nested physical path (contains {@code .}).
   *
   * @param physicalName the physical schema name
   * @return {@code true} if the name contains {@code .}
   */
  public static boolean isPhysicalNested(String physicalName) {
    return StringUtils.isNotBlank(physicalName) && physicalName.contains(PHYSICAL_SEPARATOR);
  }

  /**
   * Given a list of physical schema names (using {@code .}) and a physical parent prefix, returns
   * only the direct children of that parent (one level deeper).
   *
   * <p>Example: schemas {@code ["A", "A.B", "A.B.C", "B"]}, parent {@code "A.B"} → {@code
   * ["A.B.C"]}
   *
   * @param allPhysicalNames all physical schema names in the catalog
   * @param physicalParent the physical parent schema name (or empty/null for top-level)
   * @return direct children physical names
   */
  public static List<String> filterDirectChildren(
      List<String> allPhysicalNames, String physicalParent) {
    List<String> result = new ArrayList<>();
    if (StringUtils.isBlank(physicalParent)) {
      for (String name : allPhysicalNames) {
        if (!name.contains(PHYSICAL_SEPARATOR)) {
          result.add(name);
        }
      }
    } else {
      String prefix = physicalParent + PHYSICAL_SEPARATOR;
      for (String name : allPhysicalNames) {
        if (name.startsWith(prefix)) {
          String remainder = name.substring(prefix.length());
          if (!remainder.contains(PHYSICAL_SEPARATOR)) {
            result.add(name);
          }
        }
      }
    }
    return result;
  }
}
