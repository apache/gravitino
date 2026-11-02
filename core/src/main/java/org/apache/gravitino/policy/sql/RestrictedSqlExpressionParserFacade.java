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
package org.apache.gravitino.policy.sql;

import java.nio.charset.StandardCharsets;
import org.apache.gravitino.policy.ReadRestrictionContent;

/** Parses and validates typed definitions in the {@code restricted-sql-v1} source dialect. */
public final class RestrictedSqlExpressionParserFacade {
  private RestrictedSqlExpressionParserFacade() {}

  /**
   * Validates a complete SQL row-filter definition with a BOOLEAN result.
   *
   * @param source parameter declarations, RETURNS type, arrow and SQL body
   * @throws IllegalArgumentException if syntax, types or resource limits are invalid
   */
  public static void validateRowFilter(String source) {
    parse(source, false);
  }

  /**
   * Validates a complete SQL mask definition with one parameter and the same result type.
   *
   * @param source parameter declaration, RETURNS type, arrow and SQL body
   * @throws IllegalArgumentException if syntax, types or resource limits are invalid
   */
  public static void validateColumnMask(String source) {
    parse(source, true);
  }

  /**
   * Parses and validates a complete SQL row-filter definition.
   *
   * @param source complete typed SQL definition
   * @return immutable unresolved row filter
   * @throws IllegalArgumentException if syntax, semantics or resource limits are invalid
   */
  public static SqlDefinition parseRowFilter(String source) {
    return parse(source, false);
  }

  /**
   * Parses and validates a complete SQL column-mask definition.
   *
   * @param source complete typed SQL definition
   * @return immutable unresolved column mask
   * @throws IllegalArgumentException if syntax, semantics or resource limits are invalid
   */
  public static SqlDefinition parseColumnMask(String source) {
    return parse(source, true);
  }

  static SqlDefinition parse(String source, boolean mask) {
    if (source == null || source.trim().isEmpty()) {
      throw new IllegalArgumentException("restricted-sql-v1 definition cannot be blank");
    }
    if (source.getBytes(StandardCharsets.UTF_8).length
        > ReadRestrictionContent.MAX_SOURCE_LENGTH_BYTES) {
      throw new IllegalArgumentException(
          "restricted-sql-v1 source must not exceed 16384 UTF-8 bytes");
    }
    return RestrictedSqlSourceParser.parse(source, mask);
  }
}
