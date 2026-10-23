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
package org.apache.gravitino.policy.rego;

import com.google.common.base.Preconditions;
import java.nio.charset.StandardCharsets;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.policy.ReadRestrictionContent;
import org.apache.gravitino.policy.rego.RestrictedRegoProgram.ColumnMask;
import org.apache.gravitino.policy.rego.RestrictedRegoProgram.RowFilter;

/** Validates complete programs in the {@code restricted-rego-v1} source dialect. */
public final class RestrictedRegoExpressionParserFacade {

  private RestrictedRegoExpressionParserFacade() {}

  /**
   * Validates one complete {@code filter := ...} program.
   *
   * @param source row-filter program in {@code restricted-rego-v1}
   * @throws IllegalArgumentException if syntax or semantics are invalid or the rule head is not
   *     {@code filter}
   */
  public static void validateRowFilter(String source) {
    parseRowFilter(source);
  }

  /**
   * Validates one complete {@code mask := action(...)} program.
   *
   * @param source column-mask program in {@code restricted-rego-v1}
   * @throws IllegalArgumentException if syntax or semantics are invalid or the rule head is not
   *     {@code mask}
   */
  public static void validateColumnMask(String source) {
    parseColumnMask(source);
  }

  /**
   * Parses and validates one complete row-filter or column-mask program.
   *
   * @param source program in {@code restricted-rego-v1}
   * @return validated unresolved program
   * @throws IllegalArgumentException if syntax or semantics are invalid
   */
  static RestrictedRegoProgram parse(String source) {
    Preconditions.checkArgument(
        StringUtils.isNotBlank(source), "restricted-rego-v1 program cannot be blank");
    Preconditions.checkArgument(
        source.getBytes(StandardCharsets.UTF_8).length
            <= ReadRestrictionContent.MAX_SOURCE_LENGTH_BYTES,
        "restricted-rego-v1 source must not exceed %s UTF-8 bytes",
        ReadRestrictionContent.MAX_SOURCE_LENGTH_BYTES);

    RestrictedRegoProgram program = new RestrictedRegoSourceParser(source).parseProgram();
    RestrictedRegoSourceValidator.validate(program);
    return program;
  }

  /**
   * Parses and validates one complete {@code filter := ...} program.
   *
   * @param source row-filter program in {@code restricted-rego-v1}
   * @return validated unresolved row-filter rule
   * @throws IllegalArgumentException if syntax or semantics are invalid or the rule head is not
   *     {@code filter}
   */
  static RowFilter parseRowFilter(String source) {
    RestrictedRegoProgram program = parse(source);
    Preconditions.checkArgument(
        program instanceof RowFilter, "row-filter program must declare a filter rule");
    return (RowFilter) program;
  }

  /**
   * Parses and validates one complete {@code mask := action(...)} program.
   *
   * @param source column-mask program in {@code restricted-rego-v1}
   * @return validated unresolved column-mask rule
   * @throws IllegalArgumentException if syntax or semantics are invalid or the rule head is not
   *     {@code mask}
   */
  static ColumnMask parseColumnMask(String source) {
    RestrictedRegoProgram program = parse(source);
    Preconditions.checkArgument(
        program instanceof ColumnMask, "column-mask program must declare a mask rule");
    return (ColumnMask) program;
  }
}
