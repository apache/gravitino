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
package org.apache.gravitino.trino.connector.util;

import io.trino.spi.TrinoException;
import io.trino.spi.type.Type;
import java.util.Arrays;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.gravitino.function.Function;
import org.apache.gravitino.function.FunctionDefinition;
import org.apache.gravitino.trino.connector.GravitinoErrorCode;

/**
 * Builds the SQL routine specification Trino expects for a language function from a Gravitino
 * function definition and its stored SQL body:
 *
 * <pre>FUNCTION name(params) RETURNS type [NOT] DETERMINISTIC SECURITY INVOKER body</pre>
 *
 * <p>The stored body is either a bare expression, which is wrapped in a {@code RETURN} statement,
 * or a control statement ({@code RETURN ...} / {@code BEGIN ... END}) that is used as-is. The form
 * is decided by the first token of the body; since {@code return}, {@code begin} and {@code
 * function} are also valid identifiers, a parameter with one of these names shadows the keyword and
 * the body is treated as an expression. {@code SECURITY INVOKER} is always declared because the
 * function has no owner identity for Trino's {@code SECURITY DEFINER} default.
 *
 * <p>Identifiers are always quoted. Trino resolves routine and parameter names case-insensitively
 * regardless of quoting, so this is equivalent to plain identifiers while also covering reserved
 * words and names with special characters.
 */
public final class TrinoRoutineSpecification {

  private TrinoRoutineSpecification() {}

  /**
   * Builds the routine specification for one definition of a function.
   *
   * @param function the Gravitino function
   * @param definition the definition whose parameters and return type describe the routine
   * @param sql the stored SQL body
   * @param typeTransformer converts Gravitino types to Trino types
   * @return the complete routine specification
   * @throws TrinoException if the definition or body is not supported or a type cannot be mapped
   */
  public static String build(
      Function function,
      FunctionDefinition definition,
      String sql,
      GeneralDataTypeTransformer typeTransformer) {
    if (definition.returnType() == null) {
      throw new TrinoException(
          GravitinoErrorCode.GRAVITINO_ILLEGAL_ARGUMENT,
          "Function " + function.name() + " has a definition without a return type");
    }
    Set<String> parameterNames =
        Arrays.stream(definition.parameters())
            .map(param -> param.name().toLowerCase(Locale.ENGLISH))
            .collect(Collectors.toSet());
    String body = stripLeadingComments(sql);
    if (startsWithKeyword(body, "FUNCTION", parameterNames)) {
      throw new TrinoException(
          GravitinoErrorCode.GRAVITINO_ILLEGAL_ARGUMENT,
          "The SQL body of function "
              + function.name()
              + " must be an expression or a RETURN/BEGIN statement, not a full FUNCTION"
              + " specification");
    }

    String parameters =
        Arrays.stream(definition.parameters())
            .map(
                param ->
                    quoteIdentifier(param.name())
                        + " "
                        + formatType(typeTransformer.getTrinoType(param.dataType())))
            .collect(Collectors.joining(", ", "(", ")"));
    String statement =
        startsWithKeyword(body, "RETURN", parameterNames)
                || startsWithKeyword(body, "BEGIN", parameterNames)
            ? body
            : "RETURN " + body;
    return "FUNCTION "
        + quoteIdentifier(function.name())
        + parameters
        + " RETURNS "
        + formatType(typeTransformer.getTrinoType(definition.returnType()))
        + (function.deterministic() ? " DETERMINISTIC" : " NOT DETERMINISTIC")
        + " SECURITY INVOKER "
        + statement;
  }

  private static String quoteIdentifier(String name) {
    return "\"" + name.replace("\"", "\"\"") + "\"";
  }

  // Unlike Type#getDisplayName(), the type signature quotes row field names.
  private static String formatType(Type type) {
    return type.getTypeSignature().toString();
  }

  /** Removes leading whitespace and SQL comments so the first token can be inspected. */
  static String stripLeadingComments(String sql) {
    String body = sql.trim();
    while (true) {
      if (body.startsWith("--")) {
        int end = body.indexOf('\n');
        body = end < 0 ? "" : body.substring(end + 1).trim();
      } else if (body.startsWith("/*")) {
        int end = body.indexOf("*/", 2);
        body = end < 0 ? "" : body.substring(end + 2).trim();
      } else {
        return body;
      }
    }
  }

  private static boolean startsWithKeyword(String sql, String keyword, Set<String> parameterNames) {
    return sql.regionMatches(true, 0, keyword, 0, keyword.length())
        && (sql.length() == keyword.length() || !isIdentifierChar(sql.charAt(keyword.length())))
        && !parameterNames.contains(keyword.toLowerCase(Locale.ENGLISH));
  }

  private static boolean isIdentifierChar(char c) {
    return Character.isLetterOrDigit(c) || c == '_';
  }
}
