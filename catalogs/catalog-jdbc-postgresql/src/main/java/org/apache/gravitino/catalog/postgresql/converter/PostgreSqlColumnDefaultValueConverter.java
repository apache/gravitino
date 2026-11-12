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
package org.apache.gravitino.catalog.postgresql.converter;

import static org.apache.gravitino.catalog.jdbc.converter.JdbcTypeConverter.VARCHAR;
import static org.apache.gravitino.rel.Column.DEFAULT_VALUE_NOT_SET;
import static org.apache.gravitino.rel.Column.DEFAULT_VALUE_OF_CURRENT_TIMESTAMP;

import com.google.common.io.BaseEncoding;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.annotation.Nullable;
import org.apache.gravitino.catalog.jdbc.converter.JdbcColumnDefaultValueConverter;
import org.apache.gravitino.catalog.jdbc.converter.JdbcTypeConverter;
import org.apache.gravitino.rel.expressions.Expression;
import org.apache.gravitino.rel.expressions.UnparsedExpression;
import org.apache.gravitino.rel.expressions.literals.Literals;
import org.apache.gravitino.rel.types.Decimal;

public class PostgreSqlColumnDefaultValueConverter extends JdbcColumnDefaultValueConverter {
  private static final String TYPE_MODIFIER = "(?:\\(\\d++(?:,\\s*\\d++)?\\))?";
  // Only constant casts that the literal conversion below understands. An arbitrary suffix would
  // also match operators, COLLATE clauses, or another cast and silently change their semantics.
  private static final String LITERAL_TYPE =
      "(?:(?:text|bpchar|varchar|character(?: varying)?|boolean|bool|smallint|int2|integer|int4|bigint|int8|real|float4|double precision|float8|numeric|decimal|date)"
          + TYPE_MODIFIER
          + "|(?:timestamp|time)"
          + TYPE_MODIFIER
          + "(?: (?:with|without) time zone)?|timestamptz|timetz)";
  // Possessive repetition avoids recursive backtracking for long default literals.
  private static final Pattern QUOTED_LITERAL =
      Pattern.compile(
          "([eE]?)'((?:[^']|'')*+)'(?:\\s*::\\s*" + LITERAL_TYPE + ")?", Pattern.CASE_INSENSITIVE);
  // An unquoted constant on a string column, for example from `varchar(20) DEFAULT (10)` or
  // `text DEFAULT true`. PostgreSQL shows these as 10 or true, and the assignment cast stores the
  // same text. PostgreSQL normalizes other numeric forms, such as 1e3, before showing them; this
  // pattern also excludes them so it never guesses their stored text.
  private static final Pattern STRING_COLUMN_CONSTANT =
      Pattern.compile("\\d++(?:\\.\\d++)?|true|false");
  private static final Pattern NULL_LITERAL =
      Pattern.compile("NULL(?:\\s*::\\s*(.+))?", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

  public PostgreSqlColumnDefaultValueConverter() {}

  /**
   * Makes backslash escaping explicit for a complete quoted default read with PostgreSQL's
   * standard_conforming_strings disabled. PostgreSQL's catalog printer doubles backslashes in that
   * mode without adding an E prefix. Other expressions are preserved without rewriting fragments.
   *
   * @param defaultValue the catalog default expression, or null when absent
   * @return the equivalent explicit escape literal, or the unchanged expression
   */
  @Nullable
  public static String withExplicitBackslashEscapes(@Nullable String defaultValue) {
    return defaultValue != null
            && defaultValue.startsWith("'")
            && defaultValue.indexOf('\\') >= 0
            && QUOTED_LITERAL.matcher(defaultValue).matches()
        ? "E" + defaultValue
        : defaultValue;
  }

  /**
   * Converts a default deparsed with standard_conforming_strings enabled, or a literal with
   * explicit E escaping. Readers using nonstandard strings must first normalize complete quoted
   * literals with {@link #withExplicitBackslashEscapes(String)}. Other expressions remain unparsed.
   *
   * @param type the column's JDBC type
   * @param columnDefaultValue the default expression, or null when absent
   * @param isExpression whether the column is generated from an expression
   * @param nullable whether the column is nullable
   * @return the literal, recognized current-timestamp default, or preserved expression
   */
  @Override
  public Expression toGravitino(
      JdbcTypeConverter.JdbcTypeBean type,
      String columnDefaultValue,
      boolean isExpression,
      boolean nullable) {
    if (columnDefaultValue == null) {
      return nullable ? Literals.NULL : DEFAULT_VALUE_NOT_SET;
    }

    if (isExpression) {
      // The parsing of Postgresql expressions is complex, so we are not currently undertaking the
      // parsing.
      return UnparsedExpression.of(columnDefaultValue);
    }

    // Checked before literal parsing so string columns also report the function, not its name.
    if (columnDefaultValue.equals(CURRENT_TIMESTAMP)) {
      return DEFAULT_VALUE_OF_CURRENT_TIMESTAMP;
    }
    try {
      return parseLiteral(type, columnDefaultValue);
    } catch (Exception e) {
      return UnparsedExpression.of(columnDefaultValue);
    }
  }

  /** {@inheritDoc} */
  @Override
  @Nullable
  public String fromGravitino(Expression defaultValue) {
    String sql = super.fromGravitino(defaultValue);
    if (sql != null && sql.indexOf('\0') >= 0) {
      throw new IllegalArgumentException("PostgreSQL column default must not contain U+0000 (NUL)");
    }
    return sql;
  }

  /** Uses explicit escape strings for backslashes, independently of the server string setting. */
  @Override
  protected String quoteStringLiteral(String value) {
    String escaped = value.replace("'", "''");
    return value.indexOf('\\') >= 0
        ? "E'" + escaped.replace("\\", "\\\\") + "'"
        : "'" + escaped + "'";
  }

  /** {@inheritDoc} */
  @Override
  protected String binaryLiteral(byte[] value) {
    // PostgreSQL's X'...' literal is a bit string, so decode hex explicitly for bytea.
    return "decode('" + BaseEncoding.base16().lowerCase().encode(value) + "', 'hex')";
  }

  private Expression parseLiteral(JdbcTypeConverter.JdbcTypeBean type, String columnDefaultValue) {
    String expression = columnDefaultValue;
    Matcher nullLiteral = NULL_LITERAL.matcher(expression);
    if (nullLiteral.matches()
        && (nullLiteral.group(1) == null
            || PostgreSqlTypeConverter.isSingleTypeDeclaration(nullLiteral.group(1)))) {
      return Literals.NULL;
    }
    Matcher matcher = QUOTED_LITERAL.matcher(expression);
    boolean quoted = matcher.matches();
    if (quoted) {
      // String literals in the catalog are shown with embedded single quotes doubled.
      String content = matcher.group(2).replace("''", "'");
      if (!matcher.group(1).isEmpty()) {
        StringBuilder decoded = new StringBuilder();
        for (int i = 0; i < content.length(); i++) {
          char character = content.charAt(i);
          if (character == '\\') {
            // The renderer and metadata normalization make literal backslash pairs explicit.
            // Preserve other escape syntaxes instead of guessing their decoded value.
            if (++i >= content.length() || content.charAt(i) != '\\') {
              return UnparsedExpression.of(columnDefaultValue);
            }
          }
          decoded.append(character);
        }
        content = decoded.toString();
      }
      columnDefaultValue = content;
    }

    switch (type.getTypeName().toLowerCase(Locale.ROOT)) {
      case PostgreSqlTypeConverter.BOOL:
        if (!"true".equalsIgnoreCase(columnDefaultValue)
            && !"false".equalsIgnoreCase(columnDefaultValue)) {
          return UnparsedExpression.of(expression);
        }
        return Literals.booleanLiteral(Boolean.valueOf(columnDefaultValue));
      case PostgreSqlTypeConverter.INT_2:
        return Literals.shortLiteral(Short.valueOf(columnDefaultValue));
      case PostgreSqlTypeConverter.INT_4:
        return Literals.integerLiteral(Integer.valueOf(columnDefaultValue));
      case PostgreSqlTypeConverter.INT_8:
        return Literals.longLiteral(Long.valueOf(columnDefaultValue));
      case PostgreSqlTypeConverter.FLOAT_4:
        return Literals.floatLiteral(Float.valueOf(columnDefaultValue));
      case PostgreSqlTypeConverter.FLOAT_8:
        return Literals.doubleLiteral(Double.valueOf(columnDefaultValue));
      case PostgreSqlTypeConverter.NUMERIC:
        return Literals.decimalLiteral(
            Decimal.of(columnDefaultValue, type.getColumnSize(), type.getScale()));
      case JdbcTypeConverter.DATE:
        return Literals.dateLiteral(LocalDate.parse(columnDefaultValue, DATE_FORMATTER));
      case JdbcTypeConverter.TIME:
        return Literals.timeLiteral(LocalTime.parse(columnDefaultValue, TIME_FORMATTER));
      case JdbcTypeConverter.TIMESTAMP:
      case PostgreSqlTypeConverter.TIMESTAMP_TZ:
        return Literals.timestampLiteral(
            LocalDateTime.parse(columnDefaultValue, DATE_TIME_FORMATTER));
      case VARCHAR:
        if (!quoted && !STRING_COLUMN_CONSTANT.matcher(expression).matches()) {
          return UnparsedExpression.of(expression);
        }
        Integer length = type.getColumnSize();
        return length == null || length <= 0 || length == Integer.MAX_VALUE
            ? Literals.stringLiteral(columnDefaultValue)
            : Literals.varcharLiteral(length, columnDefaultValue);
      case PostgreSqlTypeConverter.BPCHAR:
      case JdbcTypeConverter.TEXT:
        return quoted || STRING_COLUMN_CONSTANT.matcher(expression).matches()
            ? Literals.stringLiteral(columnDefaultValue)
            : UnparsedExpression.of(expression);
      default:
        throw new IllegalArgumentException("Unknown data type for literal: " + type);
    }
  }
}
