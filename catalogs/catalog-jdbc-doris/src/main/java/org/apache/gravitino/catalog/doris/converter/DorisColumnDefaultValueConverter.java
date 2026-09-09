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
package org.apache.gravitino.catalog.doris.converter;

import static org.apache.gravitino.catalog.doris.converter.DorisTypeConverter.BIGINT;
import static org.apache.gravitino.catalog.doris.converter.DorisTypeConverter.CHAR;
import static org.apache.gravitino.catalog.doris.converter.DorisTypeConverter.DATETIME;
import static org.apache.gravitino.catalog.doris.converter.DorisTypeConverter.DECIMAL;
import static org.apache.gravitino.catalog.doris.converter.DorisTypeConverter.DOUBLE;
import static org.apache.gravitino.catalog.doris.converter.DorisTypeConverter.FLOAT;
import static org.apache.gravitino.catalog.doris.converter.DorisTypeConverter.INT;
import static org.apache.gravitino.catalog.doris.converter.DorisTypeConverter.SMALLINT;
import static org.apache.gravitino.catalog.doris.converter.DorisTypeConverter.STRING;
import static org.apache.gravitino.catalog.doris.converter.DorisTypeConverter.TINYINT;
import static org.apache.gravitino.rel.Column.DEFAULT_VALUE_NOT_SET;
import static org.apache.gravitino.rel.Column.DEFAULT_VALUE_OF_CURRENT_TIMESTAMP;

import java.time.LocalDate;
import java.time.LocalDateTime;
import javax.annotation.Nullable;
import org.apache.gravitino.catalog.jdbc.converter.JdbcColumnDefaultValueConverter;
import org.apache.gravitino.catalog.jdbc.converter.JdbcTypeConverter;
import org.apache.gravitino.rel.expressions.Expression;
import org.apache.gravitino.rel.expressions.UnparsedExpression;
import org.apache.gravitino.rel.expressions.literals.Literal;
import org.apache.gravitino.rel.expressions.literals.Literals;
import org.apache.gravitino.rel.types.Decimal;
import org.apache.gravitino.rel.types.Types;

public class DorisColumnDefaultValueConverter extends JdbcColumnDefaultValueConverter {

  /**
   * Converts a Doris default value for a full column definition.
   *
   * <p>Doris expects string defaults in the quoted form for column modification. Unparsed values
   * are already native Doris expressions returned by metadata and must be passed through without
   * attempting to interpret them in Gravitino.
   *
   * @param defaultValue the loaded Gravitino default value
   * @param doubleEscapeBackslashes whether Doris requires the additional backslash escaping layer
   * @param tripleEscapeQuotes whether Doris 3.x MODIFY COLUMN requires three backslashes before
   *     embedded double quotes
   * @return the Doris SQL representation of the default value
   */
  @Nullable
  public String fromGravitinoForColumnDefinition(
      Expression defaultValue, boolean doubleEscapeBackslashes, boolean tripleEscapeQuotes) {
    if (DEFAULT_VALUE_NOT_SET.equals(defaultValue)) {
      return null;
    }

    if (defaultValue instanceof UnparsedExpression) {
      return ((UnparsedExpression) defaultValue).unparsedExpression();
    }

    if (defaultValue instanceof Literal) {
      Literal<?> literal = (Literal<?>) defaultValue;
      if (literal.dataType() instanceof Types.StringType
          || literal.dataType() instanceof Types.VarCharType
          || literal.dataType() instanceof Types.FixedCharType) {
        String value = String.valueOf(literal.value());
        if (value.indexOf('\\') >= 0 || value.indexOf('\'') >= 0 || value.indexOf('"') >= 0) {
          return quoteDorisLiteral(value, doubleEscapeBackslashes, tripleEscapeQuotes);
        }
      }
    }

    return super.fromGravitino(defaultValue);
  }

  /**
   * Converts a default value for a CREATE TABLE column definition.
   *
   * <p>CREATE TABLE accepts the same Doris-compatible literal encoding, but unparsed expressions
   * are not accepted from client-created table definitions. They are reserved for native
   * expressions read from existing Doris metadata and replayed during MODIFY COLUMN.
   *
   * @param defaultValue the Gravitino default value
   * @return the Doris SQL representation of the default value
   */
  @Nullable
  public String fromGravitinoForCreateTableDefinition(Expression defaultValue) {
    if (defaultValue instanceof UnparsedExpression) {
      throw new IllegalArgumentException(
          "Unparsed default expressions are not supported in CREATE TABLE definitions");
    }
    return fromGravitinoForColumnDefinition(defaultValue, false, false);
  }

  @Override
  public Expression toGravitino(
      JdbcTypeConverter.JdbcTypeBean columnType,
      String columnDefaultValue,
      boolean isExpression,
      boolean nullable) {
    if (columnDefaultValue == null) {
      return nullable ? Literals.NULL : DEFAULT_VALUE_NOT_SET;
    }

    if (columnDefaultValue.equalsIgnoreCase(NULL)) {
      return Literals.NULL;
    }

    if (isExpression) {
      if (columnDefaultValue.equals(CURRENT_TIMESTAMP)) {
        return DEFAULT_VALUE_OF_CURRENT_TIMESTAMP;
      }
      // The parsing of Doris expressions is complex, so we are not currently undertaking the
      // parsing.
      return UnparsedExpression.of(columnDefaultValue);
    }

    switch (columnType.getTypeName().toLowerCase()) {
      case TINYINT:
        return Literals.byteLiteral(Byte.valueOf(columnDefaultValue));
      case SMALLINT:
        return Literals.shortLiteral(Short.valueOf(columnDefaultValue));
      case INT:
        return Literals.integerLiteral(Integer.valueOf(columnDefaultValue));
      case BIGINT:
        return Literals.longLiteral(Long.valueOf(columnDefaultValue));
      case FLOAT:
        return Literals.floatLiteral(Float.valueOf(columnDefaultValue));
      case DOUBLE:
        return Literals.doubleLiteral(Double.valueOf(columnDefaultValue));
      case DECIMAL:
        return Literals.decimalLiteral(
            Decimal.of(columnDefaultValue, columnType.getColumnSize(), columnType.getScale()));
      case JdbcTypeConverter.DATE:
        return Literals.dateLiteral(LocalDate.parse(columnDefaultValue, DATE_FORMATTER));
      case JdbcTypeConverter.TIMESTAMP:
      case DATETIME:
        return CURRENT_TIMESTAMP.equals(columnDefaultValue)
            ? DEFAULT_VALUE_OF_CURRENT_TIMESTAMP
            : Literals.timestampLiteral(
                LocalDateTime.parse(columnDefaultValue, DATE_TIME_FORMATTER));
      case JdbcTypeConverter.VARCHAR:
        return Literals.of(
            unescapeDorisLiteral(columnDefaultValue),
            Types.VarCharType.of(columnType.getColumnSize()));
      case CHAR:
        return Literals.of(
            unescapeDorisLiteral(columnDefaultValue),
            Types.FixedCharType.of(columnType.getColumnSize()));
      case STRING:
      case JdbcTypeConverter.TEXT:
        return Literals.stringLiteral(unescapeDorisLiteral(columnDefaultValue));
      default:
        return UnparsedExpression.of(columnDefaultValue);
    }
  }

  private static String quoteDorisLiteral(
      String value, boolean doubleEscapeBackslashes, boolean tripleEscapeQuotes) {
    String escapedBackslash = doubleEscapeBackslashes ? "\\".repeat(4) : "\\".repeat(2);
    String escapedQuote = "\\".repeat(tripleEscapeQuotes ? 3 : 1) + "\"";
    String escaped = value.replace("\\", escapedBackslash).replace("\"", escapedQuote);
    return "\"" + escaped + "\"";
  }

  private static String unescapeDorisLiteral(String value) {
    StringBuilder result = new StringBuilder(value.length());
    for (int i = 0; i < value.length(); i++) {
      char current = value.charAt(i);
      if (current == '\\' && i + 1 < value.length()) {
        char next = value.charAt(i + 1);
        if (next == '\\' || next == '\'' || next == '\"') {
          result.append(next);
          i++;
          continue;
        }
      } else if ((current == '\'' || current == '\"')
          && i + 1 < value.length()
          && value.charAt(i + 1) == current) {
        result.append(current);
        i++;
        continue;
      }
      result.append(current);
    }
    return result.toString();
  }
}
