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
package org.apache.gravitino.catalog.mysql.converter;

import static org.apache.gravitino.rel.Column.DEFAULT_VALUE_NOT_SET;
import static org.apache.gravitino.rel.Column.DEFAULT_VALUE_OF_CURRENT_TIMESTAMP;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeParseException;
import org.apache.gravitino.catalog.jdbc.converter.JdbcColumnDefaultValueConverter;
import org.apache.gravitino.catalog.jdbc.converter.JdbcTypeConverter;
import org.apache.gravitino.rel.expressions.Expression;
import org.apache.gravitino.rel.expressions.UnparsedExpression;
import org.apache.gravitino.rel.expressions.literals.Literals;
import org.apache.gravitino.rel.types.Decimal;
import org.apache.gravitino.rel.types.Types;

public class MysqlColumnDefaultValueConverter extends JdbcColumnDefaultValueConverter {

  @Override
  public Expression toGravitino(
      JdbcTypeConverter.JdbcTypeBean type,
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
      if (columnDefaultValue.startsWith(CURRENT_TIMESTAMP)) {
        return DEFAULT_VALUE_OF_CURRENT_TIMESTAMP;
      }
      // The parsing of MySQL expressions is complex, so we are not currently undertaking the
      // parsing.
      return UnparsedExpression.of(columnDefaultValue);
    }

    switch (type.getTypeName().toLowerCase()) {
      case MysqlTypeConverter.TINYINT:
        return Literals.byteLiteral(Byte.valueOf(columnDefaultValue));
      case MysqlTypeConverter.TINYINT_UNSIGNED:
        return Literals.unsignedByteLiteral(Short.valueOf(columnDefaultValue));
      case MysqlTypeConverter.SMALLINT:
        return Literals.shortLiteral(Short.valueOf(columnDefaultValue));
      case MysqlTypeConverter.SMALLINT_UNSIGNED:
        return Literals.unsignedShortLiteral(Integer.valueOf(columnDefaultValue));
      case MysqlTypeConverter.INT:
        return Literals.integerLiteral(Integer.valueOf(columnDefaultValue));
      case MysqlTypeConverter.INT_UNSIGNED:
        return Literals.unsignedIntegerLiteral(Long.valueOf(columnDefaultValue));
      case MysqlTypeConverter.BIGINT:
        return Literals.longLiteral(Long.valueOf(columnDefaultValue));
      case MysqlTypeConverter.BIGINT_UNSIGNED:
        return Literals.unsignedLongLiteral(Decimal.of(columnDefaultValue));
      case MysqlTypeConverter.FLOAT:
        return Literals.floatLiteral(Float.valueOf(columnDefaultValue));
      case MysqlTypeConverter.DOUBLE:
        return Literals.doubleLiteral(Double.valueOf(columnDefaultValue));
      case MysqlTypeConverter.DECIMAL_UNSIGNED:
      case MysqlTypeConverter.DECIMAL:
        return Literals.decimalLiteral(
            Decimal.of(columnDefaultValue, type.getColumnSize(), type.getScale()));
      case JdbcTypeConverter.DATE:
        return Literals.dateLiteral(LocalDate.parse(columnDefaultValue, DATE_FORMATTER));
      case JdbcTypeConverter.TIME:
        return Literals.timeLiteral(LocalTime.parse(columnDefaultValue, TIME_FORMATTER));
      case JdbcTypeConverter.TIMESTAMP:
      case MysqlTypeConverter.DATETIME:
        if (columnDefaultValue.startsWith(CURRENT_TIMESTAMP)) {
          return DEFAULT_VALUE_OF_CURRENT_TIMESTAMP;
        }
        try {
          return Literals.timestampLiteral(
              LocalDateTime.parse(columnDefaultValue, DATE_TIME_FORMATTER));
        } catch (DateTimeParseException e) {
          throw new IllegalArgumentException(
              String.format("Unable to parse datetime value: %s", columnDefaultValue));
        }
      case JdbcTypeConverter.VARCHAR:
        return Literals.of(columnDefaultValue, Types.VarCharType.of(type.getColumnSize()));
      case MysqlTypeConverter.CHAR:
        return Literals.of(columnDefaultValue, Types.FixedCharType.of(type.getColumnSize()));
      case JdbcTypeConverter.TEXT:
        return Literals.stringLiteral(columnDefaultValue);
      default:
        return UnparsedExpression.of(columnDefaultValue);
    }
  }
}
