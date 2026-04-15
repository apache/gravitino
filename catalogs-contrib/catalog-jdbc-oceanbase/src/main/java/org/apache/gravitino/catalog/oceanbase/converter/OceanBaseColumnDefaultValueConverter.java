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
package org.apache.gravitino.catalog.oceanbase.converter;

import static org.apache.gravitino.rel.Column.DEFAULT_VALUE_NOT_SET;
import static org.apache.gravitino.rel.Column.DEFAULT_VALUE_OF_CURRENT_TIMESTAMP;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Objects;
import java.util.regex.Pattern;
import org.apache.gravitino.catalog.jdbc.converter.JdbcColumnDefaultValueConverter;
import org.apache.gravitino.catalog.jdbc.converter.JdbcTypeConverter;
import org.apache.gravitino.rel.expressions.Expression;
import org.apache.gravitino.rel.expressions.UnparsedExpression;
import org.apache.gravitino.rel.expressions.literals.Literals;
import org.apache.gravitino.rel.types.Decimal;
import org.apache.gravitino.rel.types.Types;

public class OceanBaseColumnDefaultValueConverter extends JdbcColumnDefaultValueConverter {

  // match CURRENT_TIMESTAMP or CURRENT_TIMESTAMP(fsp)
  private static final Pattern CURRENT_TIMESTAMP =
      Pattern.compile("^CURRENT_TIMESTAMP(\\(\\d+\\))?$");

  @Override
  public Expression toGravitino(
      JdbcTypeConverter.JdbcTypeBean type,
      String columnDefaultValue,
      boolean isExpression,
      boolean nullable) {
    if (Objects.isNull(columnDefaultValue)) {
      return nullable ? Literals.NULL : DEFAULT_VALUE_NOT_SET;
    }

    if (columnDefaultValue.equalsIgnoreCase(NULL)) {
      return Literals.NULL;
    }

    if (isExpression) {
      if (CURRENT_TIMESTAMP.matcher(columnDefaultValue).matches()) {
        return DEFAULT_VALUE_OF_CURRENT_TIMESTAMP;
      }
      // The parsing of OceanBase expressions is complex, so we are not currently undertaking the
      // parsing.
      return UnparsedExpression.of(columnDefaultValue);
    }

    switch (type.getTypeName().toLowerCase()) {
      case OceanBaseTypeConverter.TINYINT:
        return Literals.byteLiteral(Byte.valueOf(columnDefaultValue));
      case OceanBaseTypeConverter.TINYINT_UNSIGNED:
        return Literals.unsignedByteLiteral(Short.valueOf(columnDefaultValue));
      case OceanBaseTypeConverter.SMALLINT:
        return Literals.shortLiteral(Short.valueOf(columnDefaultValue));
      case OceanBaseTypeConverter.SMALLINT_UNSIGNED:
        return Literals.unsignedShortLiteral(Integer.valueOf(columnDefaultValue));
      case OceanBaseTypeConverter.INT:
        return Literals.integerLiteral(Integer.valueOf(columnDefaultValue));
      case OceanBaseTypeConverter.INT_UNSIGNED:
        return Literals.unsignedIntegerLiteral(Long.valueOf(columnDefaultValue));
      case OceanBaseTypeConverter.BIGINT:
        return Literals.longLiteral(Long.valueOf(columnDefaultValue));
      case OceanBaseTypeConverter.BIGINT_UNSIGNED:
        return Literals.unsignedLongLiteral(Decimal.of(columnDefaultValue));
      case OceanBaseTypeConverter.FLOAT:
        return Literals.floatLiteral(Float.valueOf(columnDefaultValue));
      case OceanBaseTypeConverter.DOUBLE:
        return Literals.doubleLiteral(Double.valueOf(columnDefaultValue));
      case OceanBaseTypeConverter.NUMBER:
      case OceanBaseTypeConverter.NUMERIC:
      case OceanBaseTypeConverter.DECIMAL:
        return Literals.decimalLiteral(
            Decimal.of(columnDefaultValue, type.getColumnSize(), type.getScale()));
      case JdbcTypeConverter.DATE:
        return Literals.dateLiteral(LocalDate.parse(columnDefaultValue, DATE_FORMATTER));
      case JdbcTypeConverter.TIME:
        return Literals.timeLiteral(LocalTime.parse(columnDefaultValue, TIME_FORMATTER));
      case JdbcTypeConverter.TIMESTAMP:
      case OceanBaseTypeConverter.DATETIME:
        return CURRENT_TIMESTAMP.matcher(columnDefaultValue).matches()
            ? DEFAULT_VALUE_OF_CURRENT_TIMESTAMP
            : Literals.timestampLiteral(
                LocalDateTime.parse(columnDefaultValue, DATE_TIME_FORMATTER));
      case JdbcTypeConverter.VARCHAR:
        return Literals.of(columnDefaultValue, Types.VarCharType.of(type.getColumnSize()));
      case OceanBaseTypeConverter.CHAR:
        return Literals.of(columnDefaultValue, Types.FixedCharType.of(type.getColumnSize()));
      case JdbcTypeConverter.TEXT:
        return Literals.stringLiteral(columnDefaultValue);
      default:
        return UnparsedExpression.of(columnDefaultValue);
    }
  }
}
