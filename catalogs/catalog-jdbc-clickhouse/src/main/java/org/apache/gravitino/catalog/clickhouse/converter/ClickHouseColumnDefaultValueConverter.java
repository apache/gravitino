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
package org.apache.gravitino.catalog.clickhouse.converter;

import static org.apache.gravitino.rel.Column.DEFAULT_VALUE_NOT_SET;
import static org.apache.gravitino.rel.Column.DEFAULT_VALUE_OF_CURRENT_TIMESTAMP;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;

import org.apache.gravitino.catalog.jdbc.converter.JdbcColumnDefaultValueConverter;
import org.apache.gravitino.catalog.jdbc.converter.JdbcTypeConverter;
import org.apache.gravitino.rel.expressions.Expression;
import org.apache.gravitino.rel.expressions.UnparsedExpression;
import org.apache.gravitino.rel.expressions.literals.Literals;
import org.apache.gravitino.rel.types.Decimal;
import org.apache.gravitino.rel.types.Types;

public class ClickHouseColumnDefaultValueConverter extends JdbcColumnDefaultValueConverter {

  @Override
  public Expression toGravitino(
      JdbcTypeConverter.JdbcTypeBean type,
      String columnDefaultValue,
      boolean isExpression,
      boolean nullable) {
    if (columnDefaultValue == null || columnDefaultValue.isEmpty()) {
      return nullable ? Literals.NULL : DEFAULT_VALUE_NOT_SET;
    }

    String reallyType = type.getTypeName();
    if (reallyType.startsWith("Nullable(")) {
      reallyType = type.getTypeName().substring(9, type.getTypeName().length() - 1);
    }

    if (reallyType.startsWith("Decimal(")) {
      reallyType = "Decimal";
    }

    if (reallyType.startsWith("FixedString(")) {
      reallyType = "FixedString";
    }

    if (nullable) {
      if (columnDefaultValue.equals("NULL")) {
        return Literals.NULL;
      }
    }

//    if (!(reallyType.equals(ClickHouseTypeConverter.STRING) || reallyType.equals(
//        ClickHouseTypeConverter.FIXEDSTRING))) {
//      if (columnDefaultValue.isEmpty()) {
//        return Literals.NULL;
//      }
//    }

    if (isExpression) {
      if (columnDefaultValue.equals(CURRENT_TIMESTAMP)) {
        return DEFAULT_VALUE_OF_CURRENT_TIMESTAMP;
      }
      // The parsing of ClickHouse expressions is complex, so we are not currently undertaking the
      // parsing.
      return UnparsedExpression.of(columnDefaultValue);
    }


    switch (reallyType) {
      case ClickHouseTypeConverter.INT8:
        return Literals.byteLiteral(Byte.valueOf(columnDefaultValue));
      case ClickHouseTypeConverter.UINT8:
        return Literals.unsignedByteLiteral(Short.valueOf(columnDefaultValue));
      case ClickHouseTypeConverter.INT16:
        return Literals.shortLiteral(Short.valueOf(columnDefaultValue));
      case ClickHouseTypeConverter.UINT16:
        return Literals.unsignedShortLiteral(Integer.valueOf(columnDefaultValue));
      case ClickHouseTypeConverter.INT32:
        return Literals.integerLiteral(Integer.valueOf(columnDefaultValue));
      case ClickHouseTypeConverter.UINT32:
        return Literals.unsignedIntegerLiteral(Long.valueOf(columnDefaultValue));
      case ClickHouseTypeConverter.INT64:
        return Literals.longLiteral(Long.valueOf(columnDefaultValue));
      case ClickHouseTypeConverter.UINT64:
        return Literals.unsignedLongLiteral(Decimal.of(columnDefaultValue));
      case ClickHouseTypeConverter.FLOAT32:
        return Literals.floatLiteral(Float.valueOf(columnDefaultValue));
      case ClickHouseTypeConverter.FLOAT64:
        return Literals.doubleLiteral(Double.valueOf(columnDefaultValue));
      case ClickHouseTypeConverter.DECIMAL:
        if (columnDefaultValue.equals("0.")) {
          columnDefaultValue = "0.0";
        }
        return Literals.decimalLiteral(
            Decimal.of(
                columnDefaultValue,
                Integer.parseInt(type.getColumnSize()),
                Integer.parseInt(type.getScale())));
      case ClickHouseTypeConverter.DATE:
        if (columnDefaultValue.equals("")) {
          return Literals.NULL;
        }
        return Literals.dateLiteral(LocalDate.parse(columnDefaultValue, DATE_FORMATTER));
      case ClickHouseTypeConverter.DATETIME:
        columnDefaultValue = columnDefaultValue.substring(1, columnDefaultValue.length() - 1);
        return CURRENT_TIMESTAMP.equals(columnDefaultValue)
            ? DEFAULT_VALUE_OF_CURRENT_TIMESTAMP
            : Literals.timestampLiteral(
                LocalDateTime.parse(columnDefaultValue, DATE_TIME_FORMATTER));
      case ClickHouseTypeConverter.STRING:
        // need exclude begin and end "'"
        return Literals.of(columnDefaultValue.substring(1, columnDefaultValue.length() - 1),
            Types.StringType.get());
      case ClickHouseTypeConverter.FIXEDSTRING:
        return Literals.of(
            columnDefaultValue.substring(1, columnDefaultValue.length() - 1),
            Types.FixedCharType.of(Integer.parseInt(type.getColumnSize())));
      default:
        return UnparsedExpression.of(columnDefaultValue);
    }
  }
}
