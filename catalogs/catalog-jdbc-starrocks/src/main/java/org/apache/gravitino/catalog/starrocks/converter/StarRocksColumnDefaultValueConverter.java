/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */
package org.apache.gravitino.catalog.starrocks.converter;

import static org.apache.gravitino.catalog.starrocks.converter.StarRocksTypeConverter.BIGINT;
import static org.apache.gravitino.catalog.starrocks.converter.StarRocksTypeConverter.CHAR;
import static org.apache.gravitino.catalog.starrocks.converter.StarRocksTypeConverter.DATE;
import static org.apache.gravitino.catalog.starrocks.converter.StarRocksTypeConverter.DATETIME;
import static org.apache.gravitino.catalog.starrocks.converter.StarRocksTypeConverter.DECIMAL;
import static org.apache.gravitino.catalog.starrocks.converter.StarRocksTypeConverter.DOUBLE;
import static org.apache.gravitino.catalog.starrocks.converter.StarRocksTypeConverter.FLOAT;
import static org.apache.gravitino.catalog.starrocks.converter.StarRocksTypeConverter.INT;
import static org.apache.gravitino.catalog.starrocks.converter.StarRocksTypeConverter.SMALLINT;
import static org.apache.gravitino.catalog.starrocks.converter.StarRocksTypeConverter.TINYINT;
import static org.apache.gravitino.catalog.starrocks.converter.StarRocksTypeConverter.VARCHAR;
import static org.apache.gravitino.rel.Column.DEFAULT_VALUE_NOT_SET;
import static org.apache.gravitino.rel.Column.DEFAULT_VALUE_OF_CURRENT_TIMESTAMP;

import java.time.LocalDate;
import java.time.LocalDateTime;
import org.apache.gravitino.catalog.jdbc.converter.JdbcColumnDefaultValueConverter;
import org.apache.gravitino.catalog.jdbc.converter.JdbcTypeConverter;
import org.apache.gravitino.rel.expressions.Expression;
import org.apache.gravitino.rel.expressions.UnparsedExpression;
import org.apache.gravitino.rel.expressions.literals.Literals;
import org.apache.gravitino.rel.types.Decimal;
import org.apache.gravitino.rel.types.Types;

public class StarRocksColumnDefaultValueConverter extends JdbcColumnDefaultValueConverter {

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
      // The parsing of StarRocks expressions is complex, so we are not currently undertaking the
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
      case DATE:
        return Literals.dateLiteral(LocalDate.parse(columnDefaultValue, DATE_FORMATTER));
      case DATETIME:
        return CURRENT_TIMESTAMP.equals(columnDefaultValue)
            ? DEFAULT_VALUE_OF_CURRENT_TIMESTAMP
            : Literals.timestampLiteral(
                LocalDateTime.parse(columnDefaultValue, DATE_TIME_FORMATTER));
      case VARCHAR:
        return Literals.of(columnDefaultValue, Types.VarCharType.of(columnType.getColumnSize()));
      case CHAR:
        return Literals.of(columnDefaultValue, Types.FixedCharType.of(columnType.getColumnSize()));
      default:
        throw new IllegalArgumentException("Unknown data columnType for literal: " + columnType);
    }
  }
}
