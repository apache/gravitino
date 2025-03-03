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
package org.apache.gravitino.catalog.maxcompute.converter;

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

public class MaxComputeColumnDefaultValueConverter extends JdbcColumnDefaultValueConverter {

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
      if (columnDefaultValue.equals(CURRENT_TIMESTAMP)) {
        return DEFAULT_VALUE_OF_CURRENT_TIMESTAMP;
      }
      // The parsing of MaxCompute expressions is complex, so we are not currently undertaking the
      // parsing.
      return UnparsedExpression.of(columnDefaultValue);
    }

    switch (type.getTypeName().toLowerCase()) {
      case MaxComputeTypeConverter.TINYINT:
        return Literals.byteLiteral(Byte.valueOf(columnDefaultValue));
      case MaxComputeTypeConverter.SMALLINT:
        return Literals.shortLiteral(Short.valueOf(columnDefaultValue));
      case MaxComputeTypeConverter.INT:
        return Literals.integerLiteral(Integer.valueOf(columnDefaultValue));
      case MaxComputeTypeConverter.BIGINT:
        return Literals.longLiteral(Long.valueOf(columnDefaultValue));
      case MaxComputeTypeConverter.FLOAT:
        return Literals.floatLiteral(Float.valueOf(columnDefaultValue));
      case MaxComputeTypeConverter.DOUBLE:
        return Literals.doubleLiteral(Double.valueOf(columnDefaultValue));
      case MaxComputeTypeConverter.DECIMAL:
        return Literals.decimalLiteral(
            Decimal.of(
                columnDefaultValue,
                Integer.parseInt(type.getColumnSize()),
                Integer.parseInt(type.getScale())));
      case JdbcTypeConverter.DATE:
        return Literals.dateLiteral(LocalDate.parse(columnDefaultValue, DATE_FORMATTER));
      case JdbcTypeConverter.TIMESTAMP:
      case MaxComputeTypeConverter.DATETIME:
        return CURRENT_TIMESTAMP.equals(columnDefaultValue)
            ? DEFAULT_VALUE_OF_CURRENT_TIMESTAMP
            : Literals.timestampLiteral(
                LocalDateTime.parse(columnDefaultValue, DATE_TIME_FORMATTER));
      case JdbcTypeConverter.VARCHAR:
        return Literals.of(
            columnDefaultValue, Types.VarCharType.of(Integer.parseInt(type.getColumnSize())));
      case MaxComputeTypeConverter.CHAR:
        return Literals.of(
            columnDefaultValue, Types.FixedCharType.of(Integer.parseInt(type.getColumnSize())));
      case MaxComputeTypeConverter.STRING:
        return Literals.stringLiteral(columnDefaultValue);
      case MaxComputeTypeConverter.BOOLEAN:
        return Literals.booleanLiteral(Boolean.valueOf(columnDefaultValue));
      default:
        return UnparsedExpression.of(columnDefaultValue);
    }
  }
}
