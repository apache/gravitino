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
package org.apache.gravitino.trino.connector.catalog.jdbc.mysql;

import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static org.apache.gravitino.rel.Column.DEFAULT_VALUE_NOT_SET;
import static org.apache.gravitino.rel.Column.DEFAULT_VALUE_OF_CURRENT_TIMESTAMP;

import io.trino.spi.TrinoException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeParseException;
import org.apache.gravitino.rel.expressions.Expression;
import org.apache.gravitino.rel.expressions.literals.Literals;
import org.apache.gravitino.rel.types.Decimal;
import org.apache.gravitino.rel.types.Type;
import org.apache.gravitino.rel.types.Types;
import org.apache.gravitino.trino.connector.catalog.jdbc.JdbcColumnDefaultValueConverter;

/**
 * Column default value converter for MySQL
 *
 * <p>Referred from
 * org/apache/gravitino/catalog/mysql/converter/MysqlColumnDefaultValueConverter.java
 */
public class MysqlColumnDefaultValueConverter extends JdbcColumnDefaultValueConverter {

  @Override
  public Expression toGravitino(Type type, String columnDefaultValue, boolean nullable) {
    if (columnDefaultValue == null) {
      return nullable ? Literals.NULL : DEFAULT_VALUE_NOT_SET;
    }

    if (columnDefaultValue.equalsIgnoreCase(NULL)) {
      return Literals.NULL;
    }

    try {
      switch (type.name()) {
        case BYTE:
          return Literals.byteLiteral(Byte.valueOf(columnDefaultValue));
        case SHORT:
          return Literals.shortLiteral(Short.valueOf(columnDefaultValue));
        case INTEGER:
          return Literals.integerLiteral(Integer.valueOf(columnDefaultValue));
        case LONG:
          return Literals.longLiteral(Long.valueOf(columnDefaultValue));
        case FLOAT:
          return Literals.floatLiteral(Float.valueOf(columnDefaultValue));
        case DOUBLE:
          return Literals.doubleLiteral(Double.valueOf(columnDefaultValue));
        case DECIMAL:
          Types.DecimalType decimalType = (Types.DecimalType) type;
          return Literals.decimalLiteral(
              Decimal.of(columnDefaultValue, decimalType.precision(), decimalType.scale()));
        case DATE:
          return Literals.dateLiteral(LocalDate.parse(columnDefaultValue, DATE_FORMATTER));
        case TIME:
          return Literals.timeLiteral(LocalTime.parse(columnDefaultValue, TIME_FORMATTER));
        case TIMESTAMP:
          if (CURRENT_TIMESTAMP.equalsIgnoreCase(columnDefaultValue)) {
            return DEFAULT_VALUE_OF_CURRENT_TIMESTAMP;
          }
          try {
            return Literals.timestampLiteral(
                LocalDateTime.parse(columnDefaultValue, DATE_TIME_FORMATTER));
          } catch (DateTimeParseException e) {
            throw new IllegalArgumentException(
                String.format("Unable to parse datetime value: %s", columnDefaultValue));
          }
        case FIXEDCHAR:
          Types.FixedCharType fixedCharType = (Types.FixedCharType) type;
          return Literals.of(columnDefaultValue, Types.FixedCharType.of(fixedCharType.length()));
        case VARCHAR:
        case STRING:
          return Literals.stringLiteral(columnDefaultValue);
        default:
          throw new IllegalArgumentException("This Type not support default value");
      }
    } catch (Exception e) {
      throw new TrinoException(
          NOT_SUPPORTED,
          String.format(
              "Invalidate default value %s for %s type. %s",
              columnDefaultValue, type.name(), e.getMessage()));
    }
  }
}
