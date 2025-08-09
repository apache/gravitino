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
package org.apache.gravitino.trino.connector.catalog.jdbc;

import static org.apache.gravitino.rel.Column.DEFAULT_VALUE_NOT_SET;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import org.apache.gravitino.rel.expressions.Expression;
import org.apache.gravitino.rel.expressions.FunctionExpression;
import org.apache.gravitino.rel.expressions.literals.Literal;
import org.apache.gravitino.rel.expressions.literals.Literals;
import org.apache.gravitino.rel.types.Type;
import org.apache.gravitino.rel.types.Types;

/**
 * Column default value converter
 *
 * <p>Referred from org/apache/gravitino/catalog/jdbc/converter/JdbcColumnDefaultValueConverter.java
 */
public class JdbcColumnDefaultValueConverter {

  protected static final String CURRENT_TIMESTAMP = "CURRENT_TIMESTAMP";
  protected static final String NULL = "NULL";
  protected static final DateTimeFormatter DATE_TIME_FORMATTER =
      DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss[.SSSSSS][.SSSSS][.SSSS][.SSS][.SS][.S]");
  protected static final DateTimeFormatter DATE_FORMATTER =
      DateTimeFormatter.ofPattern("yyyy-MM-dd");
  protected static final DateTimeFormatter TIME_FORMATTER =
      DateTimeFormatter.ofPattern("HH:mm:ss[.SSSSSS][.SSSSS][.SSSS][.SSS][.SS][.S]");

  public String fromGravitino(Expression defaultValue) {
    if (DEFAULT_VALUE_NOT_SET.equals(defaultValue)) {
      return null;
    }

    if (defaultValue instanceof FunctionExpression) {
      FunctionExpression functionExpression = (FunctionExpression) defaultValue;
      if (functionExpression.functionName().equalsIgnoreCase(CURRENT_TIMESTAMP)) {
        // CURRENT_TIMESTAMP is a special case(key word), it should not be wrapped in parentheses
        return CURRENT_TIMESTAMP;
      }
    }

    if (defaultValue instanceof Literal) {
      Literal<?> literal = (Literal<?>) defaultValue;
      Type type = literal.dataType();
      if (defaultValue.equals(Literals.NULL)) {
        // Not display default value if default value is null
        return null;
      } else if (type instanceof Types.TimestampType) {
        /** @see LocalDateTime#toString() would return like 'yyyy-MM-ddTHH:mm:ss' */
        if (literal.value() instanceof String || literal.value() instanceof LocalDateTime) {
          return literal.value().toString().replace("T", " ");
        }
        return literal.value().toString();
      } else {
        return literal.value().toString();
      }
    }

    // todo support UnparseExpression default value convert
    return null;
  }

  public Expression toGravitino(Type columnType, String columnDefaultValue, boolean nullable) {
    return DEFAULT_VALUE_NOT_SET;
  }
}
