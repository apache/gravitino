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

import java.util.Objects;
import org.apache.gravitino.catalog.jdbc.converter.JdbcColumnDefaultValueConverter;
import org.apache.gravitino.catalog.jdbc.converter.JdbcTypeConverter;
import org.apache.gravitino.rel.expressions.Expression;
import org.apache.gravitino.rel.expressions.UnparsedExpression;
import org.apache.gravitino.rel.expressions.literals.Literals;

public class OceanBaseColumnDefaultValueConverter extends JdbcColumnDefaultValueConverter {

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
      if (columnDefaultValue.equals(CURRENT_TIMESTAMP)) {
        return DEFAULT_VALUE_OF_CURRENT_TIMESTAMP;
      }
      // The parsing of OceanBase expressions is complex, so we are not currently undertaking the
      // parsing.
      return UnparsedExpression.of(columnDefaultValue);
    }

    switch (type.getTypeName().toLowerCase()) {
      case OceanBaseTypeConverter.TINYINT:
        return Literals.byteLiteral(Byte.valueOf(columnDefaultValue));
      default:
        return UnparsedExpression.of(columnDefaultValue);
    }
  }
}
