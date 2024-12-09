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

package org.apache.gravitino.cli;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.expressions.Expression;
import org.apache.gravitino.rel.expressions.literals.Literals;
import org.junit.jupiter.api.Test;

class TestDefaultConverter {

  @Test
  void testConvertNulls() {
    String defaultValue = null;
    String dataType = null;
    Expression result = DefaultConverter.convert(defaultValue, dataType);

    assertEquals(
        Column.DEFAULT_VALUE_NOT_SET,
        result,
        "Expected DEFAULT_VALUE_NOT_SET for null defaultValue.");
  }

  @Test
  void testConvertEmpty() {
    String defaultValue = "";
    String dataType = "";
    Expression result = DefaultConverter.convert(defaultValue, dataType);

    assertEquals(
        Column.DEFAULT_VALUE_NOT_SET,
        result,
        "Expected DEFAULT_VALUE_NOT_SET for null defaultValue.");
  }

  @Test
  void testConvertNullDefaultValue() {
    String defaultValue = null;
    String dataType = "INTEGER";
    Expression result = DefaultConverter.convert(defaultValue, dataType);

    assertEquals(
        Column.DEFAULT_VALUE_NOT_SET,
        result,
        "Expected DEFAULT_VALUE_NOT_SET for null defaultValue.");
  }

  @Test
  void testConvertEmptyDefaultValue() {
    String defaultValue = "";
    String dataType = "INTEGER";
    Expression result = DefaultConverter.convert(defaultValue, dataType);

    assertEquals(
        Column.DEFAULT_VALUE_NOT_SET,
        result,
        "Expected DEFAULT_VALUE_NOT_SET for empty defaultValue.");
  }

  @Test
  void testConvertCurrentTimestamp() {
    String defaultValue = "current_timestamp";
    String dataType = "TIMESTAMP";
    Expression result = DefaultConverter.convert(defaultValue, dataType);

    assertEquals(
        Column.DEFAULT_VALUE_OF_CURRENT_TIMESTAMP,
        result,
        "Expected DEFAULT_VALUE_OF_CURRENT_TIMESTAMP for 'current_timestamp' defaultValue.");
  }

  @Test
  void testConvertLiteralValue() {
    String defaultValue = "42";
    String dataType = "INTEGER";
    Expression result = DefaultConverter.convert(defaultValue, dataType);
    Expression expected = Literals.of(defaultValue, ParseType.toType(dataType));

    assertEquals(expected, result, "Expected literal expression for non-special default value.");
  }
}
