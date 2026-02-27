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

package org.apache.gravitino.maintenance.optimizer.common.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.apache.gravitino.stats.StatisticValue;
import org.apache.gravitino.stats.StatisticValues;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TestStatisticValueUtils {

  @Test
  void avgReturnsEmptyForEmptyList() {
    Assertions.assertTrue(StatisticValueUtils.avg(List.of()).isEmpty());
  }

  @Test
  void avgCalculatesCorrectAverageForLongValues() {
    List<StatisticValue<?>> values =
        List.of(
            StatisticValues.longValue(10L),
            StatisticValues.longValue(20L),
            StatisticValues.longValue(30L));
    StatisticValue<?> result = StatisticValueUtils.avg(values).orElseThrow();
    Assertions.assertEquals(20.0, result.value());
  }

  @Test
  void avgCalculatesCorrectAverageForDoubleValues() {
    List<StatisticValue<?>> values =
        List.of(
            StatisticValues.doubleValue(10.5),
            StatisticValues.doubleValue(20.5),
            StatisticValues.doubleValue(30.5));
    StatisticValue<?> result = StatisticValueUtils.avg(values).orElseThrow();
    Assertions.assertEquals(20.5, result.value());
  }

  @Test
  void sumReturnsEmptyForEmptyList() {
    Assertions.assertTrue(StatisticValueUtils.sum(List.of()).isEmpty());
  }

  @Test
  void sumCalculatesCorrectSumForLongValues() {
    List<StatisticValue<?>> values =
        List.of(
            StatisticValues.longValue(10L),
            StatisticValues.longValue(20L),
            StatisticValues.longValue(30L));
    StatisticValue<?> result = StatisticValueUtils.sum(values).orElseThrow();
    Assertions.assertEquals(60L, result.value());
  }

  @Test
  void sumCalculatesCorrectSumForDoubleValues() {
    List<StatisticValue<?>> values =
        List.of(
            StatisticValues.doubleValue(10.5),
            StatisticValues.doubleValue(20.5),
            StatisticValues.doubleValue(30.5));
    StatisticValue<?> result = StatisticValueUtils.sum(values).orElseThrow();
    Assertions.assertEquals(61.5, result.value());
  }

  @Test
  void divThrowsExceptionForZeroDivisor() {
    StatisticValue<?> value = StatisticValues.longValue(10L);
    IllegalArgumentException exception =
        Assertions.assertThrowsExactly(
            IllegalArgumentException.class, () -> StatisticValueUtils.div(value, 0));
    Assertions.assertEquals("Divisor cannot be zero", exception.getMessage());
  }

  @Test
  void divCalculatesCorrectDivisionForLongValues() {
    StatisticValue<?> value = StatisticValues.longValue(10L);
    StatisticValue<?> result = StatisticValueUtils.div(value, 2);
    Assertions.assertEquals(5.0, result.value());
  }

  @Test
  void divCalculatesCorrectDivisionForDoubleValues() {
    StatisticValue<?> value = StatisticValues.doubleValue(10.5);
    StatisticValue<?> result = StatisticValueUtils.div(value, 2);
    Assertions.assertEquals(5.25, result.value());
  }

  @Test
  void avgThrowsExceptionForNonNumberValues() {
    List<StatisticValue<?>> values =
        List.of(
            StatisticValues.longValue(10L),
            StatisticValues.doubleValue(20.5),
            StatisticValues.stringValue("invalid"));
    Assertions.assertThrowsExactly(
        IllegalArgumentException.class, () -> StatisticValueUtils.avg(values));
  }

  @Test
  void testSerDeserEquals() {
    StatisticValue<?> value = StatisticValues.longValue(100L);
    String result = StatisticValueUtils.toString(value);
    StatisticValue<?> deserializeValue = StatisticValueUtils.fromString(result);
    assertEquals(value, deserializeValue);

    StatisticValue<?> doubleValue = StatisticValues.doubleValue(100.5);
    result = StatisticValueUtils.toString(doubleValue);
    deserializeValue = StatisticValueUtils.fromString(result);
    assertEquals(doubleValue, deserializeValue);
  }

  @Test
  void toStringSerializesStatisticValueCorrectly() {
    StatisticValue<?> value = StatisticValues.longValue(100L);
    String result = StatisticValueUtils.toString(value);
    assertEquals("100", result);

    value = StatisticValues.doubleValue(100.5);
    result = StatisticValueUtils.toString(value);
    assertEquals("100.5", result);
  }

  @Test
  void fromStringDeserializesStatisticValueCorrectly() {
    String valueStr = "100";
    StatisticValue<?> result = StatisticValueUtils.fromString(valueStr);
    assertTrue(result instanceof StatisticValues.LongValue);
    assertEquals(100L, ((StatisticValues.LongValue) result).value());

    String doubleValueStr = "100.5";
    StatisticValue<?> doubleResult = StatisticValueUtils.fromString(doubleValueStr);
    assertTrue(doubleResult instanceof StatisticValues.DoubleValue);
    assertEquals(100.5, ((StatisticValues.DoubleValue) doubleResult).value());
  }

  @Test
  void toStringThrowsExceptionForNullValue() {
    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> StatisticValueUtils.toString(null));
    assertEquals("StatisticValue cannot be null", exception.getMessage());
  }

  @Test
  void fromStringThrowsExceptionForNullString() {
    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> StatisticValueUtils.fromString(null));
    assertEquals("StatisticValue string cannot be null", exception.getMessage());
  }
}
