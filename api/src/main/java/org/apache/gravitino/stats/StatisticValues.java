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
package org.apache.gravitino.stats;

import com.google.common.base.Preconditions;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import org.apache.gravitino.rel.types.Type;
import org.apache.gravitino.rel.types.Types;

/** A class representing a collection of statistic values. */
public class StatisticValues {

  private StatisticValues() {}

  /**
   * Creates a statistic value that holds a boolean value.
   *
   * @param value the boolean value to be held by this statistic value
   * @return a BooleanValue instance containing the provided boolean value
   */
  public static BooleanValue booleanValue(boolean value) {
    return new BooleanValue(value);
  }

  /**
   * Creates a statistic value that holds a long value.
   *
   * @param value the long value to be held by this statistic value
   * @return a LongValue instance containing the provided long value
   */
  public static LongValue longValue(long value) {
    return new LongValue(value);
  }

  /**
   * Creates a statistic value that holds a double value.
   *
   * @param value the double value to be held by this statistic value
   * @return a DoubleValue instance containing the provided double value
   */
  public static DoubleValue doubleValue(double value) {
    return new DoubleValue(value);
  }

  /**
   * Creates a statistic value that holds a string value.
   *
   * @param value the string value to be held by this statistic value
   * @return a StringValue instance containing the provided string value
   */
  public static StringValue stringValue(String value) {
    return new StringValue(value);
  }

  /**
   * Creates a statistic value that holds a list of other statistic values.
   *
   * @param <T> the type of the values in the list
   * @param values the list of statistic values to be held by this statistic value
   * @return a ListValue instance containing the provided list of statistic values
   */
  public static <T> ListValue<T> listValue(List<StatisticValue<T>> values) {
    return new ListValue<T>(values);
  }

  /**
   * Creates a statistic value that holds a map of string keys to other statistic values.
   *
   * @param values the map of string keys to statistic values to be held by this statistic value
   * @return an ObjectValue instance containing the provided map of statistic values
   */
  public static ObjectValue objectValue(Map<String, StatisticValue<?>> values) {
    return new ObjectValue(values);
  }

  /** A statistic value that holds a Boolean value. */
  public static class BooleanValue implements StatisticValue<Boolean> {
    private final Boolean value;

    private BooleanValue(Boolean value) {
      this.value = value;
    }

    @Override
    public Boolean value() {
      return value;
    }

    @Override
    public Type dataType() {
      return Types.BooleanType.get();
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(value);
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }

      if (!(obj instanceof BooleanValue)) {
        return false;
      }

      return Objects.equals(value, ((BooleanValue) obj).value());
    }
  }

  /** A statistic value that holds a Long value. */
  public static class LongValue implements StatisticValue<Long> {
    private final Long value;

    private LongValue(Long value) {
      this.value = value;
    }

    @Override
    public Long value() {
      return value;
    }

    @Override
    public Type dataType() {
      return Types.LongType.get();
    }

    @Override
    public int hashCode() {
      return Objects.hash(value);
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }

      if (!(obj instanceof LongValue)) {
        return false;
      }

      return Objects.equals(value, ((LongValue) obj).value());
    }
  }

  /** A statistic value that holds a Double value. */
  public static class DoubleValue implements StatisticValue<Double> {
    private final Double value;

    private DoubleValue(Double value) {
      this.value = value;
    }

    @Override
    public Double value() {
      return value;
    }

    @Override
    public Type dataType() {
      return Types.DoubleType.get();
    }

    @Override
    public int hashCode() {
      return Objects.hash(value);
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }

      if (!(obj instanceof DoubleValue)) {
        return false;
      }

      return Objects.equals(value, ((DoubleValue) obj).value());
    }
  }

  /** A statistic value that holds a String value. */
  public static class StringValue implements StatisticValue<String> {
    private final String value;

    private StringValue(String value) {
      this.value = value;
    }

    @Override
    public String value() {
      return value;
    }

    @Override
    public Type dataType() {
      return Types.StringType.get();
    }

    @Override
    public int hashCode() {
      return Objects.hash(value);
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }

      if (!(obj instanceof StringValue)) {
        return false;
      }

      return Objects.equals(value, ((StringValue) obj).value());
    }
  }

  /** A statistic value that holds a List of other statistic values. */
  public static class ListValue<T> implements StatisticValue<List<StatisticValue<T>>> {
    private final List<StatisticValue<T>> valueList;

    private ListValue(List<StatisticValue<T>> valueList) {
      Preconditions.checkArgument(
          valueList != null && !valueList.isEmpty(), "Values cannot be null or empty");

      Type dataType = valueList.get(0).dataType();
      Preconditions.checkArgument(
          valueList.stream().allMatch(value -> value.dataType().equals(dataType)),
          "All values in the list must have the same data type");

      this.valueList = valueList;
    }

    @Override
    public List<StatisticValue<T>> value() {
      return valueList;
    }

    @Override
    public Type dataType() {
      return Types.ListType.nullable(valueList.get(0).dataType());
    }

    @Override
    public int hashCode() {
      return Objects.hash(valueList);
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }

      if (!(obj instanceof ListValue)) {
        return false;
      }

      return Objects.equals(valueList, ((ListValue<?>) obj).value());
    }
  }

  /** A statistic value that holds a Map of String keys to other statistic values. */
  public static class ObjectValue implements StatisticValue<Map<String, StatisticValue<?>>> {
    private final Map<String, StatisticValue<?>> valueMap;

    private ObjectValue(Map<String, StatisticValue<?>> valueMap) {
      Preconditions.checkArgument(
          valueMap != null && !valueMap.isEmpty(), "Values cannot be null or empty");
      if (valueMap instanceof TreeMap) {
        this.valueMap = valueMap;
      } else {
        this.valueMap = new TreeMap<>(valueMap);
      }
    }

    @Override
    public Map<String, StatisticValue<?>> value() {
      return valueMap;
    }

    @Override
    public Type dataType() {
      return Types.StructType.of(
          valueMap.entrySet().stream()
              .map(
                  entry ->
                      Types.StructType.Field.nullableField(
                          entry.getKey(), entry.getValue().dataType()))
              .toArray(Types.StructType.Field[]::new));
    }

    @Override
    public int hashCode() {
      return Objects.hash(valueMap);
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }

      if (!(obj instanceof ObjectValue)) {
        return false;
      }

      return Objects.equals(valueMap, ((ObjectValue) obj).value());
    }
  }
}
