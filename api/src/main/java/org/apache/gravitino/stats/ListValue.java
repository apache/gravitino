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

import java.util.List;
import java.util.Objects;

/** Represents a list of statistic values in statistics. */
public final class ListValue implements StatisticValue<List<StatisticValue>> {
  private final List<StatisticValue> values;

  /**
   * Constructs a ListValue with the given list of statistic values.
   *
   * @param values a list of StatisticValue objects
   */
  public ListValue(List<StatisticValue> values) {
    this.values = values;
  }

  @Override
  public Type type() {
    return Type.LIST;
  }

  @Override
  public List<StatisticValue> value() {
    return values;
  }

  @Override
  public String toString() {
    return "ArrayValue{" + "value=" + values + '}';
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(values);
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof ListValue)) {
      return false;
    }
    ListValue that = (ListValue) obj;
    return Objects.equals(values, that.values);
  }
}
