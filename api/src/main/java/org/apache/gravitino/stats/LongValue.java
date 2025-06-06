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

import java.util.Objects;

/** Represents a long value in statistics. */
public final class LongValue implements StatisticValue<Long> {

  private final Long value;

  /**
   * Constructor for LongValue.
   *
   * @param value the long value to be represented
   */
  public LongValue(Long value) {
    this.value = value;
  }

  @Override
  public Type type() {
    return Type.LONG;
  }

  @Override
  public Long value() {
    return value;
  }

  @Override
  public String toString() {
    return "LongValue{" + "value=" + value + '}';
  }

  @Override
  public int hashCode() {
    return Objects.hash(value);
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof LongValue)) {
      return false;
    }

    LongValue that = (LongValue) obj;

    return Objects.equals(this.value, that.value);
  }
}
