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
package org.apache.gravitino.rel.types;

import com.google.common.base.Preconditions;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Objects;
import org.apache.gravitino.rel.types.Types.DecimalType;

/**
 * Used to represent a {@link Types.DecimalType} value in Apache Gravitino.
 *
 * <p>For Decimal, we expect the precision is equal to or larger than the scale, however, in {@link
 * BigDecimal}, the digit count starts from the leftmost nonzero digit of the exact result. For
 * example, the precision of 0.01 equals to 1 based on the definition, but the scale is 2. The
 * expected precision should be 2.
 */
public class Decimal {

  private final BigDecimal value;
  private final int precision;
  private final int scale;

  /**
   * Creates a decimal value with the given value, precision and scale.
   *
   * @param value The value of the decimal.
   * @param precision The precision of the decimal.
   * @param scale The scale of the decimal.
   * @return A new {@link Decimal} instance.
   */
  public static Decimal of(BigDecimal value, int precision, int scale) {
    return new Decimal(value, precision, scale);
  }

  /**
   * Creates a decimal value with the given value.
   *
   * @param value The value of the decimal.
   * @return A new {@link Decimal} instance.
   */
  public static Decimal of(BigDecimal value) {
    return new Decimal(value);
  }

  /**
   * Creates a decimal value with the given value.
   *
   * @param value The value of the decimal.
   * @return A new {@link Decimal} instance.
   */
  public static Decimal of(String value) {
    return of(new BigDecimal(value));
  }

  /**
   * Creates a decimal value with the given value, precision and scale.
   *
   * @param value The value of the decimal.
   * @param precision The precision of the decimal.
   * @param scale The scale of the decimal.
   * @return A new {@link Decimal} instance.
   */
  public static Decimal of(String value, int precision, int scale) {
    return of(new BigDecimal(value), precision, scale);
  }

  /** @return The value of the decimal. */
  public BigDecimal value() {
    return value;
  }

  /** @return The precision of the decimal. */
  public int precision() {
    return precision;
  }

  /** @return The scale of the decimal. */
  public int scale() {
    return scale;
  }

  private Decimal(BigDecimal value, int precision, int scale) {
    DecimalType.checkPrecisionScale(precision, scale);
    Preconditions.checkArgument(
        precision >= value.precision(),
        "Precision of value cannot be greater than precision of decimal: %s > %s",
        value.precision(),
        precision);
    this.value = value.setScale(scale, RoundingMode.HALF_UP);
    this.scale = scale;
    this.precision = precision;
  }

  private Decimal(BigDecimal value) {
    this(value, Math.max(value.precision(), value.scale()), value.scale());
  }

  @Override
  public String toString() {
    return value.toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof Decimal)) {
      return false;
    }
    Decimal decimal = (Decimal) o;
    return scale == decimal.scale
        && precision == decimal.precision
        && Objects.equals(value, decimal.value);
  }

  @Override
  public int hashCode() {
    return Objects.hash(value, precision, scale);
  }
}
