/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.rel.types;

import static com.datastrato.gravitino.rel.types.Types.DecimalType.checkPrecisionScale;

import com.google.common.base.Preconditions;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Objects;

/**
 * Used to represent a {@link Types.DecimalType} value in Gravitino.
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
    checkPrecisionScale(precision, scale);
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
    return scale == decimal.scale && Objects.equals(value, decimal.value);
  }

  @Override
  public int hashCode() {
    return Objects.hash(value, precision, scale);
  }
}
