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
package org.apache.gravitino.policy.sql;

import java.util.Objects;

/** Exact logical types in the restricted SQL profile, independent of a target schema. */
public final class SqlType {
  /** SQL Boolean type. */
  public static final SqlType BOOLEAN = new SqlType("BOOLEAN");
  /** SQL string type. */
  public static final SqlType STRING = new SqlType("STRING");
  /** SQL signed integer type. */
  public static final SqlType INTEGER = new SqlType("INTEGER");
  // Exact, as yet unbound numeric constants (not a supported declaration type).
  static final SqlType NUMBER = new SqlType("NUMBER");

  final String name;
  final int precision;
  final int scale;
  final boolean withTimeZone;

  /**
   * Creates a scalar type without precision or timezone attributes.
   *
   * @param name canonical SQL type name
   */
  public SqlType(String name) {
    this(name, 0, 0, false);
  }

  /**
   * Creates an exact logical type descriptor.
   *
   * @param name canonical SQL type name
   * @param precision decimal precision or temporal fractional precision
   * @param scale decimal scale
   * @param withTimeZone whether a timestamp has a timezone
   */
  public SqlType(String name, int precision, int scale, boolean withTimeZone) {
    this.name = name;
    this.precision = precision;
    this.scale = scale;
    this.withTimeZone = withTimeZone;
  }

  /**
   * Returns the canonical SQL type name.
   *
   * @return canonical SQL type name
   */
  public String name() {
    return name;
  }

  /**
   * Returns decimal or temporal precision.
   *
   * @return declared precision, or zero when unset
   */
  public int precision() {
    return precision;
  }

  /**
   * Returns decimal scale.
   *
   * @return decimal scale
   */
  public int scale() {
    return scale;
  }

  /**
   * Returns whether the timestamp carries a timezone.
   *
   * @return whether this timestamp has a timezone
   */
  public boolean withTimeZone() {
    return withTimeZone;
  }

  /**
   * Returns whether this is a numeric type.
   *
   * @return whether this is a numeric type
   */
  public boolean numeric() {
    return name.matches("INTEGER|LONG|FLOAT|DOUBLE|DECIMAL|NUMBER");
  }

  /**
   * Returns whether this is a date or timestamp type.
   *
   * @return whether this is a date or timestamp type
   */
  public boolean temporal() {
    return name.equals("DATE") || name.equals("TIMESTAMP");
  }

  /** {@inheritDoc} */
  @Override
  public boolean equals(Object other) {
    if (!(other instanceof SqlType)) {
      return false;
    }
    SqlType that = (SqlType) other;
    return name.equals(that.name)
        && precision == that.precision
        && scale == that.scale
        && withTimeZone == that.withTimeZone;
  }

  /** {@inheritDoc} */
  @Override
  public int hashCode() {
    return Objects.hash(name, precision, scale, withTimeZone);
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    return name.equals("DECIMAL")
        ? name + "(" + precision + "," + scale + ")"
        : name + (withTimeZone ? " WITH TIME ZONE" : "");
  }
}
