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
final class SqlType {
  static final SqlType BOOLEAN = new SqlType("BOOLEAN");
  static final SqlType STRING = new SqlType("STRING");
  static final SqlType INTEGER = new SqlType("INTEGER");
  // Exact, as yet unbound numeric constants (not a supported declaration type).
  static final SqlType NUMBER = new SqlType("NUMBER");

  final String name;
  final int precision;
  final int scale;
  final boolean withTimeZone;

  SqlType(String name) {
    this(name, 0, 0, false);
  }

  SqlType(String name, int precision, int scale, boolean withTimeZone) {
    this.name = name;
    this.precision = precision;
    this.scale = scale;
    this.withTimeZone = withTimeZone;
  }

  boolean numeric() {
    return name.matches("INTEGER|LONG|FLOAT|DOUBLE|DECIMAL|NUMBER");
  }

  boolean temporal() {
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
