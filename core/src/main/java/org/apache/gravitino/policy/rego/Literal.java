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
package org.apache.gravitino.policy.rego;

import com.google.common.base.Preconditions;
import java.math.BigDecimal;
import java.util.Objects;

/** A decoded source literal. */
final class Literal implements CanonicalExpression {
  private final LiteralType literalType;
  private final Object value;
  private final boolean negativeZero;

  Literal(LiteralType literalType, Object value) {
    this(literalType, value, false);
  }

  Literal(LiteralType literalType, Object value, boolean negativeZero) {
    this.literalType = literalType;
    this.value = value;
    this.negativeZero = negativeZero;
  }

  LiteralType literalType() {
    return literalType;
  }

  Object value() {
    return value;
  }

  boolean negativeZero() {
    return negativeZero;
  }

  @Override
  public void validate() throws IllegalArgumentException {
    Preconditions.checkArgument(literalType != null, "literalType cannot be null");
    switch (literalType) {
      case STRING:
        Preconditions.checkArgument(value instanceof String, "string literal must be a string");
        Preconditions.checkArgument(!negativeZero, "string literal cannot be negative zero");
        ExpressionValidator.validateUnicodeScalars((String) value, "string literal");
        break;
      case NUMBER:
        Preconditions.checkArgument(
            value instanceof BigDecimal, "number literal must be an exact decimal");
        Preconditions.checkArgument(
            !negativeZero || ((BigDecimal) value).signum() == 0,
            "negative-zero marker requires a zero value");
        break;
      case BOOLEAN:
        Preconditions.checkArgument(value instanceof Boolean, "boolean literal must be boolean");
        Preconditions.checkArgument(!negativeZero, "boolean literal cannot be negative zero");
        break;
      case NULL:
        Preconditions.checkArgument(value == null, "null literal cannot contain a value");
        Preconditions.checkArgument(!negativeZero, "null literal cannot be negative zero");
        break;
      default:
        throw new IllegalArgumentException("Unsupported literal type: " + literalType);
    }
  }

  @Override
  public int depth() {
    return literalType == LiteralType.BOOLEAN ? 1 : 0;
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof Literal)) {
      return false;
    }
    Literal that = (Literal) other;
    if (literalType != that.literalType || negativeZero != that.negativeZero) {
      return false;
    }
    if (literalType == LiteralType.NUMBER) {
      return ((BigDecimal) value).compareTo((BigDecimal) that.value) == 0;
    }
    return Objects.equals(value, that.value);
  }

  @Override
  public int hashCode() {
    Object normalizedValue =
        literalType == LiteralType.NUMBER ? ((BigDecimal) value).stripTrailingZeros() : value;
    return Objects.hash(literalType, normalizedValue, negativeZero);
  }

  @Override
  public String toString() {
    return "Literal{"
        + "literalType="
        + literalType
        + ", value="
        + value
        + ", negativeZero="
        + negativeZero
        + '}';
  }
}
