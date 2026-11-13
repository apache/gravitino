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
package org.apache.gravitino.semantic;

import com.google.common.base.Preconditions;
import java.util.Objects;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.annotation.Evolving;

/**
 * An immutable filter applied to a field in a {@link SemanticQuery}.
 *
 * <p>The value is the literal compared with the field. Planners interpret it according to the
 * operator and the target dialect.
 */
@Evolving
public final class SemanticQueryFilter {

  /** The comparison operator of a filter. */
  public enum Operator {
    /** Equal to the value. */
    EQ,
    /** Not equal to the value. */
    NE,
    /** Greater than the value. */
    GT,
    /** Greater than or equal to the value. */
    GE,
    /** Less than the value. */
    LT,
    /** Less than or equal to the value. */
    LE
  }

  private final String field;
  private final Operator operator;
  private final String value;

  private SemanticQueryFilter(String field, Operator operator, String value) {
    Preconditions.checkArgument(
        StringUtils.isNotBlank(field), "filter field must not be null or blank");
    Preconditions.checkArgument(operator != null, "filter operator must not be null");
    Preconditions.checkArgument(value != null, "filter value must not be null");
    this.field = field;
    this.operator = operator;
    this.value = value;
  }

  /**
   * Creates a filter.
   *
   * @param field The field name to filter on.
   * @param operator The comparison operator.
   * @param value The literal value to compare with.
   * @return The immutable filter.
   * @throws IllegalArgumentException If the field is blank, the operator is null, or the value is
   *     null.
   */
  public static SemanticQueryFilter of(String field, Operator operator, String value) {
    return new SemanticQueryFilter(field, operator, value);
  }

  /**
   * Returns the field name to filter on.
   *
   * @return The field name.
   */
  public String field() {
    return field;
  }

  /**
   * Returns the comparison operator.
   *
   * @return The operator.
   */
  public Operator operator() {
    return operator;
  }

  /**
   * Returns the literal value to compare with.
   *
   * @return The value.
   */
  public String value() {
    return value;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof SemanticQueryFilter)) {
      return false;
    }
    SemanticQueryFilter that = (SemanticQueryFilter) other;
    return field.equals(that.field) && operator == that.operator && value.equals(that.value);
  }

  @Override
  public int hashCode() {
    return Objects.hash(field, operator, value);
  }

  @Override
  public String toString() {
    return "SemanticQueryFilter{"
        + "field='"
        + field
        + '\''
        + ", operator="
        + operator
        + ", value='"
        + value
        + '\''
        + '}';
  }
}
