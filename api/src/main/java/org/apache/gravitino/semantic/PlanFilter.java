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
import javax.annotation.Nullable;
import org.apache.gravitino.annotation.Evolving;

/** A filter in a {@link SemanticQueryPlan}. */
@Evolving
public final class PlanFilter {

  private final PlanFieldRef field;
  private final SemanticQueryFilter.Operator operator;
  private final String value;

  private PlanFilter(PlanFieldRef field, SemanticQueryFilter.Operator operator, String value) {
    Preconditions.checkArgument(field != null, "field must not be null");
    Preconditions.checkArgument(operator != null, "operator must not be null");
    Preconditions.checkArgument(value != null, "value must not be null");

    this.field = field;
    this.operator = operator;
    this.value = value;
  }

  /**
   * Creates a plan filter.
   *
   * @param field The filtered field.
   * @param operator The comparison operator.
   * @param value The compared value as untyped text.
   * @return The plan filter.
   */
  public static PlanFilter of(
      PlanFieldRef field, SemanticQueryFilter.Operator operator, String value) {
    return new PlanFilter(field, operator, value);
  }

  /**
   * Returns the filtered field.
   *
   * @return The field reference.
   */
  public PlanFieldRef field() {
    return field;
  }

  /**
   * Returns the comparison operator.
   *
   * @return The operator.
   */
  public SemanticQueryFilter.Operator operator() {
    return operator;
  }

  /**
   * Returns the compared value as untyped text.
   *
   * @return The value.
   */
  public String value() {
    return value;
  }

  @Override
  public boolean equals(@Nullable Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof PlanFilter)) {
      return false;
    }
    PlanFilter that = (PlanFilter) other;
    return field.equals(that.field) && operator == that.operator && value.equals(that.value);
  }

  @Override
  public int hashCode() {
    return Objects.hash(field, operator, value);
  }

  @Override
  public String toString() {
    return "PlanFilter{"
        + "field="
        + field
        + ", operator="
        + operator
        + ", value='"
        + value
        + '\''
        + '}';
  }
}
