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
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.annotation.Evolving;

/** A selected metric or dimension in a {@link SemanticQueryPlan}. */
@Evolving
public final class PlanSelection {

  /** The role of a selection. */
  public enum Role {
    /** A metric selection. */
    METRIC,
    /** A dimension selection. */
    DIMENSION
  }

  private final String name;
  private final Role role;
  private final boolean aggregate;
  private final PlanFieldRef field;

  private PlanSelection(String name, Role role, boolean aggregate, PlanFieldRef field) {
    Preconditions.checkArgument(
        StringUtils.isNotBlank(name), "selection name must not be null or blank");
    Preconditions.checkArgument(role != null, "role must not be null");
    Preconditions.checkArgument(field != null, "field must not be null");
    Preconditions.checkArgument(
        aggregate == (role == Role.METRIC), "aggregate must be true exactly for METRIC selections");
    this.name = name;
    this.role = role;
    this.aggregate = aggregate;
    this.field = field;
  }

  /**
   * Creates a plan selection.
   *
   * @param name The metric or field name as requested.
   * @param role The selection role.
   * @param aggregate Whether the selection aggregates.
   * @param field The selected field.
   * @return The plan selection.
   */
  public static PlanSelection of(String name, Role role, boolean aggregate, PlanFieldRef field) {
    return new PlanSelection(name, role, aggregate, field);
  }

  /**
   * Returns the metric or field name as requested.
   *
   * @return The selection name.
   */
  public String name() {
    return name;
  }

  /**
   * Returns the selection role.
   *
   * @return The role.
   */
  public Role role() {
    return role;
  }

  /**
   * Returns whether this selection aggregates.
   *
   * @return {@code true} for metrics, {@code false} for dimensions.
   */
  public boolean aggregate() {
    return aggregate;
  }

  /**
   * Returns the selected field.
   *
   * @return The field reference.
   */
  public PlanFieldRef field() {
    return field;
  }

  @Override
  public boolean equals(@Nullable Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof PlanSelection)) {
      return false;
    }
    PlanSelection that = (PlanSelection) other;
    return aggregate == that.aggregate
        && name.equals(that.name)
        && role == that.role
        && field.equals(that.field);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, role, aggregate, field);
  }

  @Override
  public String toString() {
    return "PlanSelection{"
        + "name='"
        + name
        + '\''
        + ", role="
        + role
        + ", aggregate="
        + aggregate
        + ", field="
        + field
        + '}';
  }
}
