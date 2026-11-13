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
import java.util.Arrays;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.annotation.Evolving;

/**
 * A join that connects two datasets of a {@link SemanticQueryPlan}. The join columns are
 * positional: {@code fromColumns[i]} is joined to {@code toColumns[i]}.
 */
@Evolving
public final class PlanJoin {

  /** The supported join types. */
  public enum Type {
    /** An inner join. */
    INNER
  }

  private final Type type;

  @Nullable private final String relationship;

  private final String fromDataset;
  private final String[] fromColumns;
  private final String toDataset;
  private final String[] toColumns;

  private PlanJoin(
      Type type,
      @Nullable String relationship,
      String fromDataset,
      String[] fromColumns,
      String toDataset,
      String[] toColumns) {
    Preconditions.checkArgument(type != null, "type must not be null");
    Preconditions.checkArgument(
        relationship == null || StringUtils.isNotBlank(relationship),
        "relationship must not be blank when present");
    Preconditions.checkArgument(
        StringUtils.isNotBlank(fromDataset), "fromDataset must not be null or blank");
    Preconditions.checkArgument(
        StringUtils.isNotBlank(toDataset), "toDataset must not be null or blank");
    Preconditions.checkArgument(
        fromColumns != null && fromColumns.length > 0, "fromColumns must not be null or empty");
    Preconditions.checkArgument(
        toColumns != null && toColumns.length > 0, "toColumns must not be null or empty");
    validateColumns("fromColumns", fromColumns);
    validateColumns("toColumns", toColumns);
    Preconditions.checkArgument(
        fromColumns.length == toColumns.length,
        "fromColumns and toColumns must have the same length");

    this.type = type;
    this.relationship = relationship;
    this.fromDataset = fromDataset;
    this.fromColumns = Arrays.copyOf(fromColumns, fromColumns.length);
    this.toDataset = toDataset;
    this.toColumns = Arrays.copyOf(toColumns, toColumns.length);
  }

  /**
   * Creates a plan join.
   *
   * @param type The join type.
   * @param relationship The relationship name, or {@code null} if the join is implied.
   * @param fromDataset The dataset already in scope.
   * @param fromColumns The join columns of {@code fromDataset}.
   * @param toDataset The dataset this join brings into scope.
   * @param toColumns The join columns of {@code toDataset}, pairwise with {@code fromColumns}.
   * @return The plan join.
   */
  public static PlanJoin of(
      Type type,
      @Nullable String relationship,
      String fromDataset,
      String[] fromColumns,
      String toDataset,
      String[] toColumns) {
    return new PlanJoin(type, relationship, fromDataset, fromColumns, toDataset, toColumns);
  }

  /**
   * Returns the join type.
   *
   * @return The join type.
   */
  public Type type() {
    return type;
  }

  /**
   * Returns the Semantic Model relationship this join came from.
   *
   * @return The relationship name, or {@code null} if the join is implied.
   */
  @Nullable
  public String relationship() {
    return relationship;
  }

  /**
   * Returns the dataset already in scope.
   *
   * @return The source dataset name.
   */
  public String fromDataset() {
    return fromDataset;
  }

  /**
   * Returns a copy of the join columns of the source dataset.
   *
   * @return The source columns.
   */
  public String[] fromColumns() {
    return Arrays.copyOf(fromColumns, fromColumns.length);
  }

  /**
   * Returns the dataset this join brings into scope.
   *
   * @return The target dataset name.
   */
  public String toDataset() {
    return toDataset;
  }

  /**
   * Returns a copy of the join columns of the target dataset.
   *
   * @return The target columns, pairwise with {@link #fromColumns()}.
   */
  public String[] toColumns() {
    return Arrays.copyOf(toColumns, toColumns.length);
  }

  private static void validateColumns(String name, String[] columns) {
    for (int index = 0; index < columns.length; index++) {
      Preconditions.checkArgument(
          StringUtils.isNotBlank(columns[index]), "%s[%s] must not be null or blank", name, index);
    }
  }

  @Override
  public boolean equals(@Nullable Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof PlanJoin)) {
      return false;
    }
    PlanJoin that = (PlanJoin) other;
    return type == that.type
        && Objects.equals(relationship, that.relationship)
        && fromDataset.equals(that.fromDataset)
        && Arrays.equals(fromColumns, that.fromColumns)
        && toDataset.equals(that.toDataset)
        && Arrays.equals(toColumns, that.toColumns);
  }

  @Override
  public int hashCode() {
    int result = Objects.hash(type, relationship, fromDataset, toDataset);
    result = 31 * result + Arrays.hashCode(fromColumns);
    result = 31 * result + Arrays.hashCode(toColumns);
    return result;
  }

  @Override
  public String toString() {
    return "PlanJoin{"
        + "type="
        + type
        + ", relationship='"
        + relationship
        + '\''
        + ", fromDataset='"
        + fromDataset
        + '\''
        + ", fromColumns="
        + Arrays.toString(fromColumns)
        + ", toDataset='"
        + toDataset
        + '\''
        + ", toColumns="
        + Arrays.toString(toColumns)
        + '}';
  }
}
