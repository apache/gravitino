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

/**
 * A reference to a field of a dataset in a {@link SemanticQueryPlan}.
 *
 * <p>Exactly one of {@link #expression()} and {@link #column()} is set. A declared field carries
 * its per-dialect expression; a physical column that the model does not expose carries only its
 * name.
 */
@Evolving
public final class PlanFieldRef {

  private final String dataset;

  @Nullable private final Expression expression;
  @Nullable private final String column;
  @Nullable private final DataType datatype;

  private PlanFieldRef(
      String dataset,
      @Nullable Expression expression,
      @Nullable String column,
      @Nullable DataType datatype) {
    Preconditions.checkArgument(
        StringUtils.isNotBlank(dataset), "dataset must not be null or blank");
    Preconditions.checkArgument(
        (expression == null) != (column == null),
        "exactly one of expression and column must be set");
    Preconditions.checkArgument(
        column == null || StringUtils.isNotBlank(column), "column must not be blank when present");

    this.dataset = dataset;
    this.expression = expression;
    this.column = column;
    this.datatype = datatype;
  }

  /**
   * Creates a reference to a declared field.
   *
   * @param dataset The dataset that provides the field.
   * @param expression The declared field expression per dialect.
   * @param datatype The logical data type, or {@code null} if the model declares none.
   * @return The field reference.
   */
  public static PlanFieldRef ofExpression(
      String dataset, Expression expression, @Nullable DataType datatype) {
    return new PlanFieldRef(dataset, expression, null, datatype);
  }

  /**
   * Creates a reference to a physical column that the model does not expose as a declared field.
   *
   * @param dataset The dataset that provides the column.
   * @param column The physical column name.
   * @param datatype The logical data type, or {@code null} if the model declares none.
   * @return The field reference.
   */
  public static PlanFieldRef ofColumn(String dataset, String column, @Nullable DataType datatype) {
    return new PlanFieldRef(dataset, null, column, datatype);
  }

  /**
   * Returns the dataset that provides the field.
   *
   * @return The dataset name.
   */
  public String dataset() {
    return dataset;
  }

  /**
   * Returns the declared field expression per dialect.
   *
   * @return The expression, or {@code null} for a physical column.
   */
  @Nullable
  public Expression expression() {
    return expression;
  }

  /**
   * Returns the physical column name.
   *
   * @return The column name, or {@code null} for a declared field.
   */
  @Nullable
  public String column() {
    return column;
  }

  /**
   * Returns the logical data type.
   *
   * @return The data type, or {@code null} if the model declares none.
   */
  @Nullable
  public DataType datatype() {
    return datatype;
  }

  @Override
  public boolean equals(@Nullable Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof PlanFieldRef)) {
      return false;
    }
    PlanFieldRef that = (PlanFieldRef) other;
    return dataset.equals(that.dataset)
        && Objects.equals(expression, that.expression)
        && Objects.equals(column, that.column)
        && datatype == that.datatype;
  }

  @Override
  public int hashCode() {
    return Objects.hash(dataset, expression, column, datatype);
  }

  @Override
  public String toString() {
    return "PlanFieldRef{"
        + "dataset='"
        + dataset
        + '\''
        + ", expression="
        + expression
        + ", column='"
        + column
        + '\''
        + ", datatype="
        + datatype
        + '}';
  }
}
