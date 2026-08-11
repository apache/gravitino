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
import org.apache.gravitino.annotation.Evolving;

/**
 * An immutable metric in a semantic model. A metric gives a model-scoped name to an aggregate or
 * otherwise calculated {@link Expression} and may carry descriptive, type, AI, and extension
 * metadata.
 */
@Evolving
public final class Metric {

  private final String name;
  private final Expression expression;

  @Nullable private final String description;
  @Nullable private final DataType datatype;
  @Nullable private final AIContext aiContext;
  @Nullable private final CustomExtension[] customExtensions;

  private Metric(Builder builder) {
    this.name = builder.name;
    this.expression = builder.expression;
    this.description = builder.description;
    this.datatype = builder.datatype;
    this.aiContext = builder.aiContext;
    this.customExtensions =
        builder.customExtensions == null
            ? null
            : Arrays.copyOf(builder.customExtensions, builder.customExtensions.length);
  }

  /**
   * Creates a builder for an immutable {@link Metric}.
   *
   * @return A new builder.
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Returns the metric name.
   *
   * @return The metric name.
   */
  public String name() {
    return name;
  }

  /**
   * Returns the expression that defines the metric.
   *
   * @return The metric expression.
   */
  public Expression expression() {
    return expression;
  }

  /**
   * Returns the metric description.
   *
   * @return The metric description, or {@code null} if it is not set.
   */
  @Nullable
  public String description() {
    return description;
  }

  /**
   * Returns the semantic data type of the metric.
   *
   * @return The semantic data type, or {@code null} if it is not set.
   */
  @Nullable
  public DataType datatype() {
    return datatype;
  }

  /**
   * Returns the AI context associated with the metric.
   *
   * @return The AI context, or {@code null} if it is not set.
   */
  @Nullable
  public AIContext aiContext() {
    return aiContext;
  }

  /**
   * Returns a copy of the custom extensions associated with the metric.
   *
   * @return The custom extensions, or {@code null} if they are not set.
   */
  @Nullable
  public CustomExtension[] customExtensions() {
    return customExtensions == null
        ? null
        : Arrays.copyOf(customExtensions, customExtensions.length);
  }

  /**
   * Compares this metric with another object for value equality.
   *
   * @param other The object to compare with.
   * @return {@code true} if the objects are equal, otherwise {@code false}.
   */
  @Override
  public boolean equals(@Nullable Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof Metric)) {
      return false;
    }
    Metric that = (Metric) other;
    return name.equals(that.name)
        && expression.equals(that.expression)
        && Objects.equals(description, that.description)
        && datatype == that.datatype
        && Objects.equals(aiContext, that.aiContext)
        && Arrays.equals(customExtensions, that.customExtensions);
  }

  /**
   * Returns the value-based hash code for this metric.
   *
   * @return The hash code.
   */
  @Override
  public int hashCode() {
    return Objects.hash(
        name, expression, description, datatype, aiContext, Arrays.hashCode(customExtensions));
  }

  /**
   * Returns a string representation of this metric.
   *
   * @return The string representation.
   */
  @Override
  public String toString() {
    return "Metric{"
        + "name='"
        + name
        + '\''
        + ", expression="
        + expression
        + ", description='"
        + description
        + '\''
        + ", datatype="
        + datatype
        + ", aiContext="
        + aiContext
        + ", customExtensions="
        + Arrays.toString(customExtensions)
        + '}';
  }

  /** A builder for immutable {@link Metric} values. */
  public static final class Builder {

    private String name;
    private Expression expression;

    @Nullable private String description;
    @Nullable private DataType datatype;
    @Nullable private AIContext aiContext;
    @Nullable private CustomExtension[] customExtensions;

    private Builder() {}

    /**
     * Sets the metric name.
     *
     * @param name The non-empty metric name.
     * @return This builder.
     */
    public Builder withName(String name) {
      this.name = name;
      return this;
    }

    /**
     * Sets the expression that defines the metric.
     *
     * @param expression The metric expression.
     * @return This builder.
     */
    public Builder withExpression(Expression expression) {
      this.expression = expression;
      return this;
    }

    /**
     * Sets the optional metric description.
     *
     * @param description The metric description, or {@code null} to leave it unset.
     * @return This builder.
     */
    public Builder withDescription(@Nullable String description) {
      this.description = description;
      return this;
    }

    /**
     * Sets the optional semantic data type.
     *
     * @param datatype The semantic data type, or {@code null} to leave it unset.
     * @return This builder.
     */
    public Builder withDatatype(@Nullable DataType datatype) {
      this.datatype = datatype;
      return this;
    }

    /**
     * Sets the optional AI context.
     *
     * @param aiContext The AI context, or {@code null} to leave it unset.
     * @return This builder.
     */
    public Builder withAIContext(@Nullable AIContext aiContext) {
      this.aiContext = aiContext;
      return this;
    }

    /**
     * Sets the optional custom extensions.
     *
     * @param customExtensions The custom extensions, or {@code null} to leave them unset.
     * @return This builder.
     */
    public Builder withCustomExtensions(@Nullable CustomExtension[] customExtensions) {
      this.customExtensions = customExtensions;
      return this;
    }

    /**
     * Builds an immutable {@link Metric}.
     *
     * @return The new metric.
     * @throws IllegalArgumentException If the name is null or empty, the expression is null, or the
     *     custom extension array contains null.
     */
    public Metric build() {
      Preconditions.checkArgument(
          name != null && !name.isEmpty(), "name must not be null or empty");
      Preconditions.checkArgument(expression != null, "expression must not be null");
      Preconditions.checkArgument(
          customExtensions == null || Arrays.stream(customExtensions).allMatch(Objects::nonNull),
          "customExtensions must not contain null");
      return new Metric(this);
    }
  }
}
