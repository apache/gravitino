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
package org.apache.gravitino.rel.metric;

import com.google.common.base.Preconditions;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.gravitino.annotation.Unstable;

/** A named metric in a semantic model. */
@Unstable
public final class Metric {

  private final String name;
  private final Expression expression;
  @Nullable private final String description;
  @Nullable private final AIContext aiContext;
  private final List<CustomExtension> customExtensions;

  private Metric(
      String name,
      Expression expression,
      @Nullable String description,
      @Nullable AIContext aiContext,
      List<CustomExtension> customExtensions) {
    this.name = name;
    this.expression = expression;
    this.description = description;
    this.aiContext = aiContext;
    this.customExtensions = customExtensions;
  }

  /**
   * @return A new metric builder.
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * @return The metric name.
   */
  public String name() {
    return name;
  }

  /**
   * @return The metric expression.
   */
  public Expression expression() {
    return expression;
  }

  /**
   * @return The metric description, or {@code null}.
   */
  @Nullable
  public String description() {
    return description;
  }

  /**
   * @return The AI context, or {@code null}.
   */
  @Nullable
  public AIContext aiContext() {
    return aiContext;
  }

  /**
   * @return An immutable list of custom extensions.
   */
  public List<CustomExtension> customExtensions() {
    return customExtensions;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof Metric)) {
      return false;
    }
    Metric that = (Metric) other;
    return Objects.equals(name, that.name)
        && Objects.equals(expression, that.expression)
        && Objects.equals(description, that.description)
        && Objects.equals(aiContext, that.aiContext)
        && Objects.equals(customExtensions, that.customExtensions);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, expression, description, aiContext, customExtensions);
  }

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
        + ", aiContext="
        + aiContext
        + ", customExtensions="
        + customExtensions
        + '}';
  }

  /** Builder for {@link Metric}. */
  public static final class Builder {
    private String name;
    private Expression expression;
    @Nullable private String description;
    @Nullable private AIContext aiContext;
    private List<CustomExtension> customExtensions;

    private Builder() {}

    /**
     * Sets the metric name.
     *
     * @param name The metric name.
     * @return This builder.
     */
    public Builder withName(String name) {
      this.name = name;
      return this;
    }

    /**
     * Sets the metric expression.
     *
     * @param expression The metric expression.
     * @return This builder.
     */
    public Builder withExpression(Expression expression) {
      this.expression = expression;
      return this;
    }

    /**
     * Sets the metric description.
     *
     * @param description The metric description.
     * @return This builder.
     */
    public Builder withDescription(@Nullable String description) {
      this.description = description;
      return this;
    }

    /**
     * Sets the AI context.
     *
     * @param aiContext The AI context.
     * @return This builder.
     */
    public Builder withAIContext(@Nullable AIContext aiContext) {
      this.aiContext = aiContext;
      return this;
    }

    /**
     * Sets the custom extensions.
     *
     * @param customExtensions The custom extensions.
     * @return This builder.
     */
    public Builder withCustomExtensions(List<CustomExtension> customExtensions) {
      this.customExtensions = customExtensions;
      return this;
    }

    /**
     * @return The metric.
     */
    public Metric build() {
      Preconditions.checkArgument(expression != null, "expression must not be null");
      return new Metric(
          SemanticModelUtils.requireNonBlank(name, "name"),
          expression,
          description,
          aiContext,
          SemanticModelUtils.immutableList(customExtensions, "customExtensions"));
    }
  }
}
