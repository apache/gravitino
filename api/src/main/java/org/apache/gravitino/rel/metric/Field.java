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

/** A named field in a semantic-model dataset. */
@Unstable
public final class Field {

  private final String name;
  private final Expression expression;
  @Nullable private final Dimension dimension;
  @Nullable private final String label;
  @Nullable private final String description;
  @Nullable private final AIContext aiContext;
  private final List<CustomExtension> customExtensions;

  private Field(
      String name,
      Expression expression,
      @Nullable Dimension dimension,
      @Nullable String label,
      @Nullable String description,
      @Nullable AIContext aiContext,
      List<CustomExtension> customExtensions) {
    this.name = name;
    this.expression = expression;
    this.dimension = dimension;
    this.label = label;
    this.description = description;
    this.aiContext = aiContext;
    this.customExtensions = customExtensions;
  }

  /**
   * Creates a builder for a field.
   *
   * @return A new builder.
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * @return The field name.
   */
  public String name() {
    return name;
  }

  /**
   * @return The field expression.
   */
  public Expression expression() {
    return expression;
  }

  /**
   * @return The dimension metadata, or {@code null}.
   */
  @Nullable
  public Dimension dimension() {
    return dimension;
  }

  /**
   * @return The display label, or {@code null}.
   */
  @Nullable
  public String label() {
    return label;
  }

  /**
   * @return The field description, or {@code null}.
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
    if (!(other instanceof Field)) {
      return false;
    }
    Field that = (Field) other;
    return Objects.equals(name, that.name)
        && Objects.equals(expression, that.expression)
        && Objects.equals(dimension, that.dimension)
        && Objects.equals(label, that.label)
        && Objects.equals(description, that.description)
        && Objects.equals(aiContext, that.aiContext)
        && Objects.equals(customExtensions, that.customExtensions);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        name, expression, dimension, label, description, aiContext, customExtensions);
  }

  @Override
  public String toString() {
    return "Field{"
        + "name='"
        + name
        + '\''
        + ", expression="
        + expression
        + ", dimension="
        + dimension
        + ", label='"
        + label
        + '\''
        + ", description='"
        + description
        + '\''
        + ", aiContext="
        + aiContext
        + ", customExtensions="
        + customExtensions
        + '}';
  }

  /** Builder for {@link Field}. */
  public static final class Builder {
    private String name;
    private Expression expression;
    @Nullable private Dimension dimension;
    @Nullable private String label;
    @Nullable private String description;
    @Nullable private AIContext aiContext;
    private List<CustomExtension> customExtensions;

    private Builder() {}

    /**
     * Sets the field name.
     *
     * @param name The field name.
     * @return This builder.
     */
    public Builder withName(String name) {
      this.name = name;
      return this;
    }

    /**
     * Sets the field expression.
     *
     * @param expression The field expression.
     * @return This builder.
     */
    public Builder withExpression(Expression expression) {
      this.expression = expression;
      return this;
    }

    /**
     * Sets the dimension metadata.
     *
     * @param dimension The dimension metadata.
     * @return This builder.
     */
    public Builder withDimension(@Nullable Dimension dimension) {
      this.dimension = dimension;
      return this;
    }

    /**
     * Sets the display label.
     *
     * @param label The display label.
     * @return This builder.
     */
    public Builder withLabel(@Nullable String label) {
      this.label = label;
      return this;
    }

    /**
     * Sets the field description.
     *
     * @param description The field description.
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
     * @return The field.
     */
    public Field build() {
      Preconditions.checkArgument(expression != null, "expression must not be null");
      return new Field(
          SemanticModelUtils.requireNonBlank(name, "name"),
          expression,
          dimension,
          label,
          description,
          aiContext,
          SemanticModelUtils.immutableList(customExtensions, "customExtensions"));
    }
  }
}
