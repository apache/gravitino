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
 * An immutable semantic field in a {@link Dataset}. A field gives a name to an {@link Expression}
 * and may carry dimension, display, type, AI, and extension metadata.
 */
@Evolving
public final class Field {

  private final String name;
  private final Expression expression;

  @Nullable private final Dimension dimension;
  @Nullable private final String label;
  @Nullable private final String description;
  @Nullable private final DataType datatype;
  @Nullable private final AIContext aiContext;
  @Nullable private final CustomExtension[] customExtensions;

  private Field(Builder builder) {
    this.name = builder.name;
    this.expression = builder.expression;
    this.dimension = builder.dimension;
    this.label = builder.label;
    this.description = builder.description;
    this.datatype = builder.datatype;
    this.aiContext = builder.aiContext;
    this.customExtensions =
        builder.customExtensions == null
            ? null
            : Arrays.copyOf(builder.customExtensions, builder.customExtensions.length);
  }

  /**
   * Creates a builder for an immutable {@link Field}.
   *
   * @return A new builder.
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Returns the field name.
   *
   * @return The field name.
   */
  public String name() {
    return name;
  }

  /**
   * Returns the expression that defines the field.
   *
   * @return The field expression.
   */
  public Expression expression() {
    return expression;
  }

  /**
   * Returns the dimension metadata for the field.
   *
   * @return The dimension metadata, or {@code null} if it is not set.
   */
  @Nullable
  public Dimension dimension() {
    return dimension;
  }

  /**
   * Returns the display label for the field.
   *
   * @return The field label, or {@code null} if it is not set.
   */
  @Nullable
  public String label() {
    return label;
  }

  /**
   * Returns the field description.
   *
   * @return The field description, or {@code null} if it is not set.
   */
  @Nullable
  public String description() {
    return description;
  }

  /**
   * Returns the semantic data type of the field.
   *
   * @return The semantic data type, or {@code null} if it is not set.
   */
  @Nullable
  public DataType datatype() {
    return datatype;
  }

  /**
   * Returns the AI context associated with the field.
   *
   * @return The AI context, or {@code null} if it is not set.
   */
  @Nullable
  public AIContext aiContext() {
    return aiContext;
  }

  /**
   * Returns a copy of the custom extensions associated with the field.
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
   * Compares this field with another object for value equality.
   *
   * @param other The object to compare with.
   * @return {@code true} if the objects are equal, otherwise {@code false}.
   */
  @Override
  public boolean equals(@Nullable Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof Field)) {
      return false;
    }
    Field that = (Field) other;
    return name.equals(that.name)
        && expression.equals(that.expression)
        && Objects.equals(dimension, that.dimension)
        && Objects.equals(label, that.label)
        && Objects.equals(description, that.description)
        && datatype == that.datatype
        && Objects.equals(aiContext, that.aiContext)
        && Arrays.equals(customExtensions, that.customExtensions);
  }

  /**
   * Returns the value-based hash code for this field.
   *
   * @return The hash code.
   */
  @Override
  public int hashCode() {
    return Objects.hash(
        name,
        expression,
        dimension,
        label,
        description,
        datatype,
        aiContext,
        Arrays.hashCode(customExtensions));
  }

  /**
   * Returns a string representation of this field.
   *
   * @return The string representation.
   */
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
        + ", datatype="
        + datatype
        + ", aiContext="
        + aiContext
        + ", customExtensions="
        + Arrays.toString(customExtensions)
        + '}';
  }

  /** A builder for immutable {@link Field} values. */
  public static final class Builder {

    private String name;
    private Expression expression;

    @Nullable private Dimension dimension;
    @Nullable private String label;
    @Nullable private String description;
    @Nullable private DataType datatype;
    @Nullable private AIContext aiContext;
    @Nullable private CustomExtension[] customExtensions;

    private Builder() {}

    /**
     * Sets the field name.
     *
     * @param name The non-empty field name.
     * @return This builder.
     */
    public Builder withName(String name) {
      this.name = name;
      return this;
    }

    /**
     * Sets the expression that defines the field.
     *
     * @param expression The field expression.
     * @return This builder.
     */
    public Builder withExpression(Expression expression) {
      this.expression = expression;
      return this;
    }

    /**
     * Sets the optional dimension metadata.
     *
     * @param dimension The dimension metadata, or {@code null} to leave it unset.
     * @return This builder.
     */
    public Builder withDimension(@Nullable Dimension dimension) {
      this.dimension = dimension;
      return this;
    }

    /**
     * Sets the optional display label.
     *
     * @param label The display label, or {@code null} to leave it unset.
     * @return This builder.
     */
    public Builder withLabel(@Nullable String label) {
      this.label = label;
      return this;
    }

    /**
     * Sets the optional field description.
     *
     * @param description The field description, or {@code null} to leave it unset.
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
     * Builds an immutable {@link Field}.
     *
     * @return The new field.
     * @throws IllegalArgumentException If the name is null or empty, the expression is null, or the
     *     custom extension array contains null.
     */
    public Field build() {
      Preconditions.checkArgument(
          name != null && !name.isEmpty(), "name must not be null or empty");
      Preconditions.checkArgument(expression != null, "expression must not be null");
      Preconditions.checkArgument(
          customExtensions == null || Arrays.stream(customExtensions).allMatch(Objects::nonNull),
          "customExtensions must not contain null");
      return new Field(this);
    }
  }
}
