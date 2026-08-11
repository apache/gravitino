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
 * An immutable relationship between two datasets in the same semantic model. The endpoint column
 * arrays describe the corresponding join columns on the source and target datasets.
 */
@Evolving
public final class Relationship {

  private final String name;
  private final String from;
  private final String to;
  private final String[] fromColumns;
  private final String[] toColumns;

  @Nullable private final AIContext aiContext;
  @Nullable private final CustomExtension[] customExtensions;

  private Relationship(Builder builder) {
    this.name = builder.name;
    this.from = builder.from;
    this.to = builder.to;
    this.fromColumns = Arrays.copyOf(builder.fromColumns, builder.fromColumns.length);
    this.toColumns = Arrays.copyOf(builder.toColumns, builder.toColumns.length);
    this.aiContext = builder.aiContext;
    this.customExtensions =
        builder.customExtensions == null
            ? null
            : Arrays.copyOf(builder.customExtensions, builder.customExtensions.length);
  }

  /**
   * Creates a builder for an immutable {@link Relationship}.
   *
   * @return A new builder.
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Returns the relationship name.
   *
   * @return The relationship name.
   */
  public String name() {
    return name;
  }

  /**
   * Returns the name of the source dataset.
   *
   * @return The source dataset name.
   */
  public String from() {
    return from;
  }

  /**
   * Returns the name of the target dataset.
   *
   * @return The target dataset name.
   */
  public String to() {
    return to;
  }

  /**
   * Returns a copy of the source dataset columns.
   *
   * @return The source columns.
   */
  public String[] fromColumns() {
    return Arrays.copyOf(fromColumns, fromColumns.length);
  }

  /**
   * Returns a copy of the target dataset columns.
   *
   * @return The target columns.
   */
  public String[] toColumns() {
    return Arrays.copyOf(toColumns, toColumns.length);
  }

  /**
   * Returns the AI context associated with the relationship.
   *
   * @return The AI context, or {@code null} if it is not set.
   */
  @Nullable
  public AIContext aiContext() {
    return aiContext;
  }

  /**
   * Returns a copy of the custom extensions associated with the relationship.
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
   * Compares this relationship with another object for value equality.
   *
   * @param other The object to compare with.
   * @return {@code true} if the objects are equal, otherwise {@code false}.
   */
  @Override
  public boolean equals(@Nullable Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof Relationship)) {
      return false;
    }
    Relationship that = (Relationship) other;
    return name.equals(that.name)
        && from.equals(that.from)
        && to.equals(that.to)
        && Arrays.equals(fromColumns, that.fromColumns)
        && Arrays.equals(toColumns, that.toColumns)
        && Objects.equals(aiContext, that.aiContext)
        && Arrays.equals(customExtensions, that.customExtensions);
  }

  /**
   * Returns the value-based hash code for this relationship.
   *
   * @return The hash code.
   */
  @Override
  public int hashCode() {
    return Objects.hash(
        name,
        from,
        to,
        Arrays.hashCode(fromColumns),
        Arrays.hashCode(toColumns),
        aiContext,
        Arrays.hashCode(customExtensions));
  }

  /**
   * Returns a string representation of this relationship.
   *
   * @return The string representation.
   */
  @Override
  public String toString() {
    return "Relationship{"
        + "name='"
        + name
        + '\''
        + ", from='"
        + from
        + '\''
        + ", to='"
        + to
        + '\''
        + ", fromColumns="
        + Arrays.toString(fromColumns)
        + ", toColumns="
        + Arrays.toString(toColumns)
        + ", aiContext="
        + aiContext
        + ", customExtensions="
        + Arrays.toString(customExtensions)
        + '}';
  }

  /** A builder for immutable {@link Relationship} values. */
  public static final class Builder {

    private String name;
    private String from;
    private String to;
    private String[] fromColumns;
    private String[] toColumns;

    @Nullable private AIContext aiContext;
    @Nullable private CustomExtension[] customExtensions;

    private Builder() {}

    /**
     * Sets the relationship name.
     *
     * @param name The non-empty relationship name.
     * @return This builder.
     */
    public Builder withName(String name) {
      this.name = name;
      return this;
    }

    /**
     * Sets the source dataset name.
     *
     * @param from The non-empty source dataset name.
     * @return This builder.
     */
    public Builder withFrom(String from) {
      this.from = from;
      return this;
    }

    /**
     * Sets the target dataset name.
     *
     * @param to The non-empty target dataset name.
     * @return This builder.
     */
    public Builder withTo(String to) {
      this.to = to;
      return this;
    }

    /**
     * Sets the source dataset columns.
     *
     * @param fromColumns The non-empty source column array.
     * @return This builder.
     */
    public Builder withFromColumns(String[] fromColumns) {
      this.fromColumns = fromColumns;
      return this;
    }

    /**
     * Sets the target dataset columns.
     *
     * @param toColumns The non-empty target column array.
     * @return This builder.
     */
    public Builder withToColumns(String[] toColumns) {
      this.toColumns = toColumns;
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
     * Builds an immutable {@link Relationship}.
     *
     * @return The new relationship.
     * @throws IllegalArgumentException If a required name is null or empty, a required column array
     *     is null or empty, a column name or custom extension is null, or the endpoint column
     *     arrays have different lengths.
     */
    public Relationship build() {
      Preconditions.checkArgument(
          name != null && !name.isEmpty(), "name must not be null or empty");
      Preconditions.checkArgument(
          from != null && !from.isEmpty(), "from must not be null or empty");
      Preconditions.checkArgument(to != null && !to.isEmpty(), "to must not be null or empty");
      Preconditions.checkArgument(
          fromColumns != null && fromColumns.length > 0, "fromColumns must not be null or empty");
      Preconditions.checkArgument(
          toColumns != null && toColumns.length > 0, "toColumns must not be null or empty");
      validateColumnNames("fromColumns", fromColumns);
      validateColumnNames("toColumns", toColumns);
      Preconditions.checkArgument(
          fromColumns.length == toColumns.length,
          "fromColumns and toColumns must have the same length");
      Preconditions.checkArgument(
          customExtensions == null || Arrays.stream(customExtensions).allMatch(Objects::nonNull),
          "customExtensions must not contain null");
      return new Relationship(this);
    }
  }

  private static void validateColumnNames(String name, String[] columns) {
    for (int index = 0; index < columns.length; index++) {
      Preconditions.checkArgument(
          columns[index] != null && !columns[index].isEmpty(),
          "%s[%s] must not be null or empty",
          name,
          index);
    }
  }
}
