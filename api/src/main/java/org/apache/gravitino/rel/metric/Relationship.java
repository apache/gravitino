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

/** A named relationship between two datasets in a semantic model. */
@Unstable
public final class Relationship {

  private final String name;
  private final String from;
  private final String to;
  private final List<String> fromColumns;
  private final List<String> toColumns;
  @Nullable private final AIContext aiContext;
  private final List<CustomExtension> customExtensions;

  private Relationship(
      String name,
      String from,
      String to,
      List<String> fromColumns,
      List<String> toColumns,
      @Nullable AIContext aiContext,
      List<CustomExtension> customExtensions) {
    this.name = name;
    this.from = from;
    this.to = to;
    this.fromColumns = fromColumns;
    this.toColumns = toColumns;
    this.aiContext = aiContext;
    this.customExtensions = customExtensions;
  }

  /**
   * Creates a relationship builder.
   *
   * @return A new builder.
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * @return The relationship name.
   */
  public String name() {
    return name;
  }

  /**
   * @return The source dataset name.
   */
  public String from() {
    return from;
  }

  /**
   * @return The target dataset name.
   */
  public String to() {
    return to;
  }

  /**
   * @return An immutable list of source columns.
   */
  public List<String> fromColumns() {
    return fromColumns;
  }

  /**
   * @return An immutable list of target columns.
   */
  public List<String> toColumns() {
    return toColumns;
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
    if (!(other instanceof Relationship)) {
      return false;
    }
    Relationship that = (Relationship) other;
    return Objects.equals(name, that.name)
        && Objects.equals(from, that.from)
        && Objects.equals(to, that.to)
        && Objects.equals(fromColumns, that.fromColumns)
        && Objects.equals(toColumns, that.toColumns)
        && Objects.equals(aiContext, that.aiContext)
        && Objects.equals(customExtensions, that.customExtensions);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, from, to, fromColumns, toColumns, aiContext, customExtensions);
  }

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
        + fromColumns
        + ", toColumns="
        + toColumns
        + ", aiContext="
        + aiContext
        + ", customExtensions="
        + customExtensions
        + '}';
  }

  /** Builder for {@link Relationship}. */
  public static final class Builder {
    private String name;
    private String from;
    private String to;
    private List<String> fromColumns;
    private List<String> toColumns;
    @Nullable private AIContext aiContext;
    private List<CustomExtension> customExtensions;

    private Builder() {}

    /**
     * Sets the relationship name.
     *
     * @param name The relationship name.
     * @return This builder.
     */
    public Builder withName(String name) {
      this.name = name;
      return this;
    }

    /**
     * Sets the source dataset name.
     *
     * @param from The source dataset name.
     * @return This builder.
     */
    public Builder withFrom(String from) {
      this.from = from;
      return this;
    }

    /**
     * Sets the target dataset name.
     *
     * @param to The target dataset name.
     * @return This builder.
     */
    public Builder withTo(String to) {
      this.to = to;
      return this;
    }

    /**
     * Sets the source columns.
     *
     * @param fromColumns The source columns.
     * @return This builder.
     */
    public Builder withFromColumns(List<String> fromColumns) {
      this.fromColumns = fromColumns;
      return this;
    }

    /**
     * Sets the target columns.
     *
     * @param toColumns The target columns.
     * @return This builder.
     */
    public Builder withToColumns(List<String> toColumns) {
      this.toColumns = toColumns;
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
     * Builds the relationship.
     *
     * @return The relationship.
     */
    public Relationship build() {
      List<String> copiedFromColumns =
          SemanticModelUtils.requireNonEmptyList(fromColumns, "fromColumns");
      List<String> copiedToColumns = SemanticModelUtils.requireNonEmptyList(toColumns, "toColumns");
      Preconditions.checkArgument(
          copiedFromColumns.size() == copiedToColumns.size(),
          "fromColumns and toColumns must have the same size");
      return new Relationship(
          SemanticModelUtils.requireNonBlank(name, "name"),
          SemanticModelUtils.requireNonBlank(from, "from"),
          SemanticModelUtils.requireNonBlank(to, "to"),
          copiedFromColumns,
          copiedToColumns,
          aiContext,
          SemanticModelUtils.immutableList(customExtensions, "customExtensions"));
    }
  }
}
