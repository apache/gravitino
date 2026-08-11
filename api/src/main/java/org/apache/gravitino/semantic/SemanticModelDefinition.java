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
 * An immutable Semantic Model definition. This value groups the Ossie-compatible definition fields
 * used when creating or replacing a Semantic Model and has no name or independent lifecycle.
 */
@Evolving
public final class SemanticModelDefinition {

  private final Dataset[] datasets;

  @Nullable private final AIContext aiContext;
  @Nullable private final Relationship[] relationships;
  @Nullable private final Metric[] metrics;
  @Nullable private final CustomExtension[] customExtensions;

  private SemanticModelDefinition(Builder builder) {
    this.aiContext = builder.aiContext;
    this.datasets = Arrays.copyOf(builder.datasets, builder.datasets.length);
    this.relationships =
        builder.relationships == null
            ? null
            : Arrays.copyOf(builder.relationships, builder.relationships.length);
    this.metrics =
        builder.metrics == null ? null : Arrays.copyOf(builder.metrics, builder.metrics.length);
    this.customExtensions =
        builder.customExtensions == null
            ? null
            : Arrays.copyOf(builder.customExtensions, builder.customExtensions.length);
  }

  /**
   * Creates a builder for an immutable {@link SemanticModelDefinition}.
   *
   * @return A new builder.
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Returns the AI context associated with the Semantic Model definition.
   *
   * @return The AI context, or {@code null} if it is not set.
   */
  @Nullable
  public AIContext aiContext() {
    return aiContext;
  }

  /**
   * Returns a copy of the datasets in the Semantic Model definition.
   *
   * @return The non-empty dataset array.
   */
  public Dataset[] datasets() {
    return Arrays.copyOf(datasets, datasets.length);
  }

  /**
   * Returns a copy of the relationships in the Semantic Model definition.
   *
   * @return The relationships, or {@code null} if they are not set.
   */
  @Nullable
  public Relationship[] relationships() {
    return relationships == null ? null : Arrays.copyOf(relationships, relationships.length);
  }

  /**
   * Returns a copy of the metrics in the Semantic Model definition.
   *
   * @return The metrics, or {@code null} if they are not set.
   */
  @Nullable
  public Metric[] metrics() {
    return metrics == null ? null : Arrays.copyOf(metrics, metrics.length);
  }

  /**
   * Returns a copy of the custom extensions in the Semantic Model definition.
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
   * Compares this definition with another object for value equality.
   *
   * @param other The object to compare with.
   * @return {@code true} if the objects are equal, otherwise {@code false}.
   */
  @Override
  public boolean equals(@Nullable Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof SemanticModelDefinition)) {
      return false;
    }
    SemanticModelDefinition that = (SemanticModelDefinition) other;
    return Objects.equals(aiContext, that.aiContext)
        && Arrays.equals(datasets, that.datasets)
        && Arrays.equals(relationships, that.relationships)
        && Arrays.equals(metrics, that.metrics)
        && Arrays.equals(customExtensions, that.customExtensions);
  }

  /**
   * Returns the value-based hash code for this definition.
   *
   * @return The hash code.
   */
  @Override
  public int hashCode() {
    return Objects.hash(
        aiContext,
        Arrays.hashCode(datasets),
        Arrays.hashCode(relationships),
        Arrays.hashCode(metrics),
        Arrays.hashCode(customExtensions));
  }

  /**
   * Returns a string representation of this definition.
   *
   * @return The string representation.
   */
  @Override
  public String toString() {
    return "SemanticModelDefinition{"
        + "aiContext="
        + aiContext
        + ", datasets="
        + Arrays.toString(datasets)
        + ", relationships="
        + Arrays.toString(relationships)
        + ", metrics="
        + Arrays.toString(metrics)
        + ", customExtensions="
        + Arrays.toString(customExtensions)
        + '}';
  }

  /** A builder for immutable {@link SemanticModelDefinition} values. */
  public static final class Builder {

    private Dataset[] datasets;

    @Nullable private AIContext aiContext;
    @Nullable private Relationship[] relationships;
    @Nullable private Metric[] metrics;
    @Nullable private CustomExtension[] customExtensions;

    private Builder() {}

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
     * Sets the datasets in the Semantic Model definition.
     *
     * @param datasets The non-empty dataset array.
     * @return This builder.
     */
    public Builder withDatasets(Dataset[] datasets) {
      this.datasets = datasets;
      return this;
    }

    /**
     * Sets the optional relationships.
     *
     * @param relationships The relationships, or {@code null} to leave them unset.
     * @return This builder.
     */
    public Builder withRelationships(@Nullable Relationship[] relationships) {
      this.relationships = relationships;
      return this;
    }

    /**
     * Sets the optional metrics.
     *
     * @param metrics The metrics, or {@code null} to leave them unset.
     * @return This builder.
     */
    public Builder withMetrics(@Nullable Metric[] metrics) {
      this.metrics = metrics;
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
     * Builds an immutable {@link SemanticModelDefinition}.
     *
     * @return The new definition.
     * @throws IllegalArgumentException If the dataset array is null or empty, or any definition
     *     array contains null.
     */
    public SemanticModelDefinition build() {
      Preconditions.checkArgument(
          datasets != null && datasets.length > 0, "datasets must not be null or empty");
      Preconditions.checkArgument(
          Arrays.stream(datasets).allMatch(Objects::nonNull), "datasets must not contain null");
      Preconditions.checkArgument(
          relationships == null || Arrays.stream(relationships).allMatch(Objects::nonNull),
          "relationships must not contain null");
      Preconditions.checkArgument(
          metrics == null || Arrays.stream(metrics).allMatch(Objects::nonNull),
          "metrics must not contain null");
      Preconditions.checkArgument(
          customExtensions == null || Arrays.stream(customExtensions).allMatch(Objects::nonNull),
          "customExtensions must not contain null");
      return new SemanticModelDefinition(this);
    }
  }
}
