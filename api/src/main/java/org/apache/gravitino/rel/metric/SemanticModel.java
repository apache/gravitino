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

import java.util.List;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.gravitino.annotation.Unstable;

/** A structured, OSI-compatible semantic model carried by a Metric View. */
@Unstable
public final class SemanticModel {

  private final String name;
  @Nullable private final String description;
  @Nullable private final AIContext aiContext;
  private final List<Dataset> datasets;
  private final List<Relationship> relationships;
  private final List<Metric> metrics;
  private final List<CustomExtension> customExtensions;

  private SemanticModel(
      String name,
      @Nullable String description,
      @Nullable AIContext aiContext,
      List<Dataset> datasets,
      List<Relationship> relationships,
      List<Metric> metrics,
      List<CustomExtension> customExtensions) {
    this.name = name;
    this.description = description;
    this.aiContext = aiContext;
    this.datasets = datasets;
    this.relationships = relationships;
    this.metrics = metrics;
    this.customExtensions = customExtensions;
  }

  /**
   * @return A new semantic-model builder.
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * @return The semantic-model name.
   */
  public String name() {
    return name;
  }

  /**
   * @return The semantic-model description, or {@code null}.
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
   * @return An immutable, non-empty list of datasets.
   */
  public List<Dataset> datasets() {
    return datasets;
  }

  /**
   * @return An immutable list of relationships.
   */
  public List<Relationship> relationships() {
    return relationships;
  }

  /**
   * @return An immutable list of metrics.
   */
  public List<Metric> metrics() {
    return metrics;
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
    if (!(other instanceof SemanticModel)) {
      return false;
    }
    SemanticModel that = (SemanticModel) other;
    return Objects.equals(name, that.name)
        && Objects.equals(description, that.description)
        && Objects.equals(aiContext, that.aiContext)
        && Objects.equals(datasets, that.datasets)
        && Objects.equals(relationships, that.relationships)
        && Objects.equals(metrics, that.metrics)
        && Objects.equals(customExtensions, that.customExtensions);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        name, description, aiContext, datasets, relationships, metrics, customExtensions);
  }

  @Override
  public String toString() {
    return "SemanticModel{"
        + "name='"
        + name
        + '\''
        + ", description='"
        + description
        + '\''
        + ", aiContext="
        + aiContext
        + ", datasets="
        + datasets
        + ", relationships="
        + relationships
        + ", metrics="
        + metrics
        + ", customExtensions="
        + customExtensions
        + '}';
  }

  /** Builder for {@link SemanticModel}. */
  public static final class Builder {
    private String name;
    @Nullable private String description;
    @Nullable private AIContext aiContext;
    private List<Dataset> datasets;
    private List<Relationship> relationships;
    private List<Metric> metrics;
    private List<CustomExtension> customExtensions;

    private Builder() {}

    /**
     * Sets the semantic-model name.
     *
     * @param name The semantic-model name.
     * @return This builder.
     */
    public Builder withName(String name) {
      this.name = name;
      return this;
    }

    /**
     * Sets the semantic-model description.
     *
     * @param description The semantic-model description.
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
     * Sets the datasets.
     *
     * @param datasets The datasets.
     * @return This builder.
     */
    public Builder withDatasets(List<Dataset> datasets) {
      this.datasets = datasets;
      return this;
    }

    /**
     * Sets the relationships.
     *
     * @param relationships The relationships.
     * @return This builder.
     */
    public Builder withRelationships(List<Relationship> relationships) {
      this.relationships = relationships;
      return this;
    }

    /**
     * Sets the metrics.
     *
     * @param metrics The metrics.
     * @return This builder.
     */
    public Builder withMetrics(List<Metric> metrics) {
      this.metrics = metrics;
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
     * @return The semantic model.
     */
    public SemanticModel build() {
      return new SemanticModel(
          SemanticModelUtils.requireNonBlank(name, "name"),
          description,
          aiContext,
          SemanticModelUtils.requireNonEmptyList(datasets, "datasets"),
          SemanticModelUtils.immutableList(relationships, "relationships"),
          SemanticModelUtils.immutableList(metrics, "metrics"),
          SemanticModelUtils.immutableList(customExtensions, "customExtensions"));
    }
  }
}
