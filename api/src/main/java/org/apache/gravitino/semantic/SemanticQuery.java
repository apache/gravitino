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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.annotation.Evolving;

/**
 * An immutable semantic query that a {@link SemanticQueryPlanner} compiles to SQL for an engine.
 *
 * <p>A query selects metrics and dimensions and may restrict them with filters. The dialect is a
 * non-blank identifier chosen by the caller; Gravitino preserves it without implicit conversion so
 * consumers can use engines and custom dialect names the server does not know about.
 */
@Evolving
public final class SemanticQuery {

  private final List<String> metrics;
  private final List<String> dimensions;
  private final List<SemanticQueryFilter> filters;
  private final String dialect;

  private SemanticQuery(Builder builder) {
    Preconditions.checkArgument(
        StringUtils.isNotBlank(builder.dialect), "dialect must not be null or blank");
    Preconditions.checkArgument(
        builder.metrics != null, "metrics must not be null; use an empty list to omit them");
    Preconditions.checkArgument(
        builder.dimensions != null, "dimensions must not be null; use an empty list to omit them");
    Preconditions.checkArgument(
        builder.filters != null, "filters must not be null; use an empty list to omit them");
    validateNames("metrics", builder.metrics);
    validateNames("dimensions", builder.dimensions);
    for (int index = 0; index < builder.filters.size(); index++) {
      Preconditions.checkArgument(
          builder.filters.get(index) != null, "filters[%s] must not be null", index);
    }
    Preconditions.checkArgument(
        !builder.metrics.isEmpty() || !builder.dimensions.isEmpty(),
        "a semantic query must select at least one metric or dimension");

    this.metrics = Collections.unmodifiableList(new ArrayList<>(builder.metrics));
    this.dimensions = Collections.unmodifiableList(new ArrayList<>(builder.dimensions));
    this.filters = Collections.unmodifiableList(new ArrayList<>(builder.filters));
    this.dialect = builder.dialect;
  }

  /**
   * Creates a builder for an immutable {@link SemanticQuery}.
   *
   * @return A new builder.
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Returns the selected metric names.
   *
   * @return An unmodifiable list of metric names; empty if no metrics are selected.
   */
  public List<String> metrics() {
    return metrics;
  }

  /**
   * Returns the selected dimension names.
   *
   * @return An unmodifiable list of dimension names; empty if no dimensions are selected.
   */
  public List<String> dimensions() {
    return dimensions;
  }

  /**
   * Returns the filters applied to the query.
   *
   * @return An unmodifiable list of filters; empty if the query has no filters.
   */
  public List<SemanticQueryFilter> filters() {
    return filters;
  }

  /**
   * Returns the target engine dialect.
   *
   * @return The non-blank dialect identifier.
   */
  public String dialect() {
    return dialect;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof SemanticQuery)) {
      return false;
    }
    SemanticQuery that = (SemanticQuery) other;
    return metrics.equals(that.metrics)
        && dimensions.equals(that.dimensions)
        && filters.equals(that.filters)
        && dialect.equals(that.dialect);
  }

  @Override
  public int hashCode() {
    return Objects.hash(metrics, dimensions, filters, dialect);
  }

  @Override
  public String toString() {
    return "SemanticQuery{"
        + "metrics="
        + metrics
        + ", dimensions="
        + dimensions
        + ", filters="
        + filters
        + ", dialect='"
        + dialect
        + '\''
        + '}';
  }

  private static void validateNames(String name, List<String> values) {
    for (int index = 0; index < values.size(); index++) {
      Preconditions.checkArgument(
          StringUtils.isNotBlank(values.get(index)),
          "%s[%s] must not be null or blank",
          name,
          index);
    }
  }

  /** A builder for immutable {@link SemanticQuery} values. */
  public static final class Builder {

    private List<String> metrics;
    private List<String> dimensions;
    private List<SemanticQueryFilter> filters;
    private String dialect;

    private Builder() {}

    /**
     * Sets the selected metric names.
     *
     * @param metrics The metric names, or an empty list to select none.
     * @return This builder.
     */
    public Builder withMetrics(List<String> metrics) {
      this.metrics = metrics;
      return this;
    }

    /**
     * Sets the selected dimension names.
     *
     * @param dimensions The dimension names, or an empty list to select none.
     * @return This builder.
     */
    public Builder withDimensions(List<String> dimensions) {
      this.dimensions = dimensions;
      return this;
    }

    /**
     * Sets the filters applied to the query.
     *
     * @param filters The filters, or an empty list to apply none.
     * @return This builder.
     */
    public Builder withFilters(List<SemanticQueryFilter> filters) {
      this.filters = filters;
      return this;
    }

    /**
     * Sets the target engine dialect.
     *
     * @param dialect The non-blank dialect identifier.
     * @return This builder.
     */
    public Builder withDialect(String dialect) {
      this.dialect = dialect;
      return this;
    }

    /**
     * Builds an immutable {@link SemanticQuery}.
     *
     * @return The query.
     * @throws IllegalArgumentException If the query is invalid.
     */
    public SemanticQuery build() {
      return new SemanticQuery(this);
    }
  }
}
