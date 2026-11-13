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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.annotation.Evolving;

/**
 * A dialect-neutral logical plan for one {@link SemanticQuery}.
 *
 * <p>The plan is a single relational block. It names the physical sources and join structure, but
 * it never contains SQL text, alias names, quoting, literal syntax or a dialect choice. A {@link
 * SemanticQueryPlanConverter} turns it into an engine-specific plan.
 */
@Evolving
public final class SemanticQueryPlan {

  private final NameIdentifier semanticModel;
  private final long semanticModelVersion;
  private final List<PlanSource> sources;
  private final List<PlanJoin> joins;
  private final List<PlanSelection> selections;
  private final List<PlanFilter> filters;
  private final List<String> groupBy;

  @Nullable private final Long limit;
  private final List<String> warnings;

  private SemanticQueryPlan(Builder builder) {
    this.semanticModel = builder.semanticModel;
    this.semanticModelVersion = builder.semanticModelVersion;
    this.sources = Collections.unmodifiableList(new ArrayList<>(builder.sources));
    this.joins = Collections.unmodifiableList(new ArrayList<>(builder.joins));
    this.selections = Collections.unmodifiableList(new ArrayList<>(builder.selections));
    this.filters = Collections.unmodifiableList(new ArrayList<>(builder.filters));
    this.groupBy = Collections.unmodifiableList(new ArrayList<>(builder.groupBy));
    this.limit = builder.limit;
    this.warnings = Collections.unmodifiableList(new ArrayList<>(builder.warnings));
  }

  /**
   * Creates a builder for an immutable {@link SemanticQueryPlan}.
   *
   * @return A new builder.
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Returns the complete identifier of the Semantic Model the plan was resolved against.
   *
   * @return The Semantic Model identifier.
   */
  public NameIdentifier semanticModel() {
    return semanticModel;
  }

  /**
   * Returns the version of the definition the plan was resolved against.
   *
   * @return The monotonically increasing Semantic Model version.
   */
  public long semanticModelVersion() {
    return semanticModelVersion;
  }

  /**
   * Returns the datasets the query needs, in join order.
   *
   * @return An unmodifiable list of plan sources.
   */
  public List<PlanSource> sources() {
    return sources;
  }

  /**
   * Returns the joins that connect the sources.
   *
   * @return An unmodifiable list of joins; each one joins to a dataset already in scope.
   */
  public List<PlanJoin> joins() {
    return joins;
  }

  /**
   * Returns the selected metrics then dimensions, in request order.
   *
   * @return An unmodifiable list of selections.
   */
  public List<PlanSelection> selections() {
    return selections;
  }

  /**
   * Returns the filters, ANDed together in request order.
   *
   * @return An unmodifiable list of filters.
   */
  public List<PlanFilter> filters() {
    return filters;
  }

  /**
   * Returns the selection names that form the grouping set.
   *
   * @return The non-aggregate selection names, in selection order.
   */
  public List<String> groupBy() {
    return groupBy;
  }

  /**
   * Returns the maximum number of rows to return.
   *
   * @return The non-negative row limit, or {@code null} for no limit.
   */
  @Nullable
  public Long limit() {
    return limit;
  }

  /**
   * Returns non-fatal warnings raised while resolving.
   *
   * @return An unmodifiable list of warnings; empty when there are none.
   */
  public List<String> warnings() {
    return warnings;
  }

  @Override
  public boolean equals(@Nullable Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof SemanticQueryPlan)) {
      return false;
    }
    SemanticQueryPlan that = (SemanticQueryPlan) other;
    return semanticModelVersion == that.semanticModelVersion
        && semanticModel.equals(that.semanticModel)
        && sources.equals(that.sources)
        && joins.equals(that.joins)
        && selections.equals(that.selections)
        && filters.equals(that.filters)
        && groupBy.equals(that.groupBy)
        && Objects.equals(limit, that.limit)
        && warnings.equals(that.warnings);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        semanticModel,
        semanticModelVersion,
        sources,
        joins,
        selections,
        filters,
        groupBy,
        limit,
        warnings);
  }

  @Override
  public String toString() {
    return "SemanticQueryPlan{"
        + "semanticModel="
        + semanticModel
        + ", semanticModelVersion="
        + semanticModelVersion
        + ", sources="
        + sources
        + ", joins="
        + joins
        + ", selections="
        + selections
        + ", filters="
        + filters
        + ", groupBy="
        + groupBy
        + ", limit="
        + limit
        + ", warnings="
        + warnings
        + '}';
  }

  /** A builder for immutable {@link SemanticQueryPlan} values. */
  public static final class Builder {

    private NameIdentifier semanticModel;
    private long semanticModelVersion = -1;
    private List<PlanSource> sources = Collections.emptyList();
    private List<PlanJoin> joins = Collections.emptyList();
    private List<PlanSelection> selections = Collections.emptyList();
    private List<PlanFilter> filters = Collections.emptyList();
    private List<String> groupBy = Collections.emptyList();

    @Nullable private Long limit;
    private List<String> warnings = Collections.emptyList();

    private Builder() {}

    /**
     * Sets the Semantic Model identifier.
     *
     * @param semanticModel The complete Semantic Model identifier.
     * @return This builder.
     */
    public Builder withSemanticModel(NameIdentifier semanticModel) {
      this.semanticModel = semanticModel;
      return this;
    }

    /**
     * Sets the Semantic Model version the plan was resolved against.
     *
     * @param semanticModelVersion The positive, monotonically increasing version.
     * @return This builder.
     */
    public Builder withSemanticModelVersion(long semanticModelVersion) {
      this.semanticModelVersion = semanticModelVersion;
      return this;
    }

    /**
     * Sets the datasets the query needs, in join order.
     *
     * @param sources The plan sources.
     * @return This builder.
     */
    public Builder withSources(List<PlanSource> sources) {
      this.sources = sources;
      return this;
    }

    /**
     * Sets the joins that connect the sources.
     *
     * @param joins The plan joins.
     * @return This builder.
     */
    public Builder withJoins(List<PlanJoin> joins) {
      this.joins = joins;
      return this;
    }

    /**
     * Sets the selected metrics then dimensions, in request order.
     *
     * @param selections The plan selections.
     * @return This builder.
     */
    public Builder withSelections(List<PlanSelection> selections) {
      this.selections = selections;
      return this;
    }

    /**
     * Sets the filters, ANDed together in request order.
     *
     * @param filters The plan filters.
     * @return This builder.
     */
    public Builder withFilters(List<PlanFilter> filters) {
      this.filters = filters;
      return this;
    }

    /**
     * Sets the selection names that form the grouping set.
     *
     * @param groupBy The non-aggregate selection names, in selection order.
     * @return This builder.
     */
    public Builder withGroupBy(List<String> groupBy) {
      this.groupBy = groupBy;
      return this;
    }

    /**
     * Sets the maximum number of rows to return.
     *
     * @param limit The non-negative row limit, or {@code null} for no limit.
     * @return This builder.
     */
    public Builder withLimit(@Nullable Long limit) {
      this.limit = limit;
      return this;
    }

    /**
     * Sets non-fatal warnings raised while resolving.
     *
     * @param warnings The warnings, or an empty list for none.
     * @return This builder.
     */
    public Builder withWarnings(List<String> warnings) {
      this.warnings = warnings;
      return this;
    }

    /**
     * Builds and validates an immutable {@link SemanticQueryPlan}.
     *
     * @return The plan.
     * @throws IllegalArgumentException If the plan violates a contract invariant.
     */
    public SemanticQueryPlan build() {
      Preconditions.checkArgument(semanticModel != null, "semanticModel must not be null");
      Preconditions.checkArgument(
          semanticModelVersion > 0, "semanticModelVersion must be positive");
      Preconditions.checkArgument(
          sources != null && !sources.isEmpty(), "sources must not be null or empty");
      Preconditions.checkArgument(
          selections != null && !selections.isEmpty(), "selections must not be null or empty");
      Preconditions.checkArgument(joins != null, "joins must not be null");
      Preconditions.checkArgument(filters != null, "filters must not be null");
      Preconditions.checkArgument(groupBy != null, "groupBy must not be null");
      Preconditions.checkArgument(warnings != null, "warnings must not be null");
      Preconditions.checkArgument(
          limit == null || limit >= 0, "limit must be null or non-negative");

      Set<String> sourceDatasets = new HashSet<>();
      for (int index = 0; index < sources.size(); index++) {
        PlanSource source = sources.get(index);
        Preconditions.checkArgument(source != null, "sources[%s] must not be null", index);
        Preconditions.checkArgument(
            sourceDatasets.add(source.dataset()),
            "sources must not contain duplicate dataset: %s",
            source.dataset());
      }

      Set<String> selectionNames = new HashSet<>();
      List<String> expectedGroupBy = new ArrayList<>();
      for (int index = 0; index < selections.size(); index++) {
        PlanSelection selection = selections.get(index);
        Preconditions.checkArgument(selection != null, "selections[%s] must not be null", index);
        Preconditions.checkArgument(
            selectionNames.add(selection.name()),
            "selection names must be unique: %s",
            selection.name());
        Preconditions.checkArgument(
            sourceDatasets.contains(selection.field().dataset()),
            "selection %s references dataset %s, which is not in sources",
            selection.name(),
            selection.field().dataset());
        if (!selection.aggregate()) {
          expectedGroupBy.add(selection.name());
        }
      }
      Preconditions.checkArgument(
          expectedGroupBy.equals(groupBy),
          "groupBy must equal the non-aggregate selection names in selection order");

      Map<String, Integer> sourceIndexes = new HashMap<>();
      for (int index = 0; index < sources.size(); index++) {
        sourceIndexes.put(sources.get(index).dataset(), index);
      }
      for (int index = 0; index < joins.size(); index++) {
        PlanJoin join = joins.get(index);
        Preconditions.checkArgument(join != null, "joins[%s] must not be null", index);
        Integer fromIndex = sourceIndexes.get(join.fromDataset());
        Integer toIndex = sourceIndexes.get(join.toDataset());
        Preconditions.checkArgument(
            fromIndex != null && toIndex != null,
            "join %s must reference datasets declared in sources",
            index);
        Preconditions.checkArgument(
            fromIndex < toIndex, "join %s must add a dataset that appears later in sources", index);
      }

      for (int index = 0; index < filters.size(); index++) {
        PlanFilter filter = filters.get(index);
        Preconditions.checkArgument(filter != null, "filters[%s] must not be null", index);
        Preconditions.checkArgument(
            sourceDatasets.contains(filter.field().dataset()),
            "filter %s references dataset %s, which is not in sources",
            index,
            filter.field().dataset());
      }

      for (int index = 0; index < warnings.size(); index++) {
        Preconditions.checkArgument(
            warnings.get(index) != null, "warnings[%s] must not be null", index);
      }

      return new SemanticQueryPlan(this);
    }
  }
}
