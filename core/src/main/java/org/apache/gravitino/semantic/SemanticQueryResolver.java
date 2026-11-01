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
import com.google.errorprone.annotations.FormatMethod;
import com.google.errorprone.annotations.FormatString;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.exceptions.SemanticQueryPlanException;

/**
 * Resolves a {@link SemanticQuery} against one Semantic Model definition into a {@link
 * SemanticQueryPlan}.
 *
 * <p>The resolver owns every decision that needs model knowledge: metric ownership, field lookup,
 * ambiguity detection, the join path, aggregate classification, grouping and filters. It never
 * renders SQL or chooses a dialect.
 */
public final class SemanticQueryResolver {

  private static final Pattern QUALIFIED_REFERENCE =
      Pattern.compile("\\b([A-Za-z_][A-Za-z0-9_]*)\\.([A-Za-z_][A-Za-z0-9_]*)\\b");
  private static final Pattern IDENTIFIER = Pattern.compile("\\b[A-Za-z_][A-Za-z0-9_]*\\b");

  private SemanticQueryResolver() {}

  /**
   * Resolves a query against the current definition of a Semantic Model.
   *
   * @param semanticModel The complete Semantic Model identifier.
   * @param model The loaded Semantic Model.
   * @param query The query to resolve.
   * @return The dialect-neutral plan.
   * @throws IllegalArgumentException If the query or model cannot produce a valid plan.
   * @throws SemanticQueryPlanException If the model cannot satisfy the query.
   */
  public static SemanticQueryPlan resolve(
      NameIdentifier semanticModel, SemanticModel model, SemanticQuery query) {
    Preconditions.checkArgument(semanticModel != null, "semanticModel must not be null");
    Preconditions.checkArgument(model != null, "model must not be null");
    Preconditions.checkArgument(query != null, "query must not be null");
    Preconditions.checkState(
        model.version() > 0,
        "Semantic Model %s does not expose a committed version",
        semanticModel);

    Resolver resolver = new Resolver(semanticModel, model, query);
    return resolver.resolve();
  }

  private static final class Resolver {

    private final NameIdentifier semanticModel;
    private final SemanticModel model;
    private final SemanticQuery query;

    private final Map<String, Dataset> datasets = new LinkedHashMap<>();
    private final Map<String, Map<String, Field>> fieldsByDataset = new LinkedHashMap<>();
    private final Map<String, Metric> metrics = new LinkedHashMap<>();
    private final List<Relationship> relationships = new ArrayList<>();

    private final List<PlanSelection> selections = new ArrayList<>();
    private final List<PlanFilter> filters = new ArrayList<>();
    private final List<PlanJoin> joins = new ArrayList<>();
    private final Map<String, Edge> parents = new HashMap<>();
    private final Set<String> sourceDatasets = new LinkedHashSet<>();

    private String firstSelectionDataset;

    private Resolver(NameIdentifier semanticModel, SemanticModel model, SemanticQuery query) {
      this.semanticModel = semanticModel;
      this.model = model;
      this.query = query;
      indexModel();
    }

    private void indexModel() {
      SemanticModelDefinition definition = model.definition();
      Preconditions.checkArgument(definition != null, "model definition must not be null");
      Dataset[] declaredDatasets = definition.datasets();
      Preconditions.checkArgument(
          declaredDatasets != null && declaredDatasets.length > 0,
          "model must declare at least one dataset");
      for (Dataset dataset : declaredDatasets) {
        datasets.put(dataset.name(), dataset);
        Map<String, Field> fields = new LinkedHashMap<>();
        if (dataset.fields() != null) {
          for (Field field : dataset.fields()) {
            fields.put(field.name(), field);
          }
        }
        fieldsByDataset.put(dataset.name(), fields);
      }

      Metric[] declaredMetrics = definition.metrics();
      if (declaredMetrics != null) {
        for (Metric metric : declaredMetrics) {
          metrics.put(metric.name(), metric);
        }
      }

      Relationship[] declaredRelationships = definition.relationships();
      if (declaredRelationships != null) {
        Collections.addAll(relationships, declaredRelationships);
      }
    }

    private SemanticQueryPlan resolve() {
      resolveSelections();
      resolveFilters();
      resolveSourcesAndJoins();

      return SemanticQueryPlan.builder()
          .withSemanticModel(semanticModel)
          .withSemanticModelVersion(model.version())
          .withSources(toPlanSources())
          .withJoins(joins)
          .withSelections(selections)
          .withFilters(filters)
          .withGroupBy(
              selections.stream()
                  .filter(selection -> !selection.aggregate())
                  .map(PlanSelection::name)
                  .collect(Collectors.toList()))
          .withLimit(query.limit())
          .withWarnings(Collections.emptyList())
          .build();
    }

    private void resolveSelections() {
      Set<String> selectionNames = new HashSet<>();
      for (String metricName : query.metrics()) {
        Metric metric = metrics.get(metricName);
        if (metric == null) {
          throw unsatisfiable(
              "Metric '%s' does not exist in Semantic Model %s", metricName, semanticModel);
        }
        String dataset = resolveMetricDataset(metric);
        addSelectionName(selectionNames, metricName);
        PlanFieldRef field =
            PlanFieldRef.ofExpression(dataset, metric.expression(), metric.datatype());
        selections.add(PlanSelection.of(metricName, PlanSelection.Role.METRIC, true, field));
        rememberFirstSelection(dataset);
      }

      for (String dimensionName : query.dimensions()) {
        PlanFieldRef field = resolveField(dimensionName);
        addSelectionName(selectionNames, dimensionName);
        selections.add(PlanSelection.of(dimensionName, PlanSelection.Role.DIMENSION, false, field));
        rememberFirstSelection(field.dataset());
      }

      if (selections.isEmpty()) {
        throw new IllegalArgumentException(
            "a semantic query must select at least one metric or dimension");
      }
    }

    private void rememberFirstSelection(String dataset) {
      if (firstSelectionDataset == null) {
        firstSelectionDataset = dataset;
      }
    }

    private static void addSelectionName(Set<String> names, String name) {
      if (!names.add(name)) {
        throw new SemanticQueryPlanException(
            "Selection name '%s' appears more than once in the query", name);
      }
    }

    private void resolveFilters() {
      for (SemanticQueryFilter filter : query.filters()) {
        PlanFieldRef field = resolveField(filter.field());
        filters.add(PlanFilter.of(field, filter.operator(), filter.value()));
      }
    }

    private String resolveMetricDataset(Metric metric) {
      Set<String> qualifiedDatasets = new LinkedHashSet<>();
      for (String expression : expressionTexts(metric.expression())) {
        Matcher matcher = QUALIFIED_REFERENCE.matcher(expression);
        while (matcher.find()) {
          if (datasets.containsKey(matcher.group(1))) {
            qualifiedDatasets.add(matcher.group(1));
          }
        }
      }
      if (qualifiedDatasets.size() == 1) {
        return qualifiedDatasets.iterator().next();
      }
      if (qualifiedDatasets.size() > 1) {
        throw unsatisfiable(
            "Metric '%s' references more than one dataset (%s); qualify the metric with a single"
                + " dataset",
            metric.name(), String.join(", ", qualifiedDatasets));
      }

      Set<String> declaredFieldDatasets = new LinkedHashSet<>();
      for (String expression : expressionTexts(metric.expression())) {
        for (Map.Entry<String, Map<String, Field>> entry : fieldsByDataset.entrySet()) {
          if (mentionsAny(expression, entry.getValue().keySet())) {
            declaredFieldDatasets.add(entry.getKey());
          }
        }
      }
      if (declaredFieldDatasets.size() == 1) {
        return declaredFieldDatasets.iterator().next();
      }
      if (declaredFieldDatasets.size() > 1) {
        throw unsatisfiable(
            "Metric '%s' mentions fields of more than one dataset (%s); qualify the metric with a"
                + " single dataset",
            metric.name(), String.join(", ", declaredFieldDatasets));
      }
      throw unsatisfiable(
          "Metric '%s' cannot be attributed to a dataset; qualify at least one source reference"
              + " with a dataset name",
          metric.name());
    }

    private PlanFieldRef resolveField(String name) {
      int separator = name.indexOf('.');
      if (separator > 0) {
        return resolveQualifiedField(name, separator);
      }

      List<String> declaringDatasets =
          fieldsByDataset.entrySet().stream()
              .filter(entry -> entry.getValue().containsKey(name))
              .map(Map.Entry::getKey)
              .collect(Collectors.toList());
      if (declaringDatasets.size() == 1) {
        String dataset = declaringDatasets.get(0);
        return declaredField(dataset, fieldsByDataset.get(dataset).get(name));
      }
      if (declaringDatasets.size() > 1) {
        throw unsatisfiable(
            "Field '%s' is declared by more than one dataset (%s); qualify it with a dataset name",
            name, String.join(", ", declaringDatasets));
      }
      if (firstSelectionDataset == null) {
        throw unsatisfiable(
            "Field '%s' is not declared by any dataset and no selection determines its dataset",
            name);
      }
      return PlanFieldRef.ofColumn(firstSelectionDataset, name, null);
    }

    private PlanFieldRef resolveQualifiedField(String name, int separator) {
      String dataset = name.substring(0, separator);
      String fieldName = name.substring(separator + 1);
      if (!datasets.containsKey(dataset)) {
        throw unsatisfiable(
            "Dataset '%s' does not exist in Semantic Model %s", dataset, semanticModel);
      }
      Field field = fieldsByDataset.get(dataset).get(fieldName);
      if (field == null) {
        throw unsatisfiable("Field '%s' is not declared by dataset '%s'", fieldName, dataset);
      }
      return declaredField(dataset, field);
    }

    private static PlanFieldRef declaredField(String dataset, Field field) {
      return PlanFieldRef.ofExpression(dataset, field.expression(), field.datatype());
    }

    private void resolveSourcesAndJoins() {
      sourceDatasets.add(firstSelectionDataset);
      for (PlanSelection selection : selections) {
        addDataset(selection.field().dataset());
      }
      for (PlanFilter filter : filters) {
        addDataset(filter.field().dataset());
      }
    }

    private void addDataset(String target) {
      if (sourceDatasets.contains(target)) {
        return;
      }

      parents.clear();
      Set<String> visited = new HashSet<>(sourceDatasets);
      Deque<String> queue = new ArrayDeque<>(sourceDatasets);
      boolean found = false;
      while (!queue.isEmpty() && !found) {
        String current = queue.removeFirst();
        for (Edge edge : edgesFrom(current)) {
          if (!visited.add(edge.to)) {
            continue;
          }
          parents.put(edge.to, edge);
          if (edge.to.equals(target)) {
            found = true;
            break;
          }
          queue.addLast(edge.to);
        }
      }

      if (!found) {
        throw unsatisfiable(
            "Dataset '%s' cannot be joined to the datasets used by the query", target);
      }

      List<Edge> path = new ArrayList<>();
      String node = target;
      while (!sourceDatasets.contains(node)) {
        Edge edge = parents.get(node);
        path.add(0, edge);
        node = edge.from;
      }
      for (Edge edge : path) {
        joins.add(
            PlanJoin.of(
                PlanJoin.Type.INNER,
                edge.relationship,
                edge.from,
                edge.fromColumns,
                edge.to,
                edge.toColumns));
        sourceDatasets.add(edge.to);
      }
    }

    private List<Edge> edgesFrom(String dataset) {
      List<Edge> edges = new ArrayList<>();
      for (Relationship relationship : relationships) {
        if (relationship.from().equals(dataset) && datasets.containsKey(relationship.to())) {
          edges.add(
              new Edge(
                  relationship.name(),
                  relationship.from(),
                  relationship.fromColumns(),
                  relationship.to(),
                  relationship.toColumns()));
        }
        if (relationship.to().equals(dataset) && datasets.containsKey(relationship.from())) {
          edges.add(
              new Edge(
                  relationship.name(),
                  relationship.to(),
                  relationship.toColumns(),
                  relationship.from(),
                  relationship.fromColumns()));
        }
      }
      return edges;
    }

    private List<PlanSource> toPlanSources() {
      List<PlanSource> sources = new ArrayList<>(sourceDatasets.size());
      for (String dataset : sourceDatasets) {
        sources.add(PlanSource.of(dataset, datasets.get(dataset).source()));
      }
      return sources;
    }

    private static List<String> expressionTexts(Expression expression) {
      List<String> texts = new ArrayList<>();
      if (expression == null || expression.dialects() == null) {
        return texts;
      }
      for (DialectExpression dialectExpression : expression.dialects()) {
        texts.add(normalize(dialectExpression.expression()));
      }
      return texts;
    }

    private static String normalize(String expression) {
      return expression.replace("`", "").replace("\"", "").replace("[", "").replace("]", "");
    }

    private static boolean mentionsAny(String expression, Set<String> names) {
      Matcher matcher = IDENTIFIER.matcher(expression);
      while (matcher.find()) {
        if (names.contains(matcher.group())) {
          return true;
        }
      }
      return false;
    }

    @FormatMethod
    private static SemanticQueryPlanException unsatisfiable(
        @FormatString String message, Object... args) {
      return new SemanticQueryPlanException(message, args);
    }
  }

  private static final class Edge {
    private final String relationship;
    private final String from;
    private final String[] fromColumns;
    private final String to;
    private final String[] toColumns;

    private Edge(
        String relationship, String from, String[] fromColumns, String to, String[] toColumns) {
      this.relationship = relationship;
      this.from = from;
      this.fromColumns = fromColumns;
      this.to = to;
      this.toColumns = toColumns;
    }
  }
}
