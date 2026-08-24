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
package org.apache.gravitino.catalog;

import static org.apache.gravitino.catalog.CapabilityHelpers.applyCaseSensitiveOnName;
import static org.apache.gravitino.catalog.CapabilityHelpers.getCapability;

import com.google.common.base.Preconditions;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.connector.capability.Capability;
import org.apache.gravitino.exceptions.IllegalSemanticModelException;
import org.apache.gravitino.exceptions.NotFoundException;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.View;
import org.apache.gravitino.semantic.AIContext;
import org.apache.gravitino.semantic.AIContextObject;
import org.apache.gravitino.semantic.CustomExtension;
import org.apache.gravitino.semantic.Dataset;
import org.apache.gravitino.semantic.DialectExpression;
import org.apache.gravitino.semantic.Expression;
import org.apache.gravitino.semantic.Field;
import org.apache.gravitino.semantic.Metric;
import org.apache.gravitino.semantic.Relationship;
import org.apache.gravitino.semantic.SemanticModelDefinition;

/**
 * Validates Semantic Model writes using Gravitino's Java value model and catalog metadata.
 *
 * <p>At implementation time, Apache Ossie commit {@code 88e0011148283302c9a04cd0287e00e0b9d87354},
 * whose core specification version is {@code 0.2.0.dev0}, did not publish a reusable Java SDK or
 * general-purpose Java validator artifact. The Java schema validation in that upstream tree was
 * converter-specific. Gravitino therefore implements the applicable structural and model-local
 * rules directly in Java. If a future Ossie release publishes a compatible Java SDK or validator,
 * Gravitino should evaluate replacing this implementation with that upstream library.
 *
 * <p>{@code validateDefinition} is deterministic and performs no catalog I/O. {@code
 * validateSources} resolves Table and logical View metadata and applies catalog capabilities,
 * including column case sensitivity. {@code validateForWrite} composes both phases for create and
 * definition-replacement writes. SQL expression semantics, transitive View semantics, and query
 * engine compatibility are outside this validator's scope.
 */
final class SemanticModelValidator {

  private final CatalogManager catalogManager;
  private final TableDispatcher tableDispatcher;
  private final ViewDispatcher viewDispatcher;

  SemanticModelValidator(
      CatalogManager catalogManager,
      TableDispatcher tableDispatcher,
      ViewDispatcher viewDispatcher) {
    Preconditions.checkArgument(catalogManager != null, "Catalog manager must not be null");
    Preconditions.checkArgument(tableDispatcher != null, "Table dispatcher must not be null");
    Preconditions.checkArgument(viewDispatcher != null, "View dispatcher must not be null");
    this.catalogManager = catalogManager;
    this.tableDispatcher = tableDispatcher;
    this.viewDispatcher = viewDispatcher;
  }

  static void validateDefinition(@Nullable SemanticModelDefinition definition) {
    if (definition == null) {
      throw invalid("$", "definition must not be null");
    }

    validateAIContext(definition.aiContext(), "aiContext");

    Dataset[] datasets = definition.datasets();
    if (datasets == null || datasets.length == 0) {
      throw invalid("datasets", "must not be null or empty");
    }

    Map<String, String> datasetNames = new HashMap<>();
    for (int index = 0; index < datasets.length; index++) {
      validateDataset(datasets[index], "datasets[" + index + "]", datasetNames);
    }

    validateRelationships(definition.relationships(), datasetNames);
    validateMetrics(definition.metrics());
    validateCustomExtensions(definition.customExtensions(), "customExtensions");
  }

  void validateSources(String metalake, SemanticModelDefinition definition) {
    Dataset[] datasets = definition.datasets();
    Map<String, SourceColumns> columnsByDataset = new HashMap<>();

    for (int datasetIndex = 0; datasetIndex < datasets.length; datasetIndex++) {
      Dataset dataset = datasets[datasetIndex];
      String datasetPath = "datasets[" + datasetIndex + "]";
      NameIdentifier fullSource =
          qualifySource(metalake, dataset.source(), datasetPath + ".source");
      SourceColumns sourceColumns = loadSourceColumns(fullSource, datasetPath + ".source");
      columnsByDataset.put(dataset.name(), sourceColumns);

      validateSourceColumns(dataset.primaryKey(), datasetPath + ".primaryKey", sourceColumns);
      String[][] uniqueKeys = dataset.uniqueKeys();
      if (uniqueKeys != null) {
        for (int keyIndex = 0; keyIndex < uniqueKeys.length; keyIndex++) {
          validateSourceColumns(
              uniqueKeys[keyIndex], datasetPath + ".uniqueKeys[" + keyIndex + "]", sourceColumns);
        }
      }
    }

    Relationship[] relationships = definition.relationships();
    if (relationships == null) {
      return;
    }
    for (int relationshipIndex = 0; relationshipIndex < relationships.length; relationshipIndex++) {
      Relationship relationship = relationships[relationshipIndex];
      String path = "relationships[" + relationshipIndex + "]";
      SourceColumns fromColumns = columnsByDataset.get(relationship.from());
      SourceColumns toColumns = columnsByDataset.get(relationship.to());
      if (fromColumns == null || toColumns == null) {
        throw invalid(
            path, "relationship endpoints must reference datasets in the same Semantic Model");
      }
      validateSourceColumns(relationship.fromColumns(), path + ".fromColumns", fromColumns);
      validateSourceColumns(relationship.toColumns(), path + ".toColumns", toColumns);
    }
  }

  void validateForWrite(
      NameIdentifier semanticModelIdent, @Nullable SemanticModelDefinition definition) {
    if (semanticModelIdent == null || semanticModelIdent.namespace().length() != 3) {
      throw invalid("$", "Semantic Model identifier must use metalake.catalog.schema.name");
    }
    validateDefinition(definition);
    validateSources(semanticModelIdent.namespace().level(0), definition);
  }

  private static void validateDataset(
      @Nullable Dataset dataset, String path, Map<String, String> datasetNames) {
    if (dataset == null) {
      throw invalid(path, "must not be null");
    }

    String namePath = path + ".name";
    validateRequiredString(dataset.name(), namePath);
    validateUniqueName(dataset.name(), namePath, "dataset", datasetNames);
    validateSource(dataset.source(), path + ".source");
    validateOptionalColumnNames(dataset.primaryKey(), path + ".primaryKey");
    validateUniqueKeys(dataset.uniqueKeys(), path + ".uniqueKeys");
    validateAIContext(dataset.aiContext(), path + ".aiContext");
    validateFields(dataset.fields(), path + ".fields");
    validateCustomExtensions(dataset.customExtensions(), path + ".customExtensions");
  }

  private static void validateSource(@Nullable NameIdentifier source, String path) {
    if (source == null) {
      throw invalid(path, "must not be null");
    }
    if (source.namespace().length() != 2) {
      throw invalid(
          path, "must contain exactly catalog.schema.name, but was '" + source.toString() + "'");
    }
  }

  private static void validateUniqueKeys(@Nullable String[][] uniqueKeys, String path) {
    if (uniqueKeys == null) {
      return;
    }

    for (int index = 0; index < uniqueKeys.length; index++) {
      String keyPath = path + "[" + index + "]";
      String[] uniqueKey = uniqueKeys[index];
      if (uniqueKey == null || uniqueKey.length == 0) {
        throw invalid(keyPath, "must not be null or empty");
      }
      validateColumnNames(uniqueKey, keyPath, false);
    }
  }

  private static void validateFields(@Nullable Field[] fields, String path) {
    if (fields == null) {
      return;
    }

    Map<String, String> fieldNames = new HashMap<>();
    for (int index = 0; index < fields.length; index++) {
      String fieldPath = path + "[" + index + "]";
      Field field = fields[index];
      if (field == null) {
        throw invalid(fieldPath, "must not be null");
      }

      String namePath = fieldPath + ".name";
      validateRequiredString(field.name(), namePath);
      validateUniqueName(field.name(), namePath, "field", fieldNames);
      validateExpression(field.expression(), fieldPath + ".expression");
      validateAIContext(field.aiContext(), fieldPath + ".aiContext");
      validateCustomExtensions(field.customExtensions(), fieldPath + ".customExtensions");
    }
  }

  private static void validateRelationships(
      @Nullable Relationship[] relationships, Map<String, String> datasetNames) {
    if (relationships == null) {
      return;
    }

    Map<String, String> relationshipNames = new HashMap<>();
    for (int index = 0; index < relationships.length; index++) {
      String path = "relationships[" + index + "]";
      Relationship relationship = relationships[index];
      if (relationship == null) {
        throw invalid(path, "must not be null");
      }

      String namePath = path + ".name";
      validateRequiredString(relationship.name(), namePath);
      validateUniqueName(relationship.name(), namePath, "relationship", relationshipNames);
      validateEndpoint(relationship.from(), path + ".from", datasetNames);
      validateEndpoint(relationship.to(), path + ".to", datasetNames);

      String[] fromColumns = relationship.fromColumns();
      String[] toColumns = relationship.toColumns();
      validateColumnNames(fromColumns, path + ".fromColumns", false);
      validateColumnNames(toColumns, path + ".toColumns", false);
      if (fromColumns.length != toColumns.length) {
        throw invalid(
            path + ".toColumns",
            "must contain "
                + fromColumns.length
                + " columns to match "
                + path
                + ".fromColumns, but contained "
                + toColumns.length);
      }
      validateAIContext(relationship.aiContext(), path + ".aiContext");
      validateCustomExtensions(relationship.customExtensions(), path + ".customExtensions");
    }
  }

  private static void validateEndpoint(
      @Nullable String endpoint, String path, Map<String, String> datasetNames) {
    validateRequiredString(endpoint, path);
    if (!datasetNames.containsKey(endpoint)) {
      throw invalid(
          path,
          "unknown dataset '"
              + endpoint
              + "'; relationship endpoints must reference datasets in the same model");
    }
  }

  private static void validateMetrics(@Nullable Metric[] metrics) {
    if (metrics == null) {
      return;
    }

    Map<String, String> metricNames = new HashMap<>();
    for (int index = 0; index < metrics.length; index++) {
      String path = "metrics[" + index + "]";
      Metric metric = metrics[index];
      if (metric == null) {
        throw invalid(path, "must not be null");
      }

      String namePath = path + ".name";
      validateRequiredString(metric.name(), namePath);
      validateUniqueName(metric.name(), namePath, "metric", metricNames);
      validateExpression(metric.expression(), path + ".expression");
      validateAIContext(metric.aiContext(), path + ".aiContext");
      validateCustomExtensions(metric.customExtensions(), path + ".customExtensions");
    }
  }

  private static void validateExpression(@Nullable Expression expression, String path) {
    if (expression == null) {
      throw invalid(path, "must not be null");
    }

    DialectExpression[] dialectExpressions = expression.dialects();
    if (dialectExpressions == null || dialectExpressions.length == 0) {
      throw invalid(path + ".dialects", "must not be null or empty");
    }

    Map<String, String> dialectPaths = new HashMap<>();
    for (int index = 0; index < dialectExpressions.length; index++) {
      String dialectExpressionPath = path + ".dialects[" + index + "]";
      DialectExpression dialectExpression = dialectExpressions[index];
      if (dialectExpression == null) {
        throw invalid(dialectExpressionPath, "must not be null");
      }

      String dialect = dialectExpression.dialect();
      String dialectPath = dialectExpressionPath + ".dialect";
      validateRequiredString(dialect, dialectPath);
      String firstPath = dialectPaths.putIfAbsent(dialect, dialectPath);
      if (firstPath != null) {
        throw invalid(
            dialectPath, "duplicate dialect '" + dialect + "'; first declared at " + firstPath);
      }
      validateRequiredString(dialectExpression.expression(), dialectExpressionPath + ".expression");
    }
  }

  private static void validateAIContext(@Nullable AIContext aiContext, String path) {
    if (aiContext == null) {
      return;
    }

    String text = aiContext.text();
    AIContextObject object = aiContext.object();
    if ((text == null) == (object == null)) {
      throw invalid(path, "must contain exactly one string or object value");
    }
    if (object == null) {
      return;
    }

    validateStringElements(object.synonyms(), path + ".synonyms");
    validateStringElements(object.examples(), path + ".examples");
    if (object.additionalProperties() == null) {
      throw invalid(path + ".additionalProperties", "must not be null");
    }
    for (String name : object.additionalProperties().keySet()) {
      if (name == null) {
        throw invalid(path + ".additionalProperties", "property name must not be null");
      }
    }
  }

  private static void validateColumnNames(
      @Nullable String[] columns, String path, boolean allowEmpty) {
    if (columns == null || (!allowEmpty && columns.length == 0)) {
      throw invalid(path, allowEmpty ? "must not be null" : "must not be null or empty");
    }
    for (int index = 0; index < columns.length; index++) {
      validateRequiredString(columns[index], path + "[" + index + "]");
    }
  }

  private static void validateOptionalColumnNames(@Nullable String[] columns, String path) {
    if (columns == null) {
      return;
    }
    for (int index = 0; index < columns.length; index++) {
      validateRequiredString(columns[index], path + "[" + index + "]");
    }
  }

  private static void validateStringElements(@Nullable String[] values, String path) {
    if (values == null) {
      return;
    }
    for (int index = 0; index < values.length; index++) {
      if (values[index] == null) {
        throw invalid(path + "[" + index + "]", "must not be null");
      }
    }
  }

  private static void validateCustomExtensions(
      @Nullable CustomExtension[] customExtensions, String path) {
    if (customExtensions == null) {
      return;
    }
    for (int index = 0; index < customExtensions.length; index++) {
      String extensionPath = path + "[" + index + "]";
      CustomExtension extension = customExtensions[index];
      if (extension == null) {
        throw invalid(extensionPath, "must not be null");
      }
      if (extension.vendorName() == null) {
        throw invalid(extensionPath + ".vendorName", "must not be null");
      }
      if (extension.data() == null) {
        throw invalid(extensionPath + ".data", "must not be null");
      }
    }
  }

  private static void validateRequiredString(@Nullable String value, String path) {
    if (value == null || value.isEmpty()) {
      throw invalid(path, "must not be null or empty");
    }
  }

  private static void validateUniqueName(
      String name, String path, String memberType, Map<String, String> names) {
    String firstPath = names.putIfAbsent(name, path);
    if (firstPath != null) {
      throw invalid(
          path, "duplicate " + memberType + " name '" + name + "'; first declared at " + firstPath);
    }
  }

  private static NameIdentifier qualifySource(
      String metalake, NameIdentifier source, String sourcePath) {
    if (source == null || source.namespace().length() != 2) {
      throw invalid(sourcePath, "source must use catalog.schema.name");
    }
    return NameIdentifier.of(
        Namespace.of(metalake, source.namespace().level(0), source.namespace().level(1)),
        source.name());
  }

  private SourceColumns loadSourceColumns(NameIdentifier source, String sourcePath) {
    try {
      Table table = tableDispatcher.loadTable(source);
      return sourceColumns(source, table == null ? null : table.columns());
    } catch (NotFoundException | UnsupportedOperationException tableNotFound) {
      try {
        View view = viewDispatcher.loadView(source);
        return sourceColumns(source, view == null ? null : view.columns());
      } catch (NotFoundException | UnsupportedOperationException viewNotFound) {
        throw new IllegalSemanticModelException(
            viewNotFound,
            "%s: source %s does not exist as a Table or logical View",
            sourcePath,
            source);
      }
    }
  }

  private SourceColumns sourceColumns(NameIdentifier source, Column[] columns) {
    Capability capability = getCapability(source, catalogManager);
    Function<String, String> normalizeColumn =
        name -> applyCaseSensitiveOnName(Capability.Scope.COLUMN, name, capability);
    Set<String> names =
        columns == null
            ? Collections.emptySet()
            : Arrays.stream(columns)
                .map(Column::name)
                .map(normalizeColumn)
                .collect(Collectors.toSet());
    return new SourceColumns(source, names, normalizeColumn);
  }

  private static void validateSourceColumns(
      @Nullable String[] columns, String path, SourceColumns sourceColumns) {
    if (columns == null) {
      return;
    }
    for (int columnIndex = 0; columnIndex < columns.length; columnIndex++) {
      String column = columns[columnIndex];
      if (!sourceColumns.contains(column)) {
        throw new IllegalSemanticModelException(
            "%s[%s]: column '%s' does not exist in source %s",
            path, columnIndex, column, sourceColumns.source);
      }
    }
  }

  private static IllegalSemanticModelException invalid(String path, String detail) {
    return new IllegalSemanticModelException("%s: %s", path, detail);
  }

  private static final class SourceColumns {
    private final NameIdentifier source;
    private final Set<String> columns;
    private final Function<String, String> normalizeColumn;

    private SourceColumns(
        NameIdentifier source, Set<String> columns, Function<String, String> normalizeColumn) {
      this.source = source;
      this.columns = columns;
      this.normalizeColumn = normalizeColumn;
    }

    private boolean contains(String column) {
      return columns.contains(normalizeColumn.apply(column));
    }
  }
}
