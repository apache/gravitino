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

import static org.apache.gravitino.semantic.CustomExtension.GRAVITINO_PROPERTIES_VENDOR;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.exceptions.IllegalSemanticModelException;
import org.apache.gravitino.semantic.CustomExtension;
import org.apache.gravitino.semantic.Dataset;
import org.apache.gravitino.semantic.Field;
import org.apache.gravitino.semantic.Metric;
import org.apache.gravitino.semantic.Relationship;
import org.apache.gravitino.semantic.SemanticModelDefinition;

/**
 * Validates model-level constraints for Semantic Model writes.
 *
 * <p>At implementation time, Apache Ossie commit {@code 88e0011148283302c9a04cd0287e00e0b9d87354},
 * whose core specification version is {@code 0.2.0.dev0}, did not publish a reusable Java SDK or
 * general-purpose Java validator artifact. The Java schema validation in that upstream tree was
 * converter-specific. Gravitino therefore implements the applicable model-level rules directly in
 * Java. If a future Ossie release publishes a compatible Java SDK or validator, Gravitino should
 * evaluate replacing this implementation with that upstream library.
 *
 * <p>The API value-object builders enforce value-level invariants, including required values, array
 * element validity, relationship column shapes, and expression dialect uniqueness. This validator
 * handles constraints that span value objects: name uniqueness, relationship endpoint resolution,
 * key and relationship field resolution, Semantic Model source identifier shape, and the reserved
 * root properties extension vendor.
 *
 * <p>Following the Ossie Name Resolution rules, {@code primaryKey}, {@code uniqueKeys}, {@code
 * fromColumns}, and {@code toColumns} name fields declared in the referenced datasets, not columns
 * of the datasets' sources. Field names are compared case-insensitively. A dataset that declares no
 * fields exposes no names to keys or relationships.
 *
 * <p>Validation is deterministic and performs no catalog I/O. Catalog-backed source validation,
 * including existence and authorization, must be completed by the caller before this validator is
 * invoked. SQL expression semantics, transitive View semantics, and query engine compatibility are
 * outside this validator's scope.
 */
final class SemanticModelValidator {

  private SemanticModelValidator() {}

  static void validateDefinition(@Nullable SemanticModelDefinition definition) {
    if (definition == null) {
      throw invalid("$", "definition must not be null");
    }

    Dataset[] datasets = definition.datasets();
    Map<String, String> datasetNames = new HashMap<>();
    Map<String, Set<String>> datasetFields = new HashMap<>();
    for (int index = 0; index < datasets.length; index++) {
      validateDataset(datasets[index], "datasets[" + index + "]", datasetNames, datasetFields);
    }

    validateRelationships(definition.relationships(), datasetNames, datasetFields);
    validateMetrics(definition.metrics());
    validateRootCustomExtensions(definition.customExtensions());
  }

  private static void validateRootCustomExtensions(@Nullable CustomExtension[] extensions) {
    if (extensions == null) {
      return;
    }
    for (int index = 0; index < extensions.length; index++) {
      if (GRAVITINO_PROPERTIES_VENDOR.equals(extensions[index].vendorName())) {
        throw invalid(
            "customExtensions[" + index + "].vendorName",
            "'" + GRAVITINO_PROPERTIES_VENDOR + "' is reserved for Gravitino properties");
      }
    }
  }

  private static void validateDataset(
      Dataset dataset,
      String path,
      Map<String, String> datasetNames,
      Map<String, Set<String>> datasetFields) {
    String namePath = path + ".name";
    validateUniqueName(dataset.name(), namePath, "dataset", datasetNames);
    validateSource(dataset.source(), path + ".source");
    Set<String> fieldNames = validateFields(dataset.fields(), path + ".fields");
    validateKeys(dataset, path, fieldNames);
    datasetFields.put(dataset.name(), fieldNames);
  }

  private static void validateSource(NameIdentifier source, String path) {
    if (source.namespace().length() != 2) {
      throw invalid(
          path, "must contain exactly catalog.schema.name, but was '" + source.toString() + "'");
    }
  }

  private static Set<String> validateFields(@Nullable Field[] fields, String path) {
    Set<String> fieldNames = new HashSet<>();
    if (fields == null) {
      return fieldNames;
    }

    Map<String, String> uniqueNames = new HashMap<>();
    for (int index = 0; index < fields.length; index++) {
      String fieldPath = path + "[" + index + "]";
      Field field = fields[index];
      String namePath = fieldPath + ".name";
      validateUniqueName(field.name(), namePath, "field", uniqueNames);
      fieldNames.add(normalizeFieldName(field.name()));
    }
    return fieldNames;
  }

  private static void validateKeys(Dataset dataset, String path, Set<String> fieldNames) {
    String[] primaryKey = dataset.primaryKey();
    if (primaryKey != null) {
      for (int index = 0; index < primaryKey.length; index++) {
        validateFieldReference(
            primaryKey[index], path + ".primaryKey[" + index + "]", dataset.name(), fieldNames);
      }
    }

    String[][] uniqueKeys = dataset.uniqueKeys();
    if (uniqueKeys != null) {
      for (int keyIndex = 0; keyIndex < uniqueKeys.length; keyIndex++) {
        String[] uniqueKey = uniqueKeys[keyIndex];
        for (int index = 0; index < uniqueKey.length; index++) {
          validateFieldReference(
              uniqueKey[index],
              path + ".uniqueKeys[" + keyIndex + "][" + index + "]",
              dataset.name(),
              fieldNames);
        }
      }
    }
  }

  private static void validateRelationships(
      @Nullable Relationship[] relationships,
      Map<String, String> datasetNames,
      Map<String, Set<String>> datasetFields) {
    if (relationships == null) {
      return;
    }

    Map<String, String> relationshipNames = new HashMap<>();
    for (int index = 0; index < relationships.length; index++) {
      String path = "relationships[" + index + "]";
      Relationship relationship = relationships[index];
      String namePath = path + ".name";
      validateUniqueName(relationship.name(), namePath, "relationship", relationshipNames);
      validateEndpoint(relationship.from(), path + ".from", datasetNames);
      validateEndpoint(relationship.to(), path + ".to", datasetNames);
      validateRelationshipColumns(relationship, path, datasetFields);
    }
  }

  private static void validateRelationshipColumns(
      Relationship relationship, String path, Map<String, Set<String>> datasetFields) {
    validateColumnReferences(
        relationship.fromColumns(),
        path + ".fromColumns",
        relationship.from(),
        datasetFields.get(relationship.from()));
    validateColumnReferences(
        relationship.toColumns(),
        path + ".toColumns",
        relationship.to(),
        datasetFields.get(relationship.to()));
  }

  private static void validateColumnReferences(
      String[] references, String path, String datasetName, @Nullable Set<String> fieldNames) {
    if (fieldNames == null) {
      // The unknown endpoint is already reported by validateEndpoint.
      return;
    }
    for (int index = 0; index < references.length; index++) {
      validateFieldReference(references[index], path + "[" + index + "]", datasetName, fieldNames);
    }
  }

  private static void validateFieldReference(
      String reference, String path, String datasetName, Set<String> fieldNames) {
    if (!fieldNames.contains(normalizeFieldName(reference))) {
      throw invalid(
          path, "'" + reference + "' is not a declared field of dataset '" + datasetName + "'");
    }
  }

  private static String normalizeFieldName(String name) {
    return name.toLowerCase(Locale.ROOT);
  }

  private static void validateEndpoint(
      String endpoint, String path, Map<String, String> datasetNames) {
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
      String namePath = path + ".name";
      validateUniqueName(metric.name(), namePath, "metric", metricNames);
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

  private static IllegalSemanticModelException invalid(String path, String detail) {
    return new IllegalSemanticModelException("%s: %s", path, detail);
  }
}
