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
package org.apache.gravitino.dto.semantic;

import com.google.common.base.Preconditions;
import java.util.HashSet;
import java.util.Set;
import javax.annotation.Nullable;

final class SemanticModelDefinitionValidator {

  private SemanticModelDefinitionValidator() {}

  static void validate(SemanticModelDefinitionDTO definition) {
    DatasetDTO[] datasets = definition.getDatasets();
    RelationshipDTO[] relationships = definition.getRelationships();
    MetricDTO[] metrics = definition.getMetrics();
    CustomExtensionDTO[] customExtensions = definition.getCustomExtensions();

    Preconditions.checkArgument(
        datasets != null && datasets.length > 0, "datasets must not be null or empty");
    validateNoNullElements("datasets", datasets);
    validateNoNullElements("relationships", relationships);
    validateNoNullElements("metrics", metrics);
    validateNoNullElements("customExtensions", customExtensions);

    for (DatasetDTO dataset : datasets) {
      validateDataset(dataset);
    }
    if (relationships != null) {
      for (RelationshipDTO relationship : relationships) {
        validateRelationship(relationship);
      }
    }
    if (metrics != null) {
      for (MetricDTO metric : metrics) {
        validateMetric(metric);
      }
    }
    validateCustomExtensions(customExtensions);
    validateAIContext(definition.getAiContext());
  }

  private static void validateDataset(DatasetDTO dataset) {
    Preconditions.checkArgument(
        dataset.getName() != null && !dataset.getName().isEmpty(),
        "name must not be null or empty");
    Preconditions.checkArgument(dataset.getSource() != null, "source must not be null");

    String[] primaryKey = dataset.getPrimaryKey();
    String[][] uniqueKeys = dataset.getUniqueKeys();
    FieldDTO[] fields = dataset.getFields();
    CustomExtensionDTO[] customExtensions = dataset.getCustomExtensions();
    validateNonEmptyStringElements("primaryKey", primaryKey);
    validateUniqueKeys(uniqueKeys);
    validateNoNullElements("fields", fields);
    validateNoNullElements("customExtensions", customExtensions);

    if (fields != null) {
      for (FieldDTO field : fields) {
        validateField(field);
      }
    }
    validateCustomExtensions(customExtensions);
    validateAIContext(dataset.getAiContext());
  }

  private static void validateRelationship(RelationshipDTO relationship) {
    Preconditions.checkArgument(
        relationship.getName() != null && !relationship.getName().isEmpty(),
        "name must not be null or empty");
    Preconditions.checkArgument(
        relationship.getFrom() != null && !relationship.getFrom().isEmpty(),
        "from must not be null or empty");
    Preconditions.checkArgument(
        relationship.getTo() != null && !relationship.getTo().isEmpty(),
        "to must not be null or empty");

    String[] fromColumns = relationship.getFromColumns();
    String[] toColumns = relationship.getToColumns();
    Preconditions.checkArgument(
        fromColumns != null && fromColumns.length > 0, "fromColumns must not be null or empty");
    Preconditions.checkArgument(
        toColumns != null && toColumns.length > 0, "toColumns must not be null or empty");
    validateNonEmptyStringElements("fromColumns", fromColumns);
    validateNonEmptyStringElements("toColumns", toColumns);
    Preconditions.checkArgument(
        fromColumns.length == toColumns.length,
        "fromColumns and toColumns must have the same length");

    CustomExtensionDTO[] customExtensions = relationship.getCustomExtensions();
    validateNoNullElements("customExtensions", customExtensions);
    validateCustomExtensions(customExtensions);
    validateAIContext(relationship.getAiContext());
  }

  private static void validateField(FieldDTO field) {
    Preconditions.checkArgument(
        field.getName() != null && !field.getName().isEmpty(), "name must not be null or empty");
    Preconditions.checkArgument(field.getExpression() != null, "expression must not be null");

    CustomExtensionDTO[] customExtensions = field.getCustomExtensions();
    validateNoNullElements("customExtensions", customExtensions);
    validateExpression(field.getExpression());
    validateCustomExtensions(customExtensions);
    validateAIContext(field.getAiContext());
  }

  private static void validateMetric(MetricDTO metric) {
    Preconditions.checkArgument(
        metric.getName() != null && !metric.getName().isEmpty(), "name must not be null or empty");
    Preconditions.checkArgument(metric.getExpression() != null, "expression must not be null");

    CustomExtensionDTO[] customExtensions = metric.getCustomExtensions();
    validateNoNullElements("customExtensions", customExtensions);
    validateExpression(metric.getExpression());
    validateCustomExtensions(customExtensions);
    validateAIContext(metric.getAiContext());
  }

  private static void validateExpression(ExpressionDTO expression) {
    DialectExpressionDTO[] dialects = expression.getDialects();
    Preconditions.checkArgument(
        dialects != null && dialects.length > 0, "dialects must not be null or empty");
    validateNoNullElements("dialects", dialects);

    Set<String> seenDialects = new HashSet<>();
    for (DialectExpressionDTO dialect : dialects) {
      Preconditions.checkArgument(
          dialect.getDialect() != null && !dialect.getDialect().isEmpty(),
          "dialect must not be null or empty");
      Preconditions.checkArgument(
          dialect.getExpression() != null && !dialect.getExpression().isEmpty(),
          "expression must not be null or empty");
      Preconditions.checkArgument(
          seenDialects.add(dialect.getDialect()),
          "dialects must not contain duplicate dialect: %s",
          dialect.getDialect());
    }
  }

  private static void validateCustomExtensions(@Nullable CustomExtensionDTO[] customExtensions) {
    if (customExtensions == null) {
      return;
    }
    for (CustomExtensionDTO customExtension : customExtensions) {
      Preconditions.checkArgument(
          customExtension.getVendorName() != null, "vendorName must not be null");
      Preconditions.checkArgument(customExtension.getData() != null, "data must not be null");
    }
  }

  private static void validateAIContext(@Nullable AIContextDTO aiContext) {
    if (aiContext == null) {
      return;
    }
    Preconditions.checkArgument(
        (aiContext.getText() == null) != (aiContext.getObject() == null),
        "AI context must contain exactly one of text or object");
    if (aiContext.getObject() != null) {
      validateNoNullElements("synonyms", aiContext.getObject().getSynonyms());
      validateNoNullElements("examples", aiContext.getObject().getExamples());
    }
  }

  private static void validateUniqueKeys(@Nullable String[][] uniqueKeys) {
    if (uniqueKeys == null) {
      return;
    }
    for (int index = 0; index < uniqueKeys.length; index++) {
      String[] uniqueKey = uniqueKeys[index];
      Preconditions.checkArgument(
          uniqueKey != null && uniqueKey.length > 0,
          "uniqueKeys[%s] must not be null or empty",
          index);
      validateNonEmptyStringElements("uniqueKeys[" + index + "]", uniqueKey);
    }
  }

  private static void validateNoNullElements(String name, @Nullable Object[] values) {
    if (values == null) {
      return;
    }
    for (int index = 0; index < values.length; index++) {
      Preconditions.checkArgument(values[index] != null, "%s[%s] must not be null", name, index);
    }
  }

  private static void validateNonEmptyStringElements(String name, @Nullable String[] values) {
    if (values == null) {
      return;
    }
    for (int index = 0; index < values.length; index++) {
      Preconditions.checkArgument(
          values[index] != null && !values[index].isEmpty(),
          "%s[%s] must not be null or empty",
          name,
          index);
    }
  }
}
