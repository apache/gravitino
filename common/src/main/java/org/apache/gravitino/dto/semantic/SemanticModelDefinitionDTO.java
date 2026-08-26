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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import javax.annotation.Nullable;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.gravitino.semantic.AIContext;
import org.apache.gravitino.semantic.CustomExtension;
import org.apache.gravitino.semantic.Dataset;
import org.apache.gravitino.semantic.Metric;
import org.apache.gravitino.semantic.Relationship;
import org.apache.gravitino.semantic.SemanticModelDefinition;

/** DTO for the complete persisted definition of a Semantic Model. */
@Getter
@EqualsAndHashCode
@NoArgsConstructor(access = AccessLevel.PRIVATE)
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@Builder(setterPrefix = "with")
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"ai_context", "datasets", "relationships", "metrics", "custom_extensions"})
public class SemanticModelDefinitionDTO {

  @Nullable
  @JsonProperty("ai_context")
  private AIContextDTO aiContext;

  @JsonProperty("datasets")
  private DatasetDTO[] datasets;

  @Nullable
  @JsonProperty("relationships")
  private RelationshipDTO[] relationships;

  @Nullable
  @JsonProperty("metrics")
  private MetricDTO[] metrics;

  @Nullable
  @JsonProperty("custom_extensions")
  private CustomExtensionDTO[] customExtensions;

  /**
   * Creates a persistence DTO from an API Semantic Model definition.
   *
   * @param definition The API Semantic Model definition.
   * @return The persistence DTO.
   */
  public static SemanticModelDefinitionDTO fromDefinition(SemanticModelDefinition definition) {
    AIContext sourceAIContext = definition.aiContext();
    return builder()
        .withAIContext(sourceAIContext == null ? null : AIContextDTO.fromAIContext(sourceAIContext))
        .withDatasets(
            SemanticDTOUtils.convertArray(
                definition.datasets(), DatasetDTO::fromDataset, DatasetDTO[]::new))
        .withRelationships(
            SemanticDTOUtils.convertArray(
                definition.relationships(),
                RelationshipDTO::fromRelationship,
                RelationshipDTO[]::new))
        .withMetrics(
            SemanticDTOUtils.convertArray(
                definition.metrics(), MetricDTO::fromMetric, MetricDTO[]::new))
        .withCustomExtensions(
            SemanticDTOUtils.convertArray(
                definition.customExtensions(),
                CustomExtensionDTO::fromCustomExtension,
                CustomExtensionDTO[]::new))
        .build();
  }

  /**
   * Converts this persistence DTO to an API Semantic Model definition.
   *
   * @return The API Semantic Model definition.
   */
  public SemanticModelDefinition toDefinition() {
    Dataset[] convertedDatasets =
        SemanticDTOUtils.convertArray(datasets, DatasetDTO::toDataset, Dataset[]::new);
    Relationship[] convertedRelationships =
        SemanticDTOUtils.convertArray(
            relationships, RelationshipDTO::toRelationship, Relationship[]::new);
    Metric[] convertedMetrics =
        SemanticDTOUtils.convertArray(metrics, MetricDTO::toMetric, Metric[]::new);
    CustomExtension[] convertedExtensions =
        SemanticDTOUtils.convertArray(
            customExtensions, CustomExtensionDTO::toCustomExtension, CustomExtension[]::new);
    return SemanticModelDefinition.builder()
        .withAIContext(aiContext == null ? null : aiContext.toAIContext())
        .withDatasets(convertedDatasets)
        .withRelationships(convertedRelationships)
        .withMetrics(convertedMetrics)
        .withCustomExtensions(convertedExtensions)
        .build();
  }

  /** Builder for {@link SemanticModelDefinitionDTO}. */
  public static class SemanticModelDefinitionDTOBuilder {

    /**
     * Sets the optional AI context.
     *
     * @param aiContext The AI context DTO.
     * @return This builder.
     */
    public SemanticModelDefinitionDTOBuilder withAIContext(@Nullable AIContextDTO aiContext) {
      this.aiContext = aiContext;
      return this;
    }
  }
}
