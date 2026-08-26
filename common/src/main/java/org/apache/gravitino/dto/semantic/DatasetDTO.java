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
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import javax.annotation.Nullable;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.semantic.AIContext;
import org.apache.gravitino.semantic.CustomExtension;
import org.apache.gravitino.semantic.Dataset;
import org.apache.gravitino.semantic.Field;

/** DTO for a dataset in a Semantic Model definition. */
@Getter
@EqualsAndHashCode
@NoArgsConstructor(access = AccessLevel.PRIVATE)
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@Builder(setterPrefix = "with")
@JsonInclude(JsonInclude.Include.NON_NULL)
public class DatasetDTO {

  @JsonProperty("name")
  private String name;

  @JsonProperty("source")
  @JsonSerialize(using = JsonUtils.NameIdentifierSerializer.class)
  @JsonDeserialize(using = JsonUtils.NameIdentifierDeserializer.class)
  private NameIdentifier source;

  @Nullable
  @JsonProperty("primary_key")
  private String[] primaryKey;

  @Nullable
  @JsonProperty("unique_keys")
  private String[][] uniqueKeys;

  @Nullable
  @JsonProperty("description")
  private String description;

  @Nullable
  @JsonProperty("ai_context")
  private AIContextDTO aiContext;

  @Nullable
  @JsonProperty("fields")
  private FieldDTO[] fields;

  @Nullable
  @JsonProperty("custom_extensions")
  private CustomExtensionDTO[] customExtensions;

  /**
   * Creates a dataset DTO from an API model.
   *
   * @param dataset The API dataset.
   * @return The dataset DTO.
   */
  public static DatasetDTO fromDataset(Dataset dataset) {
    AIContext sourceAIContext = dataset.aiContext();
    return builder()
        .withName(dataset.name())
        .withSource(dataset.source())
        .withPrimaryKey(dataset.primaryKey())
        .withUniqueKeys(dataset.uniqueKeys())
        .withDescription(dataset.description())
        .withAIContext(sourceAIContext == null ? null : AIContextDTO.fromAIContext(sourceAIContext))
        .withFields(
            SemanticDTOUtils.convertArray(dataset.fields(), FieldDTO::fromField, FieldDTO[]::new))
        .withCustomExtensions(
            SemanticDTOUtils.convertArray(
                dataset.customExtensions(),
                CustomExtensionDTO::fromCustomExtension,
                CustomExtensionDTO[]::new))
        .build();
  }

  /**
   * Converts this DTO to an API dataset.
   *
   * @return The API dataset.
   */
  public Dataset toDataset() {
    Field[] convertedFields =
        SemanticDTOUtils.convertArray(fields, FieldDTO::toField, Field[]::new);
    CustomExtension[] convertedExtensions =
        SemanticDTOUtils.convertArray(
            customExtensions, CustomExtensionDTO::toCustomExtension, CustomExtension[]::new);
    return Dataset.builder()
        .withName(name)
        .withSource(source)
        .withPrimaryKey(primaryKey)
        .withUniqueKeys(uniqueKeys)
        .withDescription(description)
        .withAIContext(aiContext == null ? null : aiContext.toAIContext())
        .withFields(convertedFields)
        .withCustomExtensions(convertedExtensions)
        .build();
  }

  /** Builder for {@link DatasetDTO}. */
  public static class DatasetDTOBuilder {

    /**
     * Sets the optional AI context.
     *
     * @param aiContext The AI context DTO.
     * @return This builder.
     */
    public DatasetDTOBuilder withAIContext(@Nullable AIContextDTO aiContext) {
      this.aiContext = aiContext;
      return this;
    }
  }
}
