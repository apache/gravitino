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
import javax.annotation.Nullable;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.gravitino.semantic.AIContext;
import org.apache.gravitino.semantic.CustomExtension;
import org.apache.gravitino.semantic.Relationship;

/** DTO for a relationship between Semantic Model datasets. */
@Getter
@EqualsAndHashCode
@NoArgsConstructor(access = AccessLevel.PRIVATE)
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@Builder(setterPrefix = "with")
@JsonInclude(JsonInclude.Include.NON_NULL)
public class RelationshipDTO {

  @JsonProperty("name")
  private String name;

  @JsonProperty("from")
  private String from;

  @JsonProperty("to")
  private String to;

  @JsonProperty("from_columns")
  private String[] fromColumns;

  @JsonProperty("to_columns")
  private String[] toColumns;

  @Nullable
  @JsonProperty("ai_context")
  private AIContextDTO aiContext;

  @Nullable
  @JsonProperty("custom_extensions")
  private CustomExtensionDTO[] customExtensions;

  /**
   * Creates a relationship DTO from an API model.
   *
   * @param relationship The API relationship.
   * @return The relationship DTO.
   */
  public static RelationshipDTO fromRelationship(Relationship relationship) {
    AIContext sourceAIContext = relationship.aiContext();
    return builder()
        .withName(relationship.name())
        .withFrom(relationship.from())
        .withTo(relationship.to())
        .withFromColumns(relationship.fromColumns())
        .withToColumns(relationship.toColumns())
        .withAIContext(sourceAIContext == null ? null : AIContextDTO.fromAIContext(sourceAIContext))
        .withCustomExtensions(
            SemanticDTOUtils.convertArray(
                relationship.customExtensions(),
                CustomExtensionDTO::fromCustomExtension,
                CustomExtensionDTO[]::new))
        .build();
  }

  /**
   * Converts this DTO to an API relationship.
   *
   * @return The API relationship.
   */
  public Relationship toRelationship() {
    CustomExtension[] convertedExtensions =
        SemanticDTOUtils.convertArray(
            customExtensions, CustomExtensionDTO::toCustomExtension, CustomExtension[]::new);
    return Relationship.builder()
        .withName(name)
        .withFrom(from)
        .withTo(to)
        .withFromColumns(fromColumns)
        .withToColumns(toColumns)
        .withAIContext(aiContext == null ? null : aiContext.toAIContext())
        .withCustomExtensions(convertedExtensions)
        .build();
  }

  /** Builder for {@link RelationshipDTO}. */
  public static class RelationshipDTOBuilder {

    /**
     * Sets the optional AI context.
     *
     * @param aiContext The AI context DTO.
     * @return This builder.
     */
    public RelationshipDTOBuilder withAIContext(@Nullable AIContextDTO aiContext) {
      this.aiContext = aiContext;
      return this;
    }
  }
}
