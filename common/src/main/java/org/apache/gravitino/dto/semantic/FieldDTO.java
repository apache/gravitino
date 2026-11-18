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
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.gravitino.semantic.AIContext;
import org.apache.gravitino.semantic.CustomExtension;
import org.apache.gravitino.semantic.DataType;
import org.apache.gravitino.semantic.Dimension;
import org.apache.gravitino.semantic.Field;

/** DTO for a field in a Semantic Model dataset. */
@Getter
@EqualsAndHashCode
@NoArgsConstructor(access = AccessLevel.PRIVATE)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class FieldDTO {

  @JsonProperty("name")
  private String name;

  @JsonProperty("expression")
  private ExpressionDTO expression;

  @Nullable
  @JsonProperty("dimension")
  private DimensionDTO dimension;

  @Nullable
  @JsonProperty("label")
  private String label;

  @Nullable
  @JsonProperty("description")
  private String description;

  @Nullable
  @JsonProperty("datatype")
  @JsonSerialize(using = SemanticDTOUtils.DataTypeSerializer.class)
  @JsonDeserialize(using = SemanticDTOUtils.DataTypeDeserializer.class)
  private DataType datatype;

  @Nullable
  @JsonProperty("aiContext")
  private AIContextDTO aiContext;

  @Nullable
  @JsonProperty("customExtensions")
  @Getter(AccessLevel.NONE)
  private CustomExtensionDTO[] customExtensions;

  @Builder(setterPrefix = "with")
  private FieldDTO(
      String name,
      ExpressionDTO expression,
      @Nullable DimensionDTO dimension,
      @Nullable String label,
      @Nullable String description,
      @Nullable DataType datatype,
      @Nullable AIContextDTO aiContext,
      @Nullable CustomExtensionDTO[] customExtensions) {
    this.name = name;
    this.expression = expression;
    this.dimension = dimension;
    this.label = label;
    this.description = description;
    this.datatype = datatype;
    this.aiContext = aiContext;
    this.customExtensions = SemanticDTOUtils.copyArray(customExtensions);
  }

  /**
   * Returns the custom extensions associated with the field.
   *
   * @return A defensive copy of the custom extensions, or {@code null} when not provided.
   */
  @Nullable
  public CustomExtensionDTO[] getCustomExtensions() {
    return SemanticDTOUtils.copyArray(customExtensions);
  }

  /**
   * Creates a field DTO from an API model.
   *
   * @param field The API field.
   * @return The field DTO.
   */
  public static FieldDTO fromField(Field field) {
    Dimension sourceDimension = field.dimension();
    AIContext sourceAIContext = field.aiContext();
    return builder()
        .withName(field.name())
        .withExpression(ExpressionDTO.fromExpression(field.expression()))
        .withDimension(sourceDimension == null ? null : DimensionDTO.fromDimension(sourceDimension))
        .withLabel(field.label())
        .withDescription(field.description())
        .withDatatype(field.datatype())
        .withAiContext(sourceAIContext == null ? null : AIContextDTO.fromAIContext(sourceAIContext))
        .withCustomExtensions(
            SemanticDTOUtils.convertArray(
                field.customExtensions(),
                CustomExtensionDTO::fromCustomExtension,
                CustomExtensionDTO[]::new))
        .build();
  }

  /**
   * Converts this DTO to an API field.
   *
   * @return The API field.
   */
  public Field toField() {
    CustomExtension[] convertedExtensions =
        SemanticDTOUtils.convertArray(
            customExtensions, CustomExtensionDTO::toCustomExtension, CustomExtension[]::new);
    return Field.builder()
        .withName(name)
        .withExpression(expression == null ? null : expression.toExpression())
        .withDimension(dimension == null ? null : dimension.toDimension())
        .withLabel(label)
        .withDescription(description)
        .withDatatype(datatype)
        .withAIContext(aiContext == null ? null : aiContext.toAIContext())
        .withCustomExtensions(convertedExtensions)
        .build();
  }

  /** Builder for {@link FieldDTO}. */
  public static class FieldDTOBuilder {

    /**
     * Sets the optional AI context.
     *
     * @param aiContext The AI context DTO.
     * @return This builder.
     */
    public FieldDTOBuilder withAiContext(@Nullable AIContextDTO aiContext) {
      this.aiContext = aiContext;
      return this;
    }
  }
}
