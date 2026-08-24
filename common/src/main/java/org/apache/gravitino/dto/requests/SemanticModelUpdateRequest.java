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
package org.apache.gravitino.dto.requests;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.Preconditions;
import javax.annotation.Nullable;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.dto.semantic.SemanticModelDefinitionDTO;
import org.apache.gravitino.rest.RESTRequest;
import org.apache.gravitino.semantic.SemanticModelChange;

/** Represents one change in a request to alter a Semantic Model. */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY)
@JsonSubTypes({
  @JsonSubTypes.Type(
      value = SemanticModelUpdateRequest.RenameSemanticModelRequest.class,
      name = "rename"),
  @JsonSubTypes.Type(
      value = SemanticModelUpdateRequest.UpdateSemanticModelCommentRequest.class,
      name = "updateComment"),
  @JsonSubTypes.Type(
      value = SemanticModelUpdateRequest.SetSemanticModelPropertyRequest.class,
      name = "setProperty"),
  @JsonSubTypes.Type(
      value = SemanticModelUpdateRequest.RemoveSemanticModelPropertyRequest.class,
      name = "removeProperty"),
  @JsonSubTypes.Type(
      value = SemanticModelUpdateRequest.ReplaceSemanticModelDefinitionRequest.class,
      name = "replaceDefinition")
})
public interface SemanticModelUpdateRequest extends RESTRequest {

  /**
   * Returns the Semantic Model change represented by this request.
   *
   * @return The Semantic Model change.
   */
  SemanticModelChange semanticModelChange();

  /** Represents a request to rename a Semantic Model. */
  @Getter
  @EqualsAndHashCode
  @ToString
  class RenameSemanticModelRequest implements SemanticModelUpdateRequest {

    @JsonProperty("newName")
    private final String newName;

    /** Default constructor for Jackson deserialization. */
    public RenameSemanticModelRequest() {
      this(null);
    }

    /**
     * Creates a request to rename a Semantic Model.
     *
     * @param newName The new Semantic Model name.
     */
    public RenameSemanticModelRequest(String newName) {
      this.newName = newName;
    }

    @Override
    public void validate() throws IllegalArgumentException {
      Preconditions.checkArgument(
          StringUtils.isNotBlank(newName), "\"newName\" field is required and cannot be empty");
    }

    @Override
    public SemanticModelChange semanticModelChange() {
      return SemanticModelChange.rename(newName);
    }
  }

  /** Represents a request to update a Semantic Model comment. */
  @Getter
  @EqualsAndHashCode
  @ToString
  class UpdateSemanticModelCommentRequest implements SemanticModelUpdateRequest {

    @Nullable
    @JsonProperty("newComment")
    private final String newComment;

    /** Default constructor for Jackson deserialization. */
    public UpdateSemanticModelCommentRequest() {
      this(null);
    }

    /**
     * Creates a request to update or clear a Semantic Model comment.
     *
     * @param newComment The new comment, or {@code null} to clear it.
     */
    public UpdateSemanticModelCommentRequest(@Nullable String newComment) {
      this.newComment = newComment;
    }

    @Override
    public void validate() throws IllegalArgumentException {
      // A null comment clears the current comment; an empty comment is stored as supplied.
    }

    @Override
    public SemanticModelChange semanticModelChange() {
      return SemanticModelChange.updateComment(newComment);
    }
  }

  /** Represents a request to set a Semantic Model property. */
  @Getter
  @EqualsAndHashCode
  @ToString
  class SetSemanticModelPropertyRequest implements SemanticModelUpdateRequest {

    @JsonProperty("property")
    private final String property;

    @JsonProperty("value")
    private final String value;

    /** Default constructor for Jackson deserialization. */
    public SetSemanticModelPropertyRequest() {
      this(null, null);
    }

    /**
     * Creates a request to set a Semantic Model property.
     *
     * @param property The property name.
     * @param value The property value.
     */
    public SetSemanticModelPropertyRequest(String property, String value) {
      this.property = property;
      this.value = value;
    }

    @Override
    public void validate() throws IllegalArgumentException {
      Preconditions.checkArgument(
          StringUtils.isNotBlank(property), "\"property\" field is required and cannot be empty");
      Preconditions.checkArgument(value != null, "\"value\" field is required and cannot be null");
    }

    @Override
    public SemanticModelChange semanticModelChange() {
      return SemanticModelChange.setProperty(property, value);
    }
  }

  /** Represents a request to remove a Semantic Model property. */
  @Getter
  @EqualsAndHashCode
  @ToString
  class RemoveSemanticModelPropertyRequest implements SemanticModelUpdateRequest {

    @JsonProperty("property")
    private final String property;

    /** Default constructor for Jackson deserialization. */
    public RemoveSemanticModelPropertyRequest() {
      this(null);
    }

    /**
     * Creates a request to remove a Semantic Model property.
     *
     * @param property The property name.
     */
    public RemoveSemanticModelPropertyRequest(String property) {
      this.property = property;
    }

    @Override
    public void validate() throws IllegalArgumentException {
      Preconditions.checkArgument(
          StringUtils.isNotBlank(property), "\"property\" field is required and cannot be empty");
    }

    @Override
    public SemanticModelChange semanticModelChange() {
      return SemanticModelChange.removeProperty(property);
    }
  }

  /** Represents a request to replace the complete Semantic Model definition. */
  @Getter
  @EqualsAndHashCode
  @ToString
  class ReplaceSemanticModelDefinitionRequest implements SemanticModelUpdateRequest {

    @JsonProperty("definition")
    private final SemanticModelDefinitionDTO definition;

    /** Default constructor for Jackson deserialization. */
    public ReplaceSemanticModelDefinitionRequest() {
      this(null);
    }

    /**
     * Creates a request to replace the complete Semantic Model definition.
     *
     * @param definition The replacement definition DTO.
     */
    public ReplaceSemanticModelDefinitionRequest(SemanticModelDefinitionDTO definition) {
      this.definition = definition;
    }

    @Override
    public void validate() throws IllegalArgumentException {
      Preconditions.checkArgument(
          definition != null, "\"definition\" field is required and cannot be null");
      definition.toDefinition();
    }

    @Override
    public SemanticModelChange semanticModelChange() {
      return SemanticModelChange.replaceDefinition(definition.toDefinition());
    }
  }
}
