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
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.model.ModelChange;
import org.apache.gravitino.rest.RESTRequest;

/** Request to update a model. */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY)
@JsonSubTypes({
  @JsonSubTypes.Type(value = ModelUpdateRequest.RenameModelRequest.class, name = "rename"),
  @JsonSubTypes.Type(
      value = ModelUpdateRequest.RemoveModelPropertyRequest.class,
      name = "removeProperty"),
  @JsonSubTypes.Type(
      value = ModelUpdateRequest.SetModelPropertyRequest.class,
      name = "setProperty"),
  @JsonSubTypes.Type(
      value = ModelUpdateRequest.UpdateModelCommentRequest.class,
      name = "updateComment")
})
public interface ModelUpdateRequest extends RESTRequest {

  /**
   * Returns the model change.
   *
   * @return the model change.
   */
  ModelChange modelChange();

  /** The model update request for rename model. */
  @EqualsAndHashCode
  @ToString
  class RenameModelRequest implements ModelUpdateRequest {

    @Getter
    @JsonProperty("newName")
    private final String newName;

    /**
     * Returns the model change.
     *
     * @return An instance of ModelChange.
     */
    @Override
    public ModelChange modelChange() {
      return ModelChange.rename(newName);
    }

    /**
     * Constructor for RenameModelRequest.
     *
     * @param newName the new name of the model
     */
    public RenameModelRequest(String newName) {
      this.newName = newName;
    }

    /** Default constructor for Jackson deserialization. */
    public RenameModelRequest() {
      this(null);
    }

    /**
     * Validates the request.
     *
     * @throws IllegalArgumentException If the request is invalid, this exception is thrown.
     */
    @Override
    public void validate() throws IllegalArgumentException {
      Preconditions.checkArgument(
          StringUtils.isNotBlank(newName), "\"newName\" field is required and cannot be empty");
    }
  }

  /** The model update request for set property of model. */
  @EqualsAndHashCode
  @AllArgsConstructor
  @NoArgsConstructor(force = true)
  @ToString
  @Getter
  class SetModelPropertyRequest implements ModelUpdateRequest {
    @JsonProperty("property")
    private final String property;

    @JsonProperty("value")
    private final String value;

    /** {@inheritDoc} */
    @Override
    public ModelChange modelChange() {
      return ModelChange.setProperty(property, value);
    }

    /**
     * Validates the request, i.e., checks if the property and value are not empty and not null.
     *
     * @throws IllegalArgumentException If the request is invalid, this exception is thrown.
     */
    @Override
    public void validate() throws IllegalArgumentException {
      Preconditions.checkArgument(
          StringUtils.isNotBlank(property), "\"property\" field is required and cannot be empty");
      Preconditions.checkArgument(value != null, "\"value\" field is required and cannot be null");
    }
  }

  /** The model update request for remove property from model. */
  @EqualsAndHashCode
  @AllArgsConstructor
  @NoArgsConstructor(force = true)
  @ToString
  @Getter
  class RemoveModelPropertyRequest implements ModelUpdateRequest {

    @JsonProperty("property")
    private final String property;

    /** {@inheritDoc} */
    @Override
    public ModelChange modelChange() {
      return ModelChange.removeProperty(property);
    }

    /**
     * Validates the request, i.e., checks if the property is not empty.
     *
     * @throws IllegalArgumentException If the request is invalid, this exception is thrown.
     */
    @Override
    public void validate() throws IllegalArgumentException {
      Preconditions.checkArgument(
          StringUtils.isNotBlank(property), "\"property\" field is required and cannot be empty");
    }
  }

  /** The model update request for update comment of model. */
  @EqualsAndHashCode
  @AllArgsConstructor
  @NoArgsConstructor(force = true)
  @ToString
  @Getter
  class UpdateModelCommentRequest implements ModelUpdateRequest {
    @JsonProperty("newComment")
    private final String newComment;

    /** {@inheritDoc} */
    @Override
    public ModelChange modelChange() {
      return ModelChange.updateComment(newComment);
    }

    /** Validates the fields of the request. Always pass. */
    @Override
    public void validate() throws IllegalArgumentException {}
  }
}
