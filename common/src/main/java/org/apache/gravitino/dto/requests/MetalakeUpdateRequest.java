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
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.MetalakeChange;
import org.apache.gravitino.rest.RESTRequest;

/** Represents an interface for Metalake update requests. */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY)
@JsonSubTypes({
  @JsonSubTypes.Type(value = MetalakeUpdateRequest.RenameMetalakeRequest.class, name = "rename"),
  @JsonSubTypes.Type(
      value = MetalakeUpdateRequest.UpdateMetalakeCommentRequest.class,
      name = "updateComment"),
  @JsonSubTypes.Type(
      value = MetalakeUpdateRequest.SetMetalakePropertyRequest.class,
      name = "setProperty"),
  @JsonSubTypes.Type(
      value = MetalakeUpdateRequest.RemoveMetalakePropertyRequest.class,
      name = "removeProperty")
})
public interface MetalakeUpdateRequest extends RESTRequest {

  /**
   * Returns the Metalake change associated with this request.
   *
   * @return The Metalake change.
   */
  MetalakeChange metalakeChange();

  /** Represents a request to rename a Metalake. */
  @EqualsAndHashCode
  @ToString
  class RenameMetalakeRequest implements MetalakeUpdateRequest {

    @Getter
    @JsonProperty("newName")
    private final String newName;

    /**
     * Constructor for RenameMetalakeRequest.
     *
     * @param newName The new name for the Metalake.
     */
    public RenameMetalakeRequest(String newName) {
      this.newName = newName;
    }

    /** Default constructor for RenameMetalakeRequest. */
    public RenameMetalakeRequest() {
      this(null);
    }

    /**
     * Validates the fields of the request.
     *
     * @throws IllegalArgumentException if the new name is not set.
     */
    @Override
    public void validate() throws IllegalArgumentException {
      Preconditions.checkArgument(
          StringUtils.isNotBlank(newName), "\"newName\" field is required and cannot be empty");
    }

    @Override
    public MetalakeChange metalakeChange() {
      return MetalakeChange.rename(newName);
    }
  }

  /** Represents a request to update the comment on a Metalake. */
  @EqualsAndHashCode
  @ToString
  class UpdateMetalakeCommentRequest implements MetalakeUpdateRequest {

    @Getter
    @JsonProperty("newComment")
    private final String newComment;

    /**
     * Constructor for UpdateMetalakeCommentRequest.
     *
     * @param newComment The new comment for the Metalake.
     */
    public UpdateMetalakeCommentRequest(String newComment) {
      this.newComment = newComment;
    }

    /** Default constructor for UpdateMetalakeCommentRequest. */
    public UpdateMetalakeCommentRequest() {
      this(null);
    }

    /** Validates the fields of the request. Always pass. */
    @Override
    public void validate() throws IllegalArgumentException {}

    @Override
    public MetalakeChange metalakeChange() {
      return MetalakeChange.updateComment(newComment);
    }
  }

  /** Represents a request to set a property on a Metalake. */
  @EqualsAndHashCode
  @ToString
  class SetMetalakePropertyRequest implements MetalakeUpdateRequest {

    @Getter
    @JsonProperty("property")
    private final String property;

    @JsonProperty("value")
    private final String value;

    /**
     * Constructor for SetMetalakePropertyRequest.
     *
     * @param property The property to set.
     * @param value The value of the property.
     */
    public SetMetalakePropertyRequest(String property, String value) {
      this.property = property;
      this.value = value;
    }

    /** Default constructor for SetMetalakePropertyRequest. */
    public SetMetalakePropertyRequest() {
      this(null, null);
    }

    /**
     * Validates the fields of the request.
     *
     * @throws IllegalArgumentException if property or value are not set.
     */
    @Override
    public void validate() throws IllegalArgumentException {
      Preconditions.checkArgument(
          StringUtils.isNotBlank(property), "\"property\" field is required and cannot be empty");
      Preconditions.checkArgument(
          StringUtils.isNotBlank(value), "\"value\" field is required and cannot be empty");
    }

    @Override
    public MetalakeChange metalakeChange() {
      return MetalakeChange.setProperty(property, value);
    }
  }

  /** Represents a request to remove a property from a Metalake. */
  @EqualsAndHashCode
  @ToString
  class RemoveMetalakePropertyRequest implements MetalakeUpdateRequest {

    @Getter
    @JsonProperty("property")
    private final String property;

    /**
     * Constructor for RemoveMetalakePropertyRequest.
     *
     * @param property The property to remove.
     */
    public RemoveMetalakePropertyRequest(String property) {
      this.property = property;
    }

    /** Default constructor for RemoveMetalakePropertyRequest. */
    public RemoveMetalakePropertyRequest() {
      this(null);
    }

    /**
     * Validates the fields of the request.
     *
     * @throws IllegalArgumentException if property is not set.
     */
    @Override
    public void validate() throws IllegalArgumentException {
      Preconditions.checkArgument(
          StringUtils.isNotBlank(property), "\"property\" field is required and cannot be empty");
    }

    @Override
    public MetalakeChange metalakeChange() {
      return MetalakeChange.removeProperty(property);
    }
  }
}
