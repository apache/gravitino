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
import org.apache.gravitino.CatalogChange;
import org.apache.gravitino.rest.RESTRequest;

/** Represents an interface for catalog update requests. */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY)
@JsonSubTypes({
  @JsonSubTypes.Type(value = CatalogUpdateRequest.RenameCatalogRequest.class, name = "rename"),
  @JsonSubTypes.Type(
      value = CatalogUpdateRequest.UpdateCatalogCommentRequest.class,
      name = "updateComment"),
  @JsonSubTypes.Type(
      value = CatalogUpdateRequest.SetCatalogPropertyRequest.class,
      name = "setProperty"),
  @JsonSubTypes.Type(
      value = CatalogUpdateRequest.RemoveCatalogPropertyRequest.class,
      name = "removeProperty")
})
public interface CatalogUpdateRequest extends RESTRequest {

  /**
   * Returns the catalog change associated with this request.
   *
   * @return The catalog change.
   */
  CatalogChange catalogChange();

  /** Request to rename a catalog. */
  @EqualsAndHashCode
  @ToString
  class RenameCatalogRequest implements CatalogUpdateRequest {

    @Getter
    @JsonProperty("newName")
    private final String newName;

    /** Default constructor for RenameCatalogRequest. */
    public RenameCatalogRequest() {
      this(null);
    }

    /**
     * Constructor for RenameCatalogRequest.
     *
     * @param newName The new name for the catalog.
     */
    public RenameCatalogRequest(String newName) {
      this.newName = newName;
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
    public CatalogChange catalogChange() {
      return CatalogChange.rename(newName);
    }
  }

  /** Request to update the comment of a catalog. */
  @EqualsAndHashCode
  @ToString
  class UpdateCatalogCommentRequest implements CatalogUpdateRequest {

    @Getter
    @JsonProperty("newComment")
    private final String newComment;

    /**
     * Constructor for UpdateCatalogCommentRequest.
     *
     * @param newComment The new comment for the catalog.
     */
    public UpdateCatalogCommentRequest(String newComment) {
      this.newComment = newComment;
    }

    /** Default constructor for UpdateCatalogCommentRequest. */
    public UpdateCatalogCommentRequest() {
      this(null);
    }

    /** Validates the fields of the request. Always pass. */
    @Override
    public void validate() throws IllegalArgumentException {}

    @Override
    public CatalogChange catalogChange() {
      return CatalogChange.updateComment(newComment);
    }
  }

  /** Request to set a property on a catalog. */
  @EqualsAndHashCode
  @ToString
  class SetCatalogPropertyRequest implements CatalogUpdateRequest {

    @Getter
    @JsonProperty("property")
    private final String property;

    @Getter
    @JsonProperty("value")
    private final String value;

    /**
     * Constructor for SetCatalogPropertyRequest.
     *
     * @param property The property to set.
     * @param value The value of the property.
     */
    public SetCatalogPropertyRequest(String property, String value) {
      this.property = property;
      this.value = value;
    }

    /** Default constructor for SetCatalogPropertyRequest. */
    public SetCatalogPropertyRequest() {
      this(null, null);
    }

    /**
     * Validates the fields of the request.
     *
     * @throws IllegalArgumentException if the property or value is not set.
     */
    @Override
    public void validate() throws IllegalArgumentException {
      Preconditions.checkArgument(
          StringUtils.isNotBlank(property), "\"property\" field is required and cannot be empty");
      Preconditions.checkArgument(
          StringUtils.isNotBlank(value), "\"value\" field is required and cannot be empty");
    }

    @Override
    public CatalogChange catalogChange() {
      return CatalogChange.setProperty(property, value);
    }
  }

  /** Request to remove a property from a catalog. */
  @EqualsAndHashCode
  @ToString
  class RemoveCatalogPropertyRequest implements CatalogUpdateRequest {

    @Getter
    @JsonProperty("property")
    private final String property;

    /**
     * Constructor for RemoveCatalogPropertyRequest.
     *
     * @param property The property to remove.
     */
    public RemoveCatalogPropertyRequest(String property) {
      this.property = property;
    }

    /** Default constructor for RemoveCatalogPropertyRequest. */
    public RemoveCatalogPropertyRequest() {
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
    public CatalogChange catalogChange() {
      return CatalogChange.removeProperty(property);
    }
  }
}
