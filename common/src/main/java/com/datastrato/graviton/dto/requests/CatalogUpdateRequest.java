/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.dto.requests;

import com.datastrato.graviton.CatalogChange;
import com.datastrato.graviton.rest.RESTRequest;
import com.datastrato.graviton.util.StringUtils;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.Preconditions;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

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

    /**
     * Validates the fields of the request.
     *
     * @throws IllegalArgumentException if the new comment is not set.
     */
    @Override
    public void validate() throws IllegalArgumentException {
      Preconditions.checkArgument(
          StringUtils.isNotBlank(newComment),
          "\"newComment\" field is required and cannot be empty");
    }

    @Override
    public CatalogChange catalogChange() {
      return CatalogChange.updateComment(newComment);
    }
  }

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
