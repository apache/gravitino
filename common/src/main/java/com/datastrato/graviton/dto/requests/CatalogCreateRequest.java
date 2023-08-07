/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.dto.requests;

import com.datastrato.graviton.Catalog;
import com.datastrato.graviton.rest.RESTRequest;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import java.util.Map;
import javax.annotation.Nullable;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/** Represents a request to create a catalog. */
@Getter
@EqualsAndHashCode
@ToString
public class CatalogCreateRequest implements RESTRequest {

  @JsonProperty("name")
  private final String name;

  @JsonProperty("type")
  private final Catalog.Type type;

  @Nullable
  @JsonProperty("comment")
  private final String comment;

  @Nullable
  @JsonProperty("properties")
  private final Map<String, String> properties;

  /** Default constructor for CatalogCreateRequest. */
  public CatalogCreateRequest() {
    this(null, null, null, null);
  }

  /**
   * Constructor for CatalogCreateRequest.
   *
   * @param name The name of the catalog.
   * @param type The type of the catalog.
   * @param comment The comment for the catalog.
   * @param properties The properties for the catalog.
   */
  public CatalogCreateRequest(
      String name, Catalog.Type type, String comment, Map<String, String> properties) {
    this.name = name;
    this.type = type;
    this.comment = comment;
    this.properties = properties;
  }

  /**
   * Validates the fields of the request.
   *
   * @throws IllegalArgumentException if name or type are not set.
   */
  @Override
  public void validate() throws IllegalArgumentException {
    Preconditions.checkArgument(
        name != null && !name.isEmpty(), "\"name\" field is required and cannot be empty");
    Preconditions.checkArgument(type != null, "\"type\" field is required and cannot be empty");
  }
}
