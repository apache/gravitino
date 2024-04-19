/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.dto.requests;

import com.datastrato.gravitino.rest.RESTRequest;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.jackson.Jacksonized;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

/** Represents a request to create a role. */
@Getter
@EqualsAndHashCode
@ToString
@Builder
@Jacksonized
public class RoleCreateRequest implements RESTRequest {

  @JsonProperty("name")
  private final String name;

  @Nullable
  @JsonProperty("properties")
  private Map<String, String> properties;

  @JsonProperty("privileges")
  private List<String> privileges;

  @JsonProperty("securableObject")
  private String securableObject;

  /** Default constructor for RoleCreateRequest. (Used for Jackson deserialization.) */
  public RoleCreateRequest() {
    this(null, null, null, null);
  }

  /**
   * Creates a new RoleCreateRequest.
   *
   * @param name The name of the role.
   * @param properties The properties of the role.
   * @param securableObject The securable object of the role.
   * @param privileges The privileges of the role.
   */
  public RoleCreateRequest(
      String name,
      Map<String, String> properties,
      List<String> privileges,
      String securableObject) {
    super();
    this.name = name;
    this.properties = properties;
    this.privileges = privileges;
    this.securableObject = securableObject;
  }

  /**
   * Validates the {@link RoleCreateRequest} request.
   *
   * @throws IllegalArgumentException If the request is invalid, this exception is thrown.
   */
  @Override
  public void validate() throws IllegalArgumentException {
    Preconditions.checkArgument(
        StringUtils.isNotBlank(name), "\"name\" field is required and cannot be empty");

    Preconditions.checkArgument(
        !CollectionUtils.isEmpty(privileges), "\"privileges\" can't be empty");

    Preconditions.checkArgument(securableObject != null, "\"securableObject\" can't null");
  }
}
