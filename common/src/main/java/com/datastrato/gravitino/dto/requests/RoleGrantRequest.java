/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.dto.requests;

import com.datastrato.gravitino.rest.RESTRequest;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import java.util.List;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.jackson.Jacksonized;

/** Represents a request to grant roles to the user or the group. */
@Getter
@EqualsAndHashCode
@ToString
@Builder
@Jacksonized
public class RoleGrantRequest implements RESTRequest {
  @JsonProperty("roleNames")
  private final List<String> roleNames;

  /**
   * Constructor for RoleGrantRequest.
   *
   * @param roleNames The roleName for the RoleGrantRequest.
   */
  public RoleGrantRequest(List<String> roleNames) {
    this.roleNames = roleNames;
  }

  /** Default constructor for RoleGrantRequest. */
  public RoleGrantRequest() {
    this(null);
  }

  /**
   * Validates the fields of the request.
   *
   * @throws IllegalArgumentException if the role names is not set or empty.
   */
  @Override
  public void validate() throws IllegalArgumentException {
    Preconditions.checkArgument(
        roleNames != null && !roleNames.isEmpty(),
        "\"roleName\" field is required and cannot be empty");
  }
}
