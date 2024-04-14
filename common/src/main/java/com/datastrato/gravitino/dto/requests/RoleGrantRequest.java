/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.dto.requests;

import com.datastrato.gravitino.rest.RESTRequest;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.jackson.Jacksonized;
import org.apache.commons.lang3.StringUtils;

/** Represents a request to add a role in the user or the group. */
@Getter
@EqualsAndHashCode
@ToString
@Builder
@Jacksonized
public class RoleGrantRequest implements RESTRequest {
  @JsonProperty("roleName")
  private final String roleName;

  /**
   * Constructor for RoleAddRequest.
   *
   * @param roleName The roleName for the RoleAddRequest.
   */
  public RoleGrantRequest(String roleName) {
    this.roleName = roleName;
  }

  /** Default constructor for RoleAddRequest. */
  public RoleGrantRequest() {
    this(null);
  }

  /**
   * Validates the fields of the request.
   *
   * @throws IllegalArgumentException if the role name is not set.
   */
  @Override
  public void validate() throws IllegalArgumentException {
    Preconditions.checkArgument(
        StringUtils.isNotBlank(roleName), "\"roleName\" field is required and cannot be empty");
  }
}
