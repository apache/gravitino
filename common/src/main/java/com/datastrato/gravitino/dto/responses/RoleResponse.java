/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.dto.responses;

import com.datastrato.gravitino.dto.authorization.RoleDTO;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

/** Represents a response for a role. */
@Getter
@ToString
@EqualsAndHashCode(callSuper = true)
public class RoleResponse extends BaseResponse {

  @JsonProperty("role")
  private final RoleDTO role;

  /**
   * Constructor for RoleResponse.
   *
   * @param role The role data transfer object.
   */
  public RoleResponse(RoleDTO role) {
    super(0);
    this.role = role;
  }

  /** Default constructor for RoleResponse. (Used for Jackson deserialization.) */
  public RoleResponse() {
    super();
    this.role = null;
  }

  /**
   * Validates the response data.
   *
   * @throws IllegalArgumentException if the name or audit is not set.
   */
  @Override
  public void validate() throws IllegalArgumentException {
    super.validate();

    Preconditions.checkArgument(role != null, "role must not be null");
    Preconditions.checkArgument(
        StringUtils.isNotBlank(role.name()), "role 'name' must not be null and empty");
    Preconditions.checkArgument(role.auditInfo() != null, "role 'auditInfo' must not be null");
    Preconditions.checkArgument(
        !CollectionUtils.isEmpty(role.privileges()), "role 'privileges' can't be empty");
    Preconditions.checkArgument(
        role.securableObject() != null, "role 'securableObject' can't null");
  }
}
