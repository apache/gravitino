/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.dto.responses;

import com.datastrato.gravitino.dto.authorization.UserDTO;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.apache.commons.lang3.StringUtils;

/** Represents a response for a user. */
@Getter
@ToString
@EqualsAndHashCode(callSuper = true)
public class UserResponse extends BaseResponse {

  @JsonProperty("user")
  private final UserDTO user;

  /**
   * Constructor for UserResponse.
   *
   * @param user The user data transfer object.
   */
  public UserResponse(UserDTO user) {
    super(0);
    this.user = user;
  }

  /** Default constructor for UserResponse. (Used for Jackson deserialization.) */
  public UserResponse() {
    super();
    this.user = null;
  }

  /**
   * Validates the response data.
   *
   * @throws IllegalArgumentException if the name or audit is not set.
   */
  @Override
  public void validate() throws IllegalArgumentException {
    super.validate();

    Preconditions.checkArgument(user != null, "user must not be null");
    Preconditions.checkArgument(
        StringUtils.isNotBlank(user.name()), "user 'name' must not be null and empty");
    Preconditions.checkArgument(user.auditInfo() != null, "user 'auditInfo' must not be null");
  }
}
