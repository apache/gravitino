/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.dto.responses;

import com.datastrato.gravitino.dto.authorization.GroupDTO;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.apache.commons.lang3.StringUtils;

/** Represents a response for a group. */
@Getter
@ToString
@EqualsAndHashCode(callSuper = true)
public class GroupResponse extends BaseResponse {

  @JsonProperty("group")
  private final GroupDTO group;

  /**
   * Constructor for GroupResponse.
   *
   * @param group The group data transfer object.
   */
  public GroupResponse(GroupDTO group) {
    super(0);
    this.group = group;
  }

  /** Default constructor for GroupResponse. (Used for Jackson deserialization.) */
  public GroupResponse() {
    super();
    this.group = null;
  }

  /**
   * Validates the response data.
   *
   * @throws IllegalArgumentException if the name or audit is not set.
   */
  @Override
  public void validate() throws IllegalArgumentException {
    super.validate();

    Preconditions.checkArgument(group != null, "group must not be null");
    Preconditions.checkArgument(
        StringUtils.isNotBlank(group.name()), "group 'name' must not be null and empty");
    Preconditions.checkArgument(group.auditInfo() != null, "group 'auditInfo' must not be null");
  }
}
