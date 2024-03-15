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

/** Request to create a group. */
@Getter
@EqualsAndHashCode
@ToString
@Builder
@Jacksonized
public class GroupCreateRequest implements RESTRequest {

  @JsonProperty("name")
  private final String name;

  @JsonProperty("users")
  private final List<String> users;

  @Nullable
  @JsonProperty("properties")
  private final Map<String, String> properties;

  /** Default constructor for GroupCreateRequest. (Used for Jackson deserialization.) */
  public GroupCreateRequest() {
    this(null, null, null);
  }

  /**
   * Creates a new GroupCreateRequest.
   *
   * @param name The name of the group.
   * @param users The users of the group.
   * @param properties The properties of the group.
   */
  public GroupCreateRequest(String name, List<String> users, Map<String, String> properties) {
    super();
    this.name = name;
    this.properties = properties;
    this.users = users;
  }

  @Override
  public void validate() throws IllegalArgumentException {
    Preconditions.checkArgument(
        StringUtils.isNotBlank(name), "\"name\" field is required and cannot be empty");
    Preconditions.checkArgument(
        CollectionUtils.isEmpty(users), "\"users\" field is required and cannot be empty");
  }
}
