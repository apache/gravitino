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
package org.apache.gravitino.dto.responses;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.dto.authorization.GroupDTO;

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
