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
import java.util.Arrays;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.dto.authorization.GroupDTO;

/** Represents a response for a list of groups. */
@Getter
@ToString
@EqualsAndHashCode(callSuper = true)
public class GroupListResponse extends BaseResponse {

  @JsonProperty("groups")
  private final GroupDTO[] groups;

  /**
   * Constructor for GroupListResponse.
   *
   * @param groups The array of group DTOs.
   */
  public GroupListResponse(GroupDTO[] groups) {
    super(0);
    this.groups = groups;
  }

  /** Default constructor for GroupListResponse. (Used for Jackson deserialization.) */
  public GroupListResponse() {
    super();
    this.groups = null;
  }

  /**
   * Validates the response data.
   *
   * @throws IllegalArgumentException if the name or audit is not set.
   */
  @Override
  public void validate() throws IllegalArgumentException {
    super.validate();

    Preconditions.checkArgument(groups != null, "groups must not be null");
    Arrays.stream(groups)
        .forEach(
            group -> {
              Preconditions.checkArgument(
                  StringUtils.isNotBlank(group.name()), "group 'name' must not be blank");
              Preconditions.checkArgument(
                  group.auditInfo() != null, "group 'auditInfo' must not be null");
            });
  }
}
