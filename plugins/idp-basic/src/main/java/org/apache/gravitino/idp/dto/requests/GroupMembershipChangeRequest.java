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

package org.apache.gravitino.idp.dto.requests;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.rest.RESTRequest;

/** Represents a request to change built-in IdP group membership. */
@Getter
@EqualsAndHashCode
@ToString
public class GroupMembershipChangeRequest implements RESTRequest {

  @JsonProperty("usersToAdd")
  private final String[] usersToAdd;

  @JsonProperty("usersToRemove")
  private final String[] usersToRemove;

  /**
   * Creates a new GroupMembershipChangeRequest.
   *
   * @param usersToAdd The user names to add to the built-in IdP group.
   * @param usersToRemove The user names to remove from the built-in IdP group.
   */
  public GroupMembershipChangeRequest(String[] usersToAdd, String[] usersToRemove) {
    this.usersToAdd = usersToAdd;
    this.usersToRemove = usersToRemove;
  }

  /** Default constructor for GroupMembershipChangeRequest. (Used for Jackson deserialization.) */
  public GroupMembershipChangeRequest() {
    this(null, null);
  }

  /**
   * Validates the {@link GroupMembershipChangeRequest} request.
   *
   * @throws IllegalArgumentException If the request is invalid, this exception is thrown.
   */
  @Override
  public void validate() throws IllegalArgumentException {
    Preconditions.checkArgument(
        usersToAdd != null || usersToRemove != null,
        "usersToAdd and usersToRemove cannot both be null");

    if (usersToAdd != null) {
      for (String user : usersToAdd) {
        Preconditions.checkArgument(
            StringUtils.isNotBlank(user), "usersToAdd must not contain null or empty user names");
      }
    }

    if (usersToRemove != null) {
      for (String user : usersToRemove) {
        Preconditions.checkArgument(
            StringUtils.isNotBlank(user),
            "usersToRemove must not contain null or empty user names");
      }
    }
  }
}
