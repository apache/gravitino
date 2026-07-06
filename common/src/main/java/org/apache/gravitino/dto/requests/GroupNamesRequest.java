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
package org.apache.gravitino.dto.requests;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import java.util.Arrays;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.jackson.Jacksonized;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.rest.RESTRequest;

/** Represents a request to operate on multiple groups. */
@Getter
@EqualsAndHashCode
@ToString
@Builder
@Jacksonized
public class GroupNamesRequest implements RESTRequest {

  @JsonProperty("groupNames")
  private final String[] groupNames;

  /** Default constructor for GroupNamesRequest. (Used for Jackson deserialization.) */
  public GroupNamesRequest() {
    this(null);
  }

  /**
   * Creates a new GroupNamesRequest.
   *
   * @param groupNames The group names to operate on.
   */
  public GroupNamesRequest(String[] groupNames) {
    this.groupNames = groupNames;
  }

  @Override
  public void validate() throws IllegalArgumentException {
    Preconditions.checkArgument(groupNames != null, "\"groupNames\" field is required");
    Preconditions.checkArgument(groupNames.length > 0, "\"groupNames\" field cannot be empty");
    Arrays.stream(groupNames)
        .forEach(
            groupName ->
                Preconditions.checkArgument(
                    StringUtils.isNotBlank(groupName), "\"groupNames\" cannot contain blank name"));
    BulkRequestValidator.checkNoDuplicateNames("groupNames", groupNames);
  }
}
