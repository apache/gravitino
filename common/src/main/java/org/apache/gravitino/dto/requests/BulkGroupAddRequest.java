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
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.apache.gravitino.rest.RESTRequest;

/** Represents a request to add groups in bulk. */
@Getter
@EqualsAndHashCode
@ToString
public class BulkGroupAddRequest implements RESTRequest {

  @JsonProperty("groups")
  private final GroupAddRequest[] groups;

  /**
   * Creates a new BulkGroupAddRequest.
   *
   * @param groups The group add requests.
   */
  public BulkGroupAddRequest(GroupAddRequest[] groups) {
    this.groups = groups;
  }

  /** Default constructor for BulkGroupAddRequest. (Used for Jackson deserialization.) */
  public BulkGroupAddRequest() {
    this(null);
  }

  @Override
  public void validate() throws IllegalArgumentException {
    Preconditions.checkArgument(
        groups != null && groups.length > 0, "\"groups\" must not be empty");
    Arrays.stream(groups)
        .forEach(
            group -> {
              Preconditions.checkArgument(group != null, "group must not be null");
              group.validate();
            });
  }
}
