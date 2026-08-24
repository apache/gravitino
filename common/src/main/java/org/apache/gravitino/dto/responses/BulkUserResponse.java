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
import org.apache.gravitino.dto.authorization.UserDTO;

/** Represents a bulk user response. */
@Getter
@ToString
@EqualsAndHashCode(callSuper = true)
public class BulkUserResponse extends BaseResponse {

  @JsonProperty("users")
  private final UserDTO[] users;

  @JsonProperty("errors")
  private final BulkError[] errors;

  @JsonProperty("summary")
  private final BulkSummary summary;

  /**
   * Creates a new BulkUserResponse.
   *
   * @param users The successfully added users.
   * @param errors The item-level errors.
   * @param summary The summary counts.
   */
  public BulkUserResponse(UserDTO[] users, BulkError[] errors, BulkSummary summary) {
    super(0);
    this.users = users;
    this.errors = errors;
    this.summary = summary;
  }

  /** Default constructor for BulkUserResponse. (Used for Jackson deserialization.) */
  public BulkUserResponse() {
    this(null, null, null);
  }

  @Override
  public void validate() throws IllegalArgumentException {
    super.validate();
    Preconditions.checkArgument(users != null, "users must not be null");
    Preconditions.checkArgument(errors != null, "errors must not be null");
    Preconditions.checkArgument(summary != null, "summary must not be null");
    Arrays.stream(errors).forEach(BulkError::validate);
    summary.validate();
  }
}
