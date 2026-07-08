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

/** Represents a request to operate on multiple users. */
@Getter
@EqualsAndHashCode
@ToString
@Builder
@Jacksonized
public class UsernamesRequest implements RESTRequest {

  @JsonProperty("usernames")
  private final String[] usernames;

  /** Default constructor for UsernamesRequest. (Used for Jackson deserialization.) */
  public UsernamesRequest() {
    this(null);
  }

  /**
   * Creates a new UsernamesRequest.
   *
   * @param usernames The usernames to operate on.
   */
  public UsernamesRequest(String[] usernames) {
    this.usernames = usernames;
  }

  /**
   * Validates the {@link UsernamesRequest} request.
   *
   * @throws IllegalArgumentException If the request is invalid, this exception is thrown.
   */
  @Override
  public void validate() throws IllegalArgumentException {
    Preconditions.checkArgument(usernames != null, "\"usernames\" field is required");
    Preconditions.checkArgument(usernames.length > 0, "\"usernames\" field cannot be empty");
    Arrays.stream(usernames)
        .forEach(
            username ->
                Preconditions.checkArgument(
                    StringUtils.isNotBlank(username), "\"usernames\" cannot contain blank name"));
    BulkRequestValidator.checkNoDuplicateNames("usernames", usernames);
  }
}
