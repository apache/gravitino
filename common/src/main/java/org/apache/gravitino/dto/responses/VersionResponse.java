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
import org.apache.gravitino.dto.VersionDTO;

/** Represents a response containing version of Gravitino. */
@Getter
@EqualsAndHashCode(callSuper = true)
@ToString
public class VersionResponse extends BaseResponse {

  @JsonProperty("version")
  private final VersionDTO version;

  /**
   * Constructor for VersionResponse.
   *
   * @param version The version of Gravitino.
   */
  public VersionResponse(VersionDTO version) {
    super(0);
    this.version = version;
  }

  /** Default constructor for VersionResponse. (Used for Jackson deserialization.) */
  public VersionResponse() {
    super();
    this.version = null;
  }

  /**
   * Validates the response data.
   *
   * @throws IllegalArgumentException if name or audit information is not set.
   */
  @Override
  public void validate() throws IllegalArgumentException {
    super.validate();

    Preconditions.checkArgument(version != null, "version DTO must be non-null");
    Preconditions.checkArgument(version.version() != null, "version must be non-null");
    Preconditions.checkArgument(version.compileDate() != null, "compile data must be non-null");
    Preconditions.checkArgument(version.gitCommit() != null, "Git commit id must be non-null");
  }
}
