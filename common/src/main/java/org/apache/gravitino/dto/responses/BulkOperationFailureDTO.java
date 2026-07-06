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

/** Represents a failed item in a bulk operation response. */
@Getter
@EqualsAndHashCode
@ToString
public class BulkOperationFailureDTO {

  @JsonProperty("name")
  private final String name;

  @JsonProperty("reason")
  private final String reason;

  /** Default constructor for BulkOperationFailureDTO. (Used for Jackson deserialization.) */
  public BulkOperationFailureDTO() {
    this(null, null);
  }

  /**
   * Creates a new BulkOperationFailureDTO.
   *
   * @param name The failed entity name.
   * @param reason The failure reason.
   */
  public BulkOperationFailureDTO(String name, String reason) {
    this.name = name;
    this.reason = reason;
  }

  /** Validates this failed item. */
  public void validate() {
    Preconditions.checkArgument(StringUtils.isNotBlank(name), "\"name\" must not be blank");
    Preconditions.checkArgument(StringUtils.isNotBlank(reason), "\"reason\" must not be blank");
  }
}
