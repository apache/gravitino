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
import lombok.EqualsAndHashCode;
import lombok.ToString;

/** Represents a response containing a list of partition names. */
@EqualsAndHashCode(callSuper = true)
@ToString
public class PartitionNameListResponse extends BaseResponse {

  @JsonProperty("names")
  private final String[] partitionNames;

  /**
   * Constructor for PartitionNameListResponse.
   *
   * @param partitionNames The array of partition names.
   */
  public PartitionNameListResponse(String[] partitionNames) {
    super(0);
    this.partitionNames = partitionNames;
  }

  /** Default constructor for PartitionNameListResponse. (Used for Jackson deserialization.) */
  public PartitionNameListResponse() {
    super();
    this.partitionNames = null;
  }

  /**
   * @return The array of partition names.
   */
  public String[] partitionNames() {
    return partitionNames;
  }

  /**
   * Validates the response data.
   *
   * @throws IllegalArgumentException if partition names are not set.
   */
  @Override
  public void validate() throws IllegalArgumentException {
    super.validate();
    if (partitionNames == null) {
      throw new IllegalArgumentException("partition names must not be null");
    }
  }
}
