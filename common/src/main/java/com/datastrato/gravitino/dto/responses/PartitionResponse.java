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
package com.datastrato.gravitino.dto.responses;

import com.datastrato.gravitino.dto.rel.partitions.PartitionDTO;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/** Represents a response for a partition. */
@Getter
@ToString
@EqualsAndHashCode(callSuper = true)
public class PartitionResponse extends BaseResponse {

  @JsonProperty("partition")
  private final PartitionDTO partition;

  /**
   * Creates a new PartitionResponse.
   *
   * @param partition The partition.
   */
  public PartitionResponse(PartitionDTO partition) {
    super(0);
    this.partition = partition;
  }

  /** This is the constructor that is used by Jackson deserializer */
  public PartitionResponse() {
    super();
    this.partition = null;
  }
}
