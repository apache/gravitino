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
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.jackson.Jacksonized;
import org.apache.gravitino.dto.rel.partitions.PartitionDTO;
import org.apache.gravitino.rest.RESTRequest;

/** Request to add partitions to a table. */
@Getter
@EqualsAndHashCode
@ToString
@Builder
@Jacksonized
public class AddPartitionsRequest implements RESTRequest {

  @JsonProperty("partitions")
  private final PartitionDTO[] partitions;

  /** Default constructor for Jackson. */
  public AddPartitionsRequest() {
    this(null);
  }

  /**
   * Constructor for the request.
   *
   * @param partitions The partitions to add.
   */
  public AddPartitionsRequest(PartitionDTO[] partitions) {
    this.partitions = partitions;
  }

  @Override
  public void validate() throws IllegalArgumentException {
    Preconditions.checkArgument(partitions != null, "partitions must not be null");
    Preconditions.checkArgument(
        partitions.length == 1, "Haven't yet implemented multiple partitions");
  }
}
