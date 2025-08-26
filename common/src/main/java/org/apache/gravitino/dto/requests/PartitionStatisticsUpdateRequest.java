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
import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.apache.gravitino.dto.stats.PartitionStatisticsUpdateDTO;
import org.apache.gravitino.rest.RESTRequest;

/** Represents a request to update partition statistics. */
@Getter
@EqualsAndHashCode
@ToString
public class PartitionStatisticsUpdateRequest implements RESTRequest {

  @JsonProperty("updates")
  private final List<PartitionStatisticsUpdateDTO> updates;

  /**
   * Creates a new PartitionStatsUpdateRequest.
   *
   * @param updates The updates to apply to the partition statistics.
   */
  public PartitionStatisticsUpdateRequest(List<PartitionStatisticsUpdateDTO> updates) {
    this.updates = updates;
  }

  /** Default constructor for PartitionStatsUpdateRequest. (Used for Jackson deserialization.) */
  public PartitionStatisticsUpdateRequest() {
    this(null);
  }

  /**
   * Validates the request.
   *
   * @throws IllegalArgumentException If the request is invalid, this exception is thrown.
   */
  @Override
  public void validate() throws IllegalArgumentException {
    Preconditions.checkArgument(
        updates != null && !updates.isEmpty(), "\"updates\" must not be null or empty.");
    for (PartitionStatisticsUpdateDTO update : updates) {
      update.validate();
    }
  }
}
