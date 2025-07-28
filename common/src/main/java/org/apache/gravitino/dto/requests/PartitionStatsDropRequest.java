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
import java.util.List;
import java.util.Map;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.apache.gravitino.rest.RESTRequest;

/** Request to drop partition statistics. */
@Getter
@EqualsAndHashCode
@ToString
public class PartitionStatsDropRequest implements RESTRequest {

  @JsonProperty("partition_statistics")
  private Map<String, List<String>> partitionStatistics;

  /**
   * Creates a new PartitionStatsDropRequest.
   *
   * @param partitionStatistics The partition statistics to drop.
   */
  public PartitionStatsDropRequest(Map<String, List<String>> partitionStatistics) {
    this.partitionStatistics = partitionStatistics;
  }

  /** This is the constructor that is used by Jackson deserializer */
  public PartitionStatsDropRequest() {
    this.partitionStatistics = null;
  }

  /**
   * Validates the request.
   *
   * @throws IllegalArgumentException If the request is invalid, this exception is thrown.
   */
  @Override
  public void validate() throws IllegalArgumentException {
    if (partitionStatistics == null || partitionStatistics.isEmpty()) {
      throw new IllegalArgumentException("Partition statistics to drop cannot be null or empty.");
    }
    partitionStatistics.forEach(
        (partition, stats) -> {
          if (stats == null || stats.isEmpty()) {
            throw new IllegalArgumentException(
                "Statistics for partition " + partition + " cannot be null or empty.");
          }
        });
  }
}
