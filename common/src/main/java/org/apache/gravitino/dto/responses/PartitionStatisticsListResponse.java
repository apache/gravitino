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
import org.apache.gravitino.dto.stats.PartitionStatisticsDTO;

/**
 * PartitionStatisticsListResponse is a response object that contains an array of
 * PartitionStatisticsDTO.
 */
@Getter
@ToString
@EqualsAndHashCode(callSuper = true)
public class PartitionStatisticsListResponse extends BaseResponse {

  @JsonProperty("partitionStatistics")
  private PartitionStatisticsDTO[] partitionStatistics;

  /**
   * Creates a new PartitionStatsListResponse.
   *
   * @param partitionStatistics The updated statistics for the partition.
   */
  public PartitionStatisticsListResponse(PartitionStatisticsDTO[] partitionStatistics) {
    super(0);
    this.partitionStatistics = partitionStatistics;
  }

  /** This is the constructor that is used by Jackson deserializer */
  public PartitionStatisticsListResponse() {
    this(null);
  }

  /**
   * Validates the response.
   *
   * @throws IllegalArgumentException If the response is invalid, this exception is thrown.
   */
  @Override
  public void validate() throws IllegalArgumentException {
    super.validate();
    Preconditions.checkArgument(
        partitionStatistics != null, "\"partitionStatistics\" must not be null");
    for (PartitionStatisticsDTO partitionStat : partitionStatistics) {
      Preconditions.checkArgument(
          partitionStat != null, "Each partition statistic must not be null");
      partitionStat.validate();
    }
  }
}
