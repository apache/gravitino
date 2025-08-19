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
package org.apache.gravitino.dto.stats;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.stats.PartitionStatisticsDrop;

/**
 * PartitionStatisticsDropDTO is a Data Transfer Object (DTO) that represents the request to drop
 * statistics for a specific partition in a data source.
 */
@EqualsAndHashCode
@ToString
public class PartitionStatisticsDropDTO implements PartitionStatisticsDrop {

  @JsonProperty("partitionName")
  private final String partitionName;

  @JsonProperty("statisticNames")
  private final List<String> statisticNames;

  /** Default constructor for Jackson. */
  protected PartitionStatisticsDropDTO() {
    this(null, null);
  }

  private PartitionStatisticsDropDTO(String partitionName, List<String> statisticNames) {
    this.partitionName = partitionName;
    this.statisticNames = statisticNames;
  }

  @Override
  public String partitionName() {
    return partitionName;
  }

  @Override
  public List<String> statisticNames() {
    return statisticNames;
  }

  /** Validates the PartitionStatisticsDropDTO instance. */
  public void validate() {
    Preconditions.checkArgument(
        StringUtils.isNotBlank(partitionName), "\"partitionName\" must not be null or empty");
    Preconditions.checkArgument(
        statisticNames != null && !statisticNames.isEmpty(),
        "\"statisticNames\" must not be null or empty");
    for (String statisticName : statisticNames) {
      Preconditions.checkArgument(
          StringUtils.isNotBlank(statisticName),
          "Each statistic \"name\" in \"statisticNames\" must not be null or empty");
    }
  }

  /**
   * Creates a new instance of PartitionStatisticsDropDTO.
   *
   * @param partitionName the name of the partition for which these statistics are applicable
   * @param statisticNames the names of the statistics to drop for the partition
   * @return a new instance of PartitionStatisticsDropDTO
   */
  public static PartitionStatisticsDropDTO of(String partitionName, List<String> statisticNames) {
    PartitionStatisticsDropDTO dto = new PartitionStatisticsDropDTO(partitionName, statisticNames);
    dto.validate();
    return dto;
  }
}
