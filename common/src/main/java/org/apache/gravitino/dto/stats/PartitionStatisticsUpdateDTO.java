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
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.base.Preconditions;
import java.util.Map;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.stats.PartitionStatisticsUpdate;
import org.apache.gravitino.stats.StatisticValue;

/**
 * PartitionStatisticsUpdateDTO is a Data Transfer Object (DTO) that represents the request to
 * update statistics for a specific partition in a data source.
 */
@EqualsAndHashCode
@ToString
public class PartitionStatisticsUpdateDTO implements PartitionStatisticsUpdate {

  @JsonProperty("partitionName")
  private final String partitionName;

  @JsonProperty("statistics")
  @JsonSerialize(contentUsing = JsonUtils.StatisticValueSerializer.class)
  @JsonDeserialize(contentUsing = JsonUtils.StatisticValueDeserializer.class)
  private final Map<String, StatisticValue<?>> statistics;

  /** Default constructor for Jackson. */
  protected PartitionStatisticsUpdateDTO() {
    this(null, null);
  }

  private PartitionStatisticsUpdateDTO(
      String partitionName, Map<String, StatisticValue<?>> statistics) {
    this.partitionName = partitionName;
    this.statistics = statistics;
  }

  @Override
  public String partitionName() {
    return partitionName;
  }

  @Override
  public Map<String, StatisticValue<?>> statistics() {
    return statistics;
  }

  /** Validates the PartitionStatisticsUpdateDTO instance. */
  public void validate() {
    Preconditions.checkArgument(
        StringUtils.isNotBlank(partitionName), "\"partitionName\" must not be null or empty");
    Preconditions.checkArgument(
        statistics != null && !statistics.isEmpty(), "\"statistics\" must not be null or empty");
  }

  /**
   * Creates a new builder for PartitionStatisticsUpdateDTO.
   *
   * @return a new Builder instance
   */
  public static Builder builder() {
    return new Builder();
  }

  /** Builder class for PartitionStatisticsUpdateDTO. */
  public static class Builder {
    private String partitionName;
    private Map<String, StatisticValue<?>> statistics;

    /**
     * Sets the partition name for the update.
     *
     * @param partitionName the name of the partition to update statistics for
     * @return this Builder instance for method chaining
     */
    public Builder withPartitionName(String partitionName) {
      this.partitionName = partitionName;
      return this;
    }

    /**
     * Sets the statistics to be updated for the partition.
     *
     * @param statistics a map of statistic names to their corresponding values to be updated
     * @return this Builder instance for method chaining
     */
    public Builder withStatistics(Map<String, StatisticValue<?>> statistics) {
      this.statistics = statistics;
      return this;
    }

    /**
     * Builds a PartitionStatisticsUpdateDTO instance with the provided partition name and
     * statistics.
     *
     * @return a new PartitionStatisticsUpdateDTO instance
     */
    public PartitionStatisticsUpdateDTO build() {
      PartitionStatisticsUpdateDTO dto =
          new PartitionStatisticsUpdateDTO(partitionName, statistics);
      dto.validate();
      return dto;
    }
  }
}
