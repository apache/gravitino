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
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.stats.PartitionStatistics;
import org.apache.gravitino.stats.Statistic;

/**
 * PartitionStatisticsDTO is a Data Transfer Object (DTO) that represents the statistics for a
 * specific partition in a data source.
 */
@EqualsAndHashCode
@ToString
public class PartitionStatisticsDTO implements PartitionStatistics {

  @JsonProperty("name")
  private String name;

  @JsonProperty("statistics")
  private StatisticDTO[] statistics;

  /** Default constructor for Jackson. */
  protected PartitionStatisticsDTO() {
    this(null, null);
  }

  private PartitionStatisticsDTO(String name, StatisticDTO[] statistics) {
    this.name = name;
    this.statistics = statistics;
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public Statistic[] statistics() {
    return statistics;
  }

  /** Validates the PartitionStatisticsDTO instance. */
  public void validate() {
    Preconditions.checkArgument(StringUtils.isNotBlank(name), "\"name\" must not be null or empty");
    Preconditions.checkArgument(statistics != null, "\"statistics\" must not be null");
    for (StatisticDTO statistic : statistics) {
      statistic.validate();
    }
  }

  /**
   * Creates a new builder for PartitionStatisticsDTO.
   *
   * @return a new Builder instance
   */
  public static Builder builder() {
    return new Builder();
  }

  /** Builder for PartitionStatisticsDTO. */
  public static class Builder {
    private String name;
    private StatisticDTO[] statistics;

    /**
     * Sets the name of the partition for which these statistics are applicable.
     *
     * @param name the name of the partition
     * @return the Builder instance for method chaining
     */
    public Builder withName(String name) {
      this.name = name;
      return this;
    }

    /**
     * Sets the statistics for the partition.
     *
     * @param statistics the list of statistics applicable to the partition
     * @return the Builder instance for method chaining
     */
    public Builder withStatistics(StatisticDTO[] statistics) {
      this.statistics = statistics;
      return this;
    }

    /**
     * Builds a PartitionStatisticsDTO instance.
     *
     * @return a new PartitionStatisticsDTO instance
     */
    public PartitionStatisticsDTO build() {
      PartitionStatisticsDTO dto = new PartitionStatisticsDTO(name, statistics);
      dto.validate();
      return dto;
    }
  }
}
