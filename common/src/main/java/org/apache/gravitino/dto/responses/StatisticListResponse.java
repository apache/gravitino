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
import org.apache.gravitino.dto.stats.StatisticDTO;

/** Represents a response containing a list of statistics. */
@Getter
@ToString
@EqualsAndHashCode(callSuper = true)
public class StatisticListResponse extends BaseResponse {

  @JsonProperty("statistics")
<<<<<<< HEAD
  private StatisticDTO[] statistics;

  /**
   * Constructor for StatisticsListResponse.
   *
   * @param statistics Array of StatisticDTO objects representing the statistics.
=======
  private final StatisticDTO[] statistics;

  /**
   * Constructor for StatisticListResponse.
   *
   * @param statistics The array of statistics.
>>>>>>> dc6f26d92 ([#7274] feat(client-java): Support statistics http client config for gravitino client)
   */
  public StatisticListResponse(StatisticDTO[] statistics) {
    super(0);
    this.statistics = statistics;
  }

<<<<<<< HEAD
  /** Default constructor for StatisticsListResponse (used by Jackson deserializer). */
  public StatisticListResponse() {
    this(null);
  }

  @Override
  public void validate() throws IllegalArgumentException {
    Preconditions.checkArgument(statistics != null, "\"statistics\" must not be null");

    for (StatisticDTO statistic : statistics) {
      Preconditions.checkArgument(statistic != null, "\"statistic\" must not be null");
      statistic.validate();
    }
=======
  /** Default constructor for StatisticListResponse (used by Jackson deserializer). */
  public StatisticListResponse() {
    super();
    this.statistics = null;
  }

  /**
   * Validates the response data.
   *
   * @throws IllegalArgumentException If the response data is invalid.
   */
  @Override
  public void validate() throws IllegalArgumentException {
    super.validate();
    Preconditions.checkArgument(statistics != null, "statistics field is required");
  }

  /**
   * Creates a new StatisticListResponse with the given statistics.
   *
   * @param statistics The statistics to include in the response.
   * @return A new StatisticListResponse.
   */
  public static StatisticListResponse ok(StatisticDTO... statistics) {
    return new StatisticListResponse(statistics);
>>>>>>> dc6f26d92 ([#7274] feat(client-java): Support statistics http client config for gravitino client)
  }
}
