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
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.util.Map;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.rest.RESTRequest;
import org.apache.gravitino.stats.StatisticValue;

/** Represents a request to update partition statistics. */
@Getter
@EqualsAndHashCode
@ToString
public class PartitionStatsUpdateRequest implements RESTRequest {

  @JsonProperty("updates")
  @JsonSerialize(contentUsing = JsonUtils.NestedMapSerializer.class)
  @JsonDeserialize(contentUsing = JsonUtils.NestedMapDeserializer.class)
  private final Map<String, Map<String, StatisticValue<?>>> updates;

  /**
   * Creates a new PartitionStatsUpdateRequest.
   *
   * @param updates The updates to apply to the partition statistics.
   */
  public PartitionStatsUpdateRequest(Map<String, Map<String, StatisticValue<?>>> updates) {
    this.updates = updates;
  }

  /** Default constructor for PartitionStatsUpdateRequest. (Used for Jackson deserialization.) */
  public PartitionStatsUpdateRequest() {
    this.updates = null;
  }

  /**
   * Validates the request.
   *
   * @throws IllegalArgumentException If the request is invalid, this exception is thrown.
   */
  @Override
  public void validate() throws IllegalArgumentException {
    if (updates == null || updates.isEmpty()) {
      throw new IllegalArgumentException("Updates map cannot be null or empty");
    }
    updates.forEach(
        (partition, stats) -> {
          if (stats == null || stats.isEmpty()) {
            throw new IllegalArgumentException(
                "Statistics for partition " + partition + " cannot be null or empty");
          }
        });
  }
}
