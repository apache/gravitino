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
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.rest.RESTRequest;

/** Represents a request to drop statistics for specified names. */
@Getter
@EqualsAndHashCode
@ToString
@Builder
@Jacksonized
public class StatisticsDropRequest implements RESTRequest {
  @JsonProperty("names")
  String[] names;

  /**
   * Creates a new StatisticsDropRequest with the specified names.
   *
   * @param names The names of the statistics to drop.
   */
  public StatisticsDropRequest(String[] names) {
    this.names = names;
  }

  /** Default constructor for deserialization. */
  public StatisticsDropRequest() {
    this(null);
  }

  @Override
  public void validate() throws IllegalArgumentException {
    Preconditions.checkArgument(
        names != null && names.length > 0, "\"names\" must not be null or empty");
    for (String name : names) {
      Preconditions.checkArgument(
          StringUtils.isNotBlank(name), "Each name must be a non-empty string");
    }
  }
}
