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
package org.apache.gravitino.dto.semantic;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import javax.annotation.Nullable;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.gravitino.semantic.Dimension;

/** DTO for Semantic Model dimension metadata. */
@Getter
@EqualsAndHashCode
@NoArgsConstructor(access = AccessLevel.PRIVATE)
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@Builder(setterPrefix = "with")
@JsonInclude(JsonInclude.Include.NON_NULL)
public class DimensionDTO {

  @Nullable
  @JsonProperty("is_time")
  private Boolean isTime;

  /**
   * Creates a dimension DTO from an API model.
   *
   * @param dimension The API dimension.
   * @return The dimension DTO.
   */
  public static DimensionDTO fromDimension(Dimension dimension) {
    return builder().withIsTime(dimension.isTime()).build();
  }

  /**
   * Converts this DTO to API dimension metadata.
   *
   * @return The API dimension metadata.
   */
  public Dimension toDimension() {
    return Dimension.builder().withIsTime(isTime).build();
  }
}
