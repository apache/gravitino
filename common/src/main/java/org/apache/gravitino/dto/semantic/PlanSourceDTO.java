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
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.jackson.Jacksonized;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.semantic.PlanSource;

/** Represents a dataset required by a Semantic Model query plan and its physical source. */
@Getter
@EqualsAndHashCode
@ToString
@Builder
@Jacksonized
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"dataset", "source"})
public class PlanSourceDTO {

  @JsonProperty("dataset")
  private final String dataset;

  @JsonProperty("source")
  private final List<String> source;

  /** Default constructor for Jackson deserialization. */
  public PlanSourceDTO() {
    this(null, null);
  }

  /**
   * Creates a plan source.
   *
   * @param dataset The dataset name declared by the Semantic Model.
   * @param source The physical source identifier levels.
   */
  public PlanSourceDTO(String dataset, List<String> source) {
    this.dataset = dataset;
    this.source = source == null ? null : new ArrayList<>(source);
  }

  /**
   * Validates the source.
   *
   * @throws IllegalArgumentException If the dataset or source identifier is missing.
   */
  public void validate() {
    Preconditions.checkArgument(
        StringUtils.isNotBlank(dataset), "\"dataset\" field is required and cannot be empty");
    Preconditions.checkArgument(
        source != null && !source.isEmpty(), "\"source\" field is required and cannot be empty");
    for (int index = 0; index < source.size(); index++) {
      Preconditions.checkArgument(
          StringUtils.isNotBlank(source.get(index)),
          "\"source\"[%s] field cannot be null or empty",
          index);
    }
  }

  /**
   * Converts this DTO to an API plan source.
   *
   * @return The plan source.
   * @throws IllegalArgumentException If the DTO is invalid.
   */
  public PlanSource toPlanSource() {
    validate();
    return PlanSource.of(dataset, NameIdentifier.of(source.toArray(new String[0])));
  }

  /**
   * Creates a DTO from an API plan source.
   *
   * @param source The API plan source.
   * @return The plan source DTO.
   */
  public static PlanSourceDTO fromPlanSource(PlanSource source) {
    NameIdentifier identifier = source.source();
    List<String> levels = new ArrayList<>();
    for (String level : identifier.namespace().levels()) {
      levels.add(level);
    }
    levels.add(identifier.name());
    return new PlanSourceDTO(source.dataset(), levels);
  }
}
