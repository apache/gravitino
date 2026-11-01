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
import org.apache.gravitino.dto.semantic.SemanticQueryPlanDTO;
import org.apache.gravitino.semantic.SemanticQueryPlan;

/** Represents the dialect-neutral logical plan resolved from a Semantic Model query. */
@Getter
@EqualsAndHashCode(callSuper = true)
@ToString
public class SemanticQueryPlanResponse extends BaseResponse {

  @JsonProperty("plan")
  private final SemanticQueryPlanDTO plan;

  /** Default constructor for Jackson deserialization. */
  public SemanticQueryPlanResponse() {
    super();
    this.plan = null;
  }

  /**
   * Creates a successful query plan response.
   *
   * @param plan The resolved plan.
   */
  public SemanticQueryPlanResponse(SemanticQueryPlan plan) {
    super(0);
    this.plan = SemanticQueryPlanDTO.fromPlan(plan);
  }

  @Override
  public void validate() throws IllegalArgumentException {
    super.validate();
    Preconditions.checkArgument(plan != null, "plan must not be null");
    plan.validate();
  }

  /**
   * Converts this response to an API plan.
   *
   * @return The resolved plan.
   * @throws IllegalArgumentException If the response is invalid.
   */
  public SemanticQueryPlan toPlan() {
    validate();
    return plan.toPlan();
  }
}
