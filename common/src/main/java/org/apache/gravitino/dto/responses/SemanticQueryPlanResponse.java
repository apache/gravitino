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
import java.util.Arrays;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.semantic.SemanticQueryPlan;

/** Represents the SQL compiled from a Semantic Model query. */
@Getter
@EqualsAndHashCode(callSuper = true)
@ToString
public class SemanticQueryPlanResponse extends BaseResponse {

  @JsonProperty("sql")
  private final String sql;

  @JsonProperty("dialect")
  private final String dialect;

  @JsonProperty("planner")
  private final String planner;

  @JsonProperty("warnings")
  private final String[] warnings;

  /** Default constructor for Jackson deserialization. */
  public SemanticQueryPlanResponse() {
    super();
    this.sql = null;
    this.dialect = null;
    this.planner = null;
    this.warnings = null;
  }

  /**
   * Creates a successful query plan response.
   *
   * @param plan The generated plan.
   */
  public SemanticQueryPlanResponse(SemanticQueryPlan plan) {
    super(0);
    this.sql = plan.sql();
    this.dialect = plan.dialect();
    this.planner = plan.planner();
    this.warnings = plan.warnings().toArray(new String[0]);
  }

  @Override
  public void validate() throws IllegalArgumentException {
    super.validate();
    Preconditions.checkArgument(StringUtils.isNotBlank(sql), "sql must not be null or empty");
    Preconditions.checkArgument(
        StringUtils.isNotBlank(dialect), "dialect must not be null or empty");
    Preconditions.checkArgument(
        StringUtils.isNotBlank(planner), "planner must not be null or empty");
    Preconditions.checkArgument(warnings != null, "warnings must not be null");
    for (int index = 0; index < warnings.length; index++) {
      Preconditions.checkArgument(
          StringUtils.isNotBlank(warnings[index]), "warnings[%s] must not be null or empty", index);
    }
  }

  /**
   * Converts this response to an API plan.
   *
   * @return The generated plan.
   * @throws IllegalArgumentException If the response is invalid.
   */
  public SemanticQueryPlan toPlan() {
    validate();
    return SemanticQueryPlan.of(sql, dialect, planner, Arrays.asList(warnings));
  }
}
