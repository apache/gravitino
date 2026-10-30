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
import lombok.ToString;
import org.apache.gravitino.dto.semantic.SemanticModelMetricDTO;

/** Response containing complete metrics discovered in a schema. */
@EqualsAndHashCode(callSuper = true)
@ToString
public class MetricInfoListResponse extends BaseResponse {

  @JsonProperty("metrics")
  private SemanticModelMetricDTO[] metrics;

  private MetricInfoListResponse() {
    super();
  }

  /**
   * Creates a response containing metric definitions and their namespaces.
   *
   * @param metrics The metric discovery results.
   */
  public MetricInfoListResponse(SemanticModelMetricDTO[] metrics) {
    super(0);
    this.metrics = metrics == null ? null : metrics.clone();
  }

  /**
   * Returns the metric discovery results.
   *
   * @return A copy of the result array.
   */
  public SemanticModelMetricDTO[] getMetrics() {
    return metrics == null ? null : metrics.clone();
  }

  @Override
  public void validate() throws IllegalArgumentException {
    super.validate();
    Preconditions.checkArgument(metrics != null, "metrics must not be null");
    for (SemanticModelMetricDTO metric : metrics) {
      Preconditions.checkArgument(metric != null, "metrics must not contain null");
    }
  }
}
