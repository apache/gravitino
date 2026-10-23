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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.google.common.base.Preconditions;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.semantic.Metric;
import org.apache.gravitino.semantic.SemanticModelMetric;

/** A metric's containing namespace and complete definition for discovery responses. */
@EqualsAndHashCode
@ToString
@JsonPropertyOrder({"namespace", "metric"})
public class SemanticModelMetricDTO implements SemanticModelMetric {

  @JsonProperty("namespace")
  private String[] namespace;

  @JsonProperty("metric")
  private MetricDTO metric;

  private SemanticModelMetricDTO() {}

  /**
   * Creates a metric discovery DTO.
   *
   * @param namespace The namespace ending with the containing model's name.
   * @param metric The complete metric DTO.
   */
  public SemanticModelMetricDTO(Namespace namespace, MetricDTO metric) {
    Preconditions.checkArgument(namespace != null, "namespace must not be null");
    Preconditions.checkArgument(metric != null, "metric must not be null");
    this.namespace = namespace.levels();
    this.metric = metric;
  }

  @Override
  public Namespace namespace() {
    return Namespace.of(namespace);
  }

  @Override
  public Metric metric() {
    return metric.toMetric();
  }

  /**
   * Converts a decoded discovery result to its REST representation.
   *
   * @param info The metric discovery result.
   * @return The metric discovery DTO.
   */
  public static SemanticModelMetricDTO fromSemanticModelMetric(SemanticModelMetric info) {
    return new SemanticModelMetricDTO(info.namespace(), MetricDTO.fromMetric(info.metric()));
  }
}
