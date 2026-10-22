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
package org.apache.gravitino.storage.relational.po;

import com.google.common.base.Preconditions;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import lombok.experimental.Accessors;
import org.apache.commons.lang3.StringUtils;

/** The persistent identity and lifecycle state of a metric contained in a Semantic Model. */
@EqualsAndHashCode
@Getter
@ToString
@Accessors(fluent = true)
public class SemanticModelMetricPO {
  private Long metricId;
  private Long metalakeId;
  private Long catalogId;
  private Long schemaId;
  private Long semanticModelId;
  private String metricName;
  private Long deletedAt;

  /** Creates an empty persistent object for MyBatis. */
  public SemanticModelMetricPO() {}

  @lombok.Builder(setterPrefix = "with")
  private SemanticModelMetricPO(
      Long metricId,
      Long metalakeId,
      Long catalogId,
      Long schemaId,
      Long semanticModelId,
      String metricName,
      Long deletedAt) {
    Preconditions.checkArgument(metricId != null, "Metric id is required");
    Preconditions.checkArgument(metalakeId != null, "Metalake id is required");
    Preconditions.checkArgument(catalogId != null, "Catalog id is required");
    Preconditions.checkArgument(schemaId != null, "Schema id is required");
    Preconditions.checkArgument(semanticModelId != null, "Semantic Model id is required");
    Preconditions.checkArgument(StringUtils.isNotBlank(metricName), "Metric name cannot be empty");
    Preconditions.checkArgument(deletedAt != null, "Deleted at is required");
    this.metricId = metricId;
    this.metalakeId = metalakeId;
    this.catalogId = catalogId;
    this.schemaId = schemaId;
    this.semanticModelId = semanticModelId;
    this.metricName = metricName;
    this.deletedAt = deletedAt;
  }
}
