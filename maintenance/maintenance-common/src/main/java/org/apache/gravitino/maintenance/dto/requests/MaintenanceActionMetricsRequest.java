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
package org.apache.gravitino.maintenance.dto.requests;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.rest.RESTRequest;

/**
 * Spark callback that persists snapshot metrics for an in-flight maintenance action.
 *
 * <p>Does not decide whether maintenance should continue — that stays in the Spark job. {@code
 * tableId} selects which bound table row to update when one Spark job covers multiple tables.
 */
@Getter
@EqualsAndHashCode
public class MaintenanceActionMetricsRequest implements RESTRequest {

  @JsonProperty("tableId")
  private final Long tableId;

  @JsonProperty("snapshotId")
  private final Long snapshotId;

  @JsonProperty("metricsJson")
  private final String metricsJson;

  /** Default constructor for Jackson. */
  public MaintenanceActionMetricsRequest() {
    this(null, null, null);
  }

  /**
   * Creates a metrics report.
   *
   * @param tableId table entity id bound to the job
   * @param snapshotId Iceberg snapshot id the metrics were collected for
   * @param metricsJson serialized metrics for {@code table_snapshot_metrics}
   */
  public MaintenanceActionMetricsRequest(Long tableId, Long snapshotId, String metricsJson) {
    this.tableId = tableId;
    this.snapshotId = snapshotId;
    this.metricsJson = metricsJson;
  }

  @Override
  public void validate() throws IllegalArgumentException {
    Preconditions.checkArgument(tableId != null && tableId > 0, "tableId must be a positive long");
    Preconditions.checkArgument(snapshotId != null, "snapshotId must not be null");
    Preconditions.checkArgument(
        StringUtils.isNotBlank(metricsJson), "metricsJson must not be blank");
  }
}
