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
 * Spark callback after sampling Iceberg snapshot {@code S0} for a cron maintenance action.
 *
 * <p>Lives in the TMS plugin, the same way built-in IdP request DTOs live in {@code idp-basic}.
 */
@Getter
@EqualsAndHashCode
public class MaintenanceActionSampleRequest implements RESTRequest {

  @JsonProperty("tableId")
  private final Long tableId;

  @JsonProperty("policyId")
  private final Long policyId;

  @JsonProperty("sampledSnapshotId")
  private final Long sampledSnapshotId;

  @JsonProperty("headSnapshotId")
  private final Long headSnapshotId;

  @JsonProperty("metricsJson")
  private final String metricsJson;

  /** Default constructor for Jackson. */
  public MaintenanceActionSampleRequest() {
    this(null, null, null, null, null);
  }

  /**
   * Creates a sample callback.
   *
   * @param tableId table entity id
   * @param policyId policy entity id
   * @param sampledSnapshotId Iceberg snapshot id at sample time ({@code S0})
   * @param headSnapshotId HEAD snapshot id observed after the sample
   * @param metricsJson serialized metrics for {@code table_snapshot_metrics}
   */
  public MaintenanceActionSampleRequest(
      Long tableId,
      Long policyId,
      Long sampledSnapshotId,
      Long headSnapshotId,
      String metricsJson) {
    this.tableId = tableId;
    this.policyId = policyId;
    this.sampledSnapshotId = sampledSnapshotId;
    this.headSnapshotId = headSnapshotId;
    this.metricsJson = metricsJson;
  }

  @Override
  public void validate() throws IllegalArgumentException {
    Preconditions.checkArgument(tableId != null && tableId > 0, "tableId must be positive");
    Preconditions.checkArgument(policyId != null && policyId > 0, "policyId must be positive");
    Preconditions.checkArgument(sampledSnapshotId != null, "sampledSnapshotId must not be null");
    Preconditions.checkArgument(headSnapshotId != null, "headSnapshotId must not be null");
    Preconditions.checkArgument(
        StringUtils.isNotBlank(metricsJson), "metricsJson must not be blank");
  }
}
