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
package org.apache.gravitino.maintenance.dto.responses;

import com.fasterxml.jackson.annotation.JsonProperty;
import javax.annotation.Nullable;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.apache.gravitino.dto.responses.BaseResponse;

/**
 * Gate outcome after TMS persists {@code S0} metrics and {@code before_snapshot_id}.
 *
 * <p>Lives in the TMS plugin, the same way built-in IdP responses live in {@code idp-basic}.
 */
@Getter
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public class MaintenanceActionSampleResponse extends BaseResponse {

  @JsonProperty("continueMaintenance")
  private final boolean continueMaintenance;

  @JsonProperty("stopReason")
  @Nullable
  private final String stopReason;

  @JsonProperty("sampledSnapshotId")
  private final long sampledSnapshotId;

  @JsonProperty("headSnapshotId")
  private final long headSnapshotId;

  /** Default constructor for Jackson. */
  public MaintenanceActionSampleResponse() {
    this(false, null, 0L, 0L);
  }

  /**
   * Creates a sample gate response.
   *
   * @param continueMaintenance whether the Spark job should run maintenance work
   * @param stopReason {@code SNAPSHOT_DRIFT} or {@code RECOMMENDER_SKIP} when stopping
   * @param sampledSnapshotId {@code S0}
   * @param headSnapshotId HEAD observed for the gate
   */
  public MaintenanceActionSampleResponse(
      boolean continueMaintenance,
      @Nullable String stopReason,
      long sampledSnapshotId,
      long headSnapshotId) {
    super(0);
    this.continueMaintenance = continueMaintenance;
    this.stopReason = stopReason;
    this.sampledSnapshotId = sampledSnapshotId;
    this.headSnapshotId = headSnapshotId;
  }
}
