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
import javax.annotation.Nullable;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.apache.gravitino.rest.RESTRequest;

/**
 * Closes the soft slot for one table/policy bound to a maintenance job without writing snapshot
 * metrics.
 *
 * <p>Use when a maintenance type does not sample metrics, or when metrics were already reported and
 * only occupancy release remains.
 */
@Getter
@EqualsAndHashCode
public class MaintenanceActionFinishRequest implements RESTRequest {

  @JsonProperty("tableId")
  private final Long tableId;

  @JsonProperty("policyId")
  private final Long policyId;

  /**
   * Optional Iceberg snapshot id to store as {@code after_snapshot_id}. Omit or null when releasing
   * without an S1 (for example skipped work with no rewrite).
   */
  @Nullable
  @JsonProperty("afterSnapshotId")
  private final Long afterSnapshotId;

  /** Default constructor for Jackson. */
  public MaintenanceActionFinishRequest() {
    this(null, null, null);
  }

  /**
   * Creates a finish request that leaves {@code after_snapshot_id} null.
   *
   * @param tableId table entity id bound to the job
   * @param policyId policy entity id bound to the job
   */
  public MaintenanceActionFinishRequest(Long tableId, Long policyId) {
    this(tableId, policyId, null);
  }

  /**
   * Creates a finish request.
   *
   * @param tableId table entity id bound to the job
   * @param policyId policy entity id bound to the job
   * @param afterSnapshotId terminal snapshot id, or null
   */
  public MaintenanceActionFinishRequest(
      Long tableId, Long policyId, @Nullable Long afterSnapshotId) {
    this.tableId = tableId;
    this.policyId = policyId;
    this.afterSnapshotId = afterSnapshotId;
  }

  @Override
  public void validate() throws IllegalArgumentException {
    Preconditions.checkArgument(tableId != null && tableId > 0, "tableId must be a positive long");
    Preconditions.checkArgument(
        policyId != null && policyId > 0, "policyId must be a positive long");
  }
}
