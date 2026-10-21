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
 * Spark callback when a cron maintenance job reaches a terminal outcome.
 *
 * <p>Lives in the TMS plugin, the same way built-in IdP request DTOs live in {@code idp-basic}.
 */
@Getter
@EqualsAndHashCode
public class MaintenanceActionFinishRequest implements RESTRequest {

  @JsonProperty("afterSnapshotId")
  @Nullable
  private final Long afterSnapshotId;

  @JsonProperty("metricsJson")
  @Nullable
  private final String metricsJson;

  /** Default constructor for Jackson. */
  public MaintenanceActionFinishRequest() {
    this(null, null);
  }

  /**
   * Creates a finish callback.
   *
   * @param afterSnapshotId Iceberg HEAD after work (or {@code S0}/HEAD when skipped); may be null
   * @param metricsJson optional S1 metrics JSON to persist before marking finished
   */
  public MaintenanceActionFinishRequest(
      @Nullable Long afterSnapshotId, @Nullable String metricsJson) {
    this.afterSnapshotId = afterSnapshotId;
    this.metricsJson = metricsJson;
  }

  @Override
  public void validate() throws IllegalArgumentException {
    Preconditions.checkArgument(
        afterSnapshotId == null || afterSnapshotId >= 0,
        "afterSnapshotId must be null or non-negative");
  }
}
