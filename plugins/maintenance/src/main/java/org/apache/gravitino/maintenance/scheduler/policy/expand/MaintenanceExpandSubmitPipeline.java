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
package org.apache.gravitino.maintenance.scheduler.policy.expand;

import com.google.common.base.Preconditions;
import java.time.Instant;
import java.util.Optional;

/**
 * Expand-pool pick handler (§5.2.1), parallel to {@code MaintenanceCronSubmitPipeline} / {@code
 * MaintenanceCommitSubmitPipeline}: consume a due {@code policy-expand} lease and return the next
 * expand due time (or empty to delete the lease).
 *
 * <p>Planning (policy gate, table enumeration, cron-job schedule) is {@link PolicyExpandPlanner}.
 * Does not submit Spark.
 */
public final class MaintenanceExpandSubmitPipeline {

  private final PolicyExpandPlanner planner;

  /**
   * @param planner expand pick planner
   */
  public MaintenanceExpandSubmitPipeline(PolicyExpandPlanner planner) {
    this.planner = Preconditions.checkNotNull(planner);
  }

  /**
   * Runs one expand pick for {@code policyId}.
   *
   * @param policyId expand task instance id
   * @return next due time when the expand row should stay, or empty to delete the row
   */
  public Optional<Instant> expand(long policyId) {
    return planner.plan(policyId);
  }
}
