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
package org.apache.gravitino.maintenance.scheduler.job.commit;

import org.apache.gravitino.maintenance.occupancy.OccupancyGate;

/**
 * Occupancy checks for commit-job paths.
 *
 * <p>{@link #withinMinInterval} gates enqueue (drops cooled-down chain members before spec build).
 * {@link #isInFlight} gates pick (any chain type still in-flight aborts submit).
 */
public interface CommitOccupancyGate {

  /**
   * @param tableId table entity id
   * @param policyId policy entity id
   * @return true when that policy still holds a soft slot
   */
  boolean isInFlight(long tableId, long policyId);

  /**
   * @param tableId table entity id
   * @param policyId policy entity id
   * @param minIntervalMs resolved cooldown
   * @param nowMs current epoch millis
   * @return true when still inside {@code minIntervalMs} after the last finish
   */
  boolean withinMinInterval(long tableId, long policyId, long minIntervalMs, long nowMs);

  /**
   * @param gate production occupancy gate
   * @return adapter
   */
  static CommitOccupancyGate of(OccupancyGate gate) {
    return new CommitOccupancyGate() {
      @Override
      public boolean isInFlight(long tableId, long policyId) {
        return gate.isInFlight(tableId, policyId);
      }

      @Override
      public boolean withinMinInterval(
          long tableId, long policyId, long minIntervalMs, long nowMs) {
        return gate.withinMinInterval(tableId, policyId, minIntervalMs, nowMs);
      }
    };
  }
}
