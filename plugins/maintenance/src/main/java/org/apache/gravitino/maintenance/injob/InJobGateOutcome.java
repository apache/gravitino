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
package org.apache.gravitino.maintenance.injob;

import com.google.common.base.Preconditions;
import javax.annotation.Nullable;

/** Result of the in-job sample / drift / recommend gates (§5.2.2). */
public final class InJobGateOutcome {

  /** Why maintenance should not continue. */
  public enum StopReason {
    /** HEAD snapshot id differs from {@code S0}; do not re-sample in a loop. */
    SNAPSHOT_DRIFT,
    /** Recommender decided not to maintain; end early without running work. */
    RECOMMENDER_SKIP
  }

  private final boolean continueMaintenance;
  @Nullable private final StopReason stopReason;
  private final long sampledSnapshotId;
  private final long headSnapshotId;

  private InJobGateOutcome(
      boolean continueMaintenance,
      @Nullable StopReason stopReason,
      long sampledSnapshotId,
      long headSnapshotId) {
    this.continueMaintenance = continueMaintenance;
    this.stopReason = stopReason;
    this.sampledSnapshotId = sampledSnapshotId;
    this.headSnapshotId = headSnapshotId;
  }

  /**
   * @param sampledSnapshotId {@code S0}
   * @return outcome when maintenance may proceed
   */
  public static InJobGateOutcome continueMaintenance(long sampledSnapshotId) {
    return new InJobGateOutcome(true, null, sampledSnapshotId, sampledSnapshotId);
  }

  /**
   * @param reason why the job or type should stop
   * @param sampledSnapshotId {@code S0}
   * @param headSnapshotId current HEAD when stopping
   * @return outcome when maintenance must not continue
   */
  public static InJobGateOutcome stop(
      StopReason reason, long sampledSnapshotId, long headSnapshotId) {
    Preconditions.checkNotNull(reason);
    return new InJobGateOutcome(false, reason, sampledSnapshotId, headSnapshotId);
  }

  /**
   * @return whether the Spark Job should run work for this type
   */
  public boolean continueMaintenance() {
    return continueMaintenance;
  }

  /**
   * @return stop reason when {@link #continueMaintenance()} is false
   */
  @Nullable
  public StopReason stopReason() {
    return stopReason;
  }

  /**
   * @return sampled snapshot id ({@code S0})
   */
  public long sampledSnapshotId() {
    return sampledSnapshotId;
  }

  /**
   * @return HEAD snapshot id observed for the gate
   */
  public long headSnapshotId() {
    return headSnapshotId;
  }
}
