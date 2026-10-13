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
package org.apache.gravitino.maintenance.pipeline;

import javax.annotation.Nullable;
import org.apache.gravitino.policy.Policy;

/**
 * Per-type runtime state for one step of the in-job maintenance pipeline.
 *
 * <p>Handlers update sample / drift fields during {@code sampleBefore}; the orchestrator reads them
 * to decide whether to continue.
 */
public final class MaintenanceTypeContext {

  private final Policy.BuiltInType policyType;
  private final long tableId;
  private final long policyId;

  @Nullable private Long beforeSnapshotId;
  @Nullable private Long currentSnapshotId;
  private boolean snapshotDrifted;

  /**
   * @param policyType built-in maintenance policy type
   * @param tableId table entity id
   * @param policyId policy entity id
   */
  public MaintenanceTypeContext(Policy.BuiltInType policyType, long tableId, long policyId) {
    this.policyType = policyType;
    this.tableId = tableId;
    this.policyId = policyId;
  }

  /**
   * @return built-in policy type for this step
   */
  public Policy.BuiltInType policyType() {
    return policyType;
  }

  /**
   * @return table entity id
   */
  public long tableId() {
    return tableId;
  }

  /**
   * @return policy entity id
   */
  public long policyId() {
    return policyId;
  }

  /**
   * @return Iceberg snapshot id sampled at S0, or {@code null} when unset / not sampled
   */
  @Nullable
  public Long beforeSnapshotId() {
    return beforeSnapshotId;
  }

  /**
   * @param beforeSnapshotId sampled S0 snapshot id
   */
  public void setBeforeSnapshotId(@Nullable Long beforeSnapshotId) {
    this.beforeSnapshotId = beforeSnapshotId;
  }

  /**
   * @return HEAD snapshot id observed for drift check, or {@code null}
   */
  @Nullable
  public Long currentSnapshotId() {
    return currentSnapshotId;
  }

  /**
   * @param currentSnapshotId HEAD snapshot id
   */
  public void setCurrentSnapshotId(@Nullable Long currentSnapshotId) {
    this.currentSnapshotId = currentSnapshotId;
  }

  /**
   * @return whether HEAD moved after S0
   */
  public boolean snapshotDrifted() {
    return snapshotDrifted;
  }

  /**
   * @param snapshotDrifted drift flag
   */
  public void setSnapshotDrifted(boolean snapshotDrifted) {
    this.snapshotDrifted = snapshotDrifted;
  }
}
