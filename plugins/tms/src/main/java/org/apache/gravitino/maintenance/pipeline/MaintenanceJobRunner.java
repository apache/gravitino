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

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Spark Job entry that runs a {@link MaintenanceTypeOrchestrator} for one or more tables.
 *
 * <p>Type chaining stays single-table ({@link MaintenanceTypeOrchestrator#run}). Multi-table jobs
 * (for example batched snapshot-expiry under {@code batch:{batch_id}:{policy_id}}) loop tables
 * here: each table runs sample → gate → execute (then the next table), inside the same Spark Job.
 * Per-table skip, drift, or failure does not abort later tables.
 */
public final class MaintenanceJobRunner {

  private static final Logger LOG = LoggerFactory.getLogger(MaintenanceJobRunner.class);

  private final MaintenanceTypeOrchestrator orchestrator;

  /**
   * @param orchestrator single-table type-chain runner
   */
  public MaintenanceJobRunner(MaintenanceTypeOrchestrator orchestrator) {
    this.orchestrator = Preconditions.checkNotNull(orchestrator, "orchestrator cannot be null");
  }

  /**
   * Runs the type chain for each table in order inside one Spark Job.
   *
   * @param tableIds tables to process (length ≥ 1); typically one table for commit-chain / cron
   *     compaction, or many for batched snapshot-expiry
   * @param policyIdByHandlerIndex policy id per handler index (same contract as {@link
   *     MaintenanceTypeOrchestrator#run})
   */
  public void runForTables(long[] tableIds, long[] policyIdByHandlerIndex) {
    Preconditions.checkNotNull(tableIds, "tableIds cannot be null");
    Preconditions.checkArgument(tableIds.length > 0, "tableIds cannot be empty");
    Preconditions.checkNotNull(policyIdByHandlerIndex, "policyIdByHandlerIndex cannot be null");

    for (long tableId : tableIds) {
      LOG.info(
          "Starting maintenance job slice for tableId={} ({} table(s) in this Spark Job)",
          tableId,
          tableIds.length);
      try {
        orchestrator.run(tableId, policyIdByHandlerIndex);
      } catch (RuntimeException e) {
        LOG.warn(
            "Maintenance slice failed for tableId={}, continuing with remaining tables",
            tableId,
            e);
      }
    }
  }
}
