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
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Runs an ordered chain of {@link MaintenanceTypeHandler}s inside one Spark Job.
 *
 * <p>Per type: optional S0 sample → drift check → {@link MaintenanceTypeHandler#shouldExecute} →
 * {@link MaintenanceTypeHandler#execute} → optional sampleAfter. Drift or {@link
 * MaintenanceGateDecision#SKIP_TYPE} ends that type only; the chain continues.
 */
public final class MaintenanceTypeOrchestrator {

  private static final Logger LOG = LoggerFactory.getLogger(MaintenanceTypeOrchestrator.class);

  private final List<MaintenanceTypeHandler> handlers;

  /**
   * @param handlers ordered type handlers (e.g. commit chain compaction → manifest-rewrite →
   *     snapshot-expiry)
   */
  public MaintenanceTypeOrchestrator(List<MaintenanceTypeHandler> handlers) {
    Preconditions.checkNotNull(handlers, "handlers cannot be null");
    Preconditions.checkArgument(!handlers.isEmpty(), "handlers cannot be empty");
    this.handlers = List.copyOf(handlers);
  }

  /**
   * Executes the handler chain for one table / shared job context seed.
   *
   * @param tableId table entity id
   * @param policyIdByHandlerIndex policy id per handler index (same length as handlers), or a
   *     single shared policy id repeated by the caller
   */
  public void run(long tableId, long[] policyIdByHandlerIndex) {
    Preconditions.checkNotNull(policyIdByHandlerIndex, "policyIdByHandlerIndex cannot be null");
    Preconditions.checkArgument(
        policyIdByHandlerIndex.length == handlers.size(),
        "policyIdByHandlerIndex length %s must match handlers size %s",
        policyIdByHandlerIndex.length,
        handlers.size());

    for (int i = 0; i < handlers.size(); i++) {
      MaintenanceTypeHandler handler = handlers.get(i);
      MaintenanceTypeContext context =
          new MaintenanceTypeContext(handler.policyType(), tableId, policyIdByHandlerIndex[i]);
      runOne(handler, context);
    }
  }

  private void runOne(MaintenanceTypeHandler handler, MaintenanceTypeContext context) {
    LOG.info(
        "Starting maintenance type {} for tableId={} policyId={}",
        handler.policyType(),
        context.tableId(),
        context.policyId());

    handler.sampleBefore(context);
    if (handler.supportsMetricsSample() && context.snapshotDrifted()) {
      LOG.info(
          "Skipping maintenance type {} due to snapshot drift after S0 (tableId={})",
          handler.policyType(),
          context.tableId());
      return;
    }

    MaintenanceGateDecision decision = handler.shouldExecute(context);
    if (decision == MaintenanceGateDecision.SKIP_TYPE) {
      LOG.info(
          "Skipping maintenance type {} (gate=SKIP_TYPE, tableId={})",
          handler.policyType(),
          context.tableId());
      return;
    }

    Preconditions.checkState(
        decision == MaintenanceGateDecision.EXECUTE, "Unexpected gate decision %s", decision);
    handler.execute(context);
    handler.sampleAfter(context);

    LOG.info(
        "Finished maintenance type {} for tableId={} policyId={}",
        handler.policyType(),
        context.tableId(),
        context.policyId());
  }
}
