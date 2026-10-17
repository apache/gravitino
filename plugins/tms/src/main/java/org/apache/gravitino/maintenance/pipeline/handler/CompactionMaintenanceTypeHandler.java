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
package org.apache.gravitino.maintenance.pipeline.handler;

import org.apache.gravitino.maintenance.pipeline.AbstractMaintenanceTypeHandler;
import org.apache.gravitino.maintenance.pipeline.MaintenanceGateDecision;
import org.apache.gravitino.maintenance.pipeline.MaintenanceTypeContext;
import org.apache.gravitino.policy.Policy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * In-job handler for {@link Policy.BuiltInType#ICEBERG_COMPACTION}.
 *
 * <p>Supports S0 metrics sample. Gate / execute bodies are filled in follow-up PRs.
 */
public final class CompactionMaintenanceTypeHandler extends AbstractMaintenanceTypeHandler {

  private static final Logger LOG = LoggerFactory.getLogger(CompactionMaintenanceTypeHandler.class);

  /** Creates a compaction type handler. */
  public CompactionMaintenanceTypeHandler() {
    super(Policy.BuiltInType.ICEBERG_COMPACTION);
  }

  @Override
  public MaintenanceGateDecision shouldExecute(MaintenanceTypeContext context) {
    // Follow-up: recommend + minIntervalMs + type thresholds.
    return MaintenanceGateDecision.EXECUTE;
  }

  @Override
  public void execute(MaintenanceTypeContext context) {
    LOG.debug(
        "Compaction execute stub for tableId={} policyId={}",
        context.tableId(),
        context.policyId());
    // Follow-up: invoke BuiltInJob / Spark rewrite-data-files leaf.
  }
}
