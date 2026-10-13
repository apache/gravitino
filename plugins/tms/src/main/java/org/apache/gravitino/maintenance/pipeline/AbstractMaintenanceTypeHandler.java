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

import org.apache.gravitino.policy.Policy;

/**
 * Base handler that wires {@link #supportsMetricsSample()} from {@link MaintenanceMetricsSupport}
 * and no-ops sample hooks when sampling is disabled.
 */
public abstract class AbstractMaintenanceTypeHandler implements MaintenanceTypeHandler {

  private final Policy.BuiltInType policyType;

  /**
   * @param policyType built-in type this handler owns
   */
  protected AbstractMaintenanceTypeHandler(Policy.BuiltInType policyType) {
    this.policyType = policyType;
  }

  @Override
  public final Policy.BuiltInType policyType() {
    return policyType;
  }

  @Override
  public final boolean supportsMetricsSample() {
    return MaintenanceMetricsSupport.supportsMetricsSample(policyType);
  }

  @Override
  public final void sampleBefore(MaintenanceTypeContext context) {
    if (!supportsMetricsSample()) {
      return;
    }
    doSampleBefore(context);
  }

  @Override
  public final void sampleAfter(MaintenanceTypeContext context) {
    if (!supportsMetricsSample()) {
      return;
    }
    doSampleAfter(context);
  }

  /**
   * Sample S0 for metrics-capable types. Invoked only when {@link #supportsMetricsSample()} is
   * {@code true}.
   *
   * @param context per-type runtime state
   */
  protected void doSampleBefore(MaintenanceTypeContext context) {
    // Wired in follow-up: persist table_snapshot_metrics + before_snapshot_id.
  }

  /**
   * Optional per-type after sample. Invoked only when {@link #supportsMetricsSample()} is {@code
   * true}.
   *
   * @param context per-type runtime state
   */
  protected void doSampleAfter(MaintenanceTypeContext context) {
    // Optional; job-level S1 may replace this.
  }
}
