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
 * One built-in maintenance type's in-job pipeline steps.
 *
 * <p>The {@link MaintenanceTypeOrchestrator} runs: optional {@link #sampleBefore}, {@link
 * #shouldExecute}, {@link #execute}, optional {@link #sampleAfter}. Types that do not sample
 * metrics return {@code false} from {@link #supportsMetricsSample()} and no-op the sample hooks.
 *
 * <p>Concrete Iceberg work (rewrite data files, rewrite manifests, …) belongs in {@code execute};
 * existing {@code BuiltInJob} templates remain the Spark leaf, not this pipeline contract.
 */
public interface MaintenanceTypeHandler {

  /**
   * @return built-in policy type this handler owns
   */
  Policy.BuiltInType policyType();

  /**
   * @return {@code true} when this type samples S0 / optional S1 (compaction, manifest-rewrite)
   */
  boolean supportsMetricsSample();

  /**
   * Sample metrics at snapshot S0 and record drift against HEAD.
   *
   * <p>No-op when {@link #supportsMetricsSample()} is {@code false}.
   *
   * @param context per-type runtime state
   */
  void sampleBefore(MaintenanceTypeContext context);

  /**
   * Pre-execute gate (recommend / minInterval / type-specific rules).
   *
   * @param context per-type runtime state
   * @return whether to execute or skip this type
   */
  MaintenanceGateDecision shouldExecute(MaintenanceTypeContext context);

  /**
   * Run the maintenance action for this type.
   *
   * @param context per-type runtime state
   */
  void execute(MaintenanceTypeContext context);

  /**
   * Optional post-execute sample for this type.
   *
   * <p>Default no-op. Job-level S1 after the whole chain may live outside the per-type handler.
   *
   * @param context per-type runtime state
   */
  default void sampleAfter(MaintenanceTypeContext context) {}
}
