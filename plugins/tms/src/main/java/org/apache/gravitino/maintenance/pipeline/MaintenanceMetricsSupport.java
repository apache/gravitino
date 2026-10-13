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
 * Which built-in maintenance types participate in S0 / S1 metrics sampling.
 *
 * <p>Only compaction and manifest-rewrite need sample → recommend → execute. Snapshot-expiry and
 * orphan-cleanup skip sampling and go through gate + execute only.
 */
public final class MaintenanceMetricsSupport {

  private MaintenanceMetricsSupport() {}

  /**
   * @param policyType built-in policy type
   * @return {@code true} when the type samples metrics before / after work
   */
  public static boolean supportsMetricsSample(Policy.BuiltInType policyType) {
    return policyType == Policy.BuiltInType.ICEBERG_COMPACTION
        || policyType == Policy.BuiltInType.ICEBERG_REWRITE_MANIFESTS;
  }
}
