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
package org.apache.gravitino.maintenance.job;

/**
 * Decides whether maintenance should run after metrics were sampled at {@code S0}.
 *
 * <p>Full type-specific recommenders land with the four maintenance executors; this gate only
 * answers maintain vs skip for the current in-job path.
 */
@FunctionalInterface
public interface MaintenanceRecommender {

  /**
   * @param tableId table entity id
   * @param policyId policy entity id
   * @param sampledSnapshotId Iceberg snapshot id at sample time
   * @param metricsJson serialized metrics for that snapshot
   * @return maintain or skip
   */
  MaintenanceRecommendDecision recommend(
      long tableId, long policyId, long sampledSnapshotId, String metricsJson);
}
