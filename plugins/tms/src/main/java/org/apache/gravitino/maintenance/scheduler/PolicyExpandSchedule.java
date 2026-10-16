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
package org.apache.gravitino.maintenance.scheduler;

import javax.annotation.Nullable;

/**
 * Registers one long-lived {@code policy-expand} row per maintenance policy.
 *
 * <p>The instance id is the policy id. A blank {@code timeTrigger}, or a disabled policy, removes
 * the row. Handlers that fan the row out into cron jobs are out of scope here.
 */
public interface PolicyExpandSchedule {

  /**
   * Inserts or moves the expand row to the next cron fire time, or removes it when the policy
   * should not run.
   *
   * @param policyId persisted policy id, used as the db-scheduler task instance
   * @param enabled whether the policy is enabled
   * @param timeTrigger cron expression, or {@code null} when the policy has no time trigger
   */
  void sync(long policyId, boolean enabled, @Nullable String timeTrigger);

  /**
   * Deletes the expand row for a dropped policy.
   *
   * @param policyId persisted policy id
   */
  void remove(long policyId);
}
