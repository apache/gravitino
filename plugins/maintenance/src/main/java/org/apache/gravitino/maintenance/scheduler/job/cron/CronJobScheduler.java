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
package org.apache.gravitino.maintenance.scheduler.job.cron;

import java.util.List;
import org.apache.gravitino.maintenance.scheduler.policy.expand.ExpandTableCandidate;
import org.apache.gravitino.meta.PolicyEntity;

/**
 * Inserts one-shot {@code cron-job} rows into the cron-pool db-scheduler table.
 *
 * <p>Existing instance ids should be left untouched so a repeated expand pick does not reset an
 * already-queued submit. Snapshot-expiry winners attached at schema/catalog are packed into {@code
 * batch:{batch_id}:{policy_id}} rows; incomplete batches are filled via {@code
 * SchedulerClient#getScheduledExecution} / {@code reschedule} before a new {@code batch_id} is
 * allocated (§5.2.2).
 *
 * <p>Enqueue builds a {@link org.apache.gravitino.maintenance.scheduler.MaintenanceJobSpec} once
 * (policy → jobConf) into {@code task_data}; pick consumes that spec.
 */
@FunctionalInterface
public interface CronJobScheduler {

  /**
   * Enqueues due-now cron jobs for the given expand candidates.
   *
   * @param policy effective maintenance policy (used to build the jobConf spec)
   * @param candidates enqueueable tables with winner levels
   * @param batchCapacity expand enqueue budget (max candidates considered for non-snapshot types).
   *     Snapshot-expiry merge capacity is a fixed per-batch table limit inside the implementation,
   *     not this value.
   * @return number of newly inserted cron-job rows (merges into existing batches do not count)
   */
  int enqueueDueNow(PolicyEntity policy, List<ExpandTableCandidate> candidates, int batchCapacity);
}
