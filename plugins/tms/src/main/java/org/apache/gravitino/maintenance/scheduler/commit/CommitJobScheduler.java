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
package org.apache.gravitino.maintenance.scheduler.commit;

/**
 * Upserts one-shot {@code commit-job} rows for IRC table updates (§5.2.3).
 *
 * <p>Instance id is {@code {table_id}}; bursts of commits on the same table coalesce via {@code
 * scheduleIfNotExists}. Enqueue builds a {@link
 * org.apache.gravitino.maintenance.scheduler.MaintenanceJobSnapshot} when at least one onCommit
 * type is due; an existing row's {@code task_data} is left untouched.
 */
public interface CommitJobScheduler {

  /**
   * Builds an enqueue-time snapshot (chain + jobConf) and schedules a due-now {@code commit-job}
   * when one is not already queued and the snapshot is non-empty.
   *
   * @param tableId table entity id
   * @return insert / coalesce / skip outcome
   */
  CommitJobUpsertResult upsertDueNow(long tableId);
}
