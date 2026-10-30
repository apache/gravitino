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
package org.apache.gravitino.maintenance.scheduler.job.commit;

/**
 * Outcome of {@link CommitJobClient#enqueueNow(long)}.
 *
 * <p>Distinguishes coalesce ({@link #ALREADY_QUEUED}) from enqueue gate skips ({@link #SKIPPED}) so
 * callers do not log "already queued" when no spec was written.
 */
public enum CommitJobEnqueueResult {

  /** A new {@code commit-job} row was inserted with a MaintenanceJobSpec. */
  INSERTED,

  /** A {@code commit-job} row already exists for the table (coalesced; {@code task_data} kept). */
  ALREADY_QUEUED,

  /**
   * Nothing written: empty / cooled-down chain, auth failure, unresolved table, or jobConf build
   * failure.
   */
  SKIPPED
}
