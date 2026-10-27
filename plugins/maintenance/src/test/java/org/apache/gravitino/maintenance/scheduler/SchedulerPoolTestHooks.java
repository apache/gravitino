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

import com.google.common.base.Preconditions;
import java.util.function.BiConsumer;

/**
 * Same-package test access to {@link TableMaintenanceSchedulerPools} submit handlers.
 *
 * <p>Keeps production pools free of {@code *ForTest} APIs while still allowing BaseITs to record
 * cron/commit enqueue without a real Spark {@code runJob}.
 */
public final class SchedulerPoolTestHooks {

  private SchedulerPoolTestHooks() {}

  /**
   * Replaces the cron-job submit handler after pools have started.
   *
   * @param submitHandler handler used by the cron pool task
   */
  public static void installCronSubmit(BiConsumer<String, MaintenanceJobSpec> submitHandler) {
    TableMaintenanceSchedulerPools.CRON_SUBMIT.set(Preconditions.checkNotNull(submitHandler));
  }

  /**
   * Replaces the commit-job submit handler after pools have started.
   *
   * @param submitHandler handler used by the commit pool task
   */
  public static void installCommitSubmit(BiConsumer<String, MaintenanceJobSpec> submitHandler) {
    TableMaintenanceSchedulerPools.COMMIT_SUBMIT.set(Preconditions.checkNotNull(submitHandler));
  }
}
