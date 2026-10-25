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
package org.apache.gravitino.maintenance.scheduler.policy.expand;

import com.github.kagkarlsson.scheduler.task.ExecutionComplete;
import com.github.kagkarlsson.scheduler.task.schedule.CronSchedule;
import com.github.kagkarlsson.scheduler.task.schedule.CronStyle;
import com.github.kagkarlsson.scheduler.task.schedule.Schedule;
import com.google.common.annotations.VisibleForTesting;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;

/**
 * Quartz cron helpers for {@code policy-expand} next-due calculation.
 *
 * <p>Always evaluates cron expressions in UTC so every scheduler node computes the same next-due
 * instant for a given {@code timeTrigger}, regardless of the JVM default time zone.
 */
final class PolicyExpandCrons {

  /** Zone used for all policy-expand cron evaluations. */
  static final ZoneId SCHEDULER_ZONE = ZoneOffset.UTC;

  private PolicyExpandCrons() {}

  /**
   * Next fire time for a Quartz cron, strictly after now, in {@link #SCHEDULER_ZONE}.
   *
   * @param timeTrigger six-field Quartz cron expression
   * @return next execution instant
   */
  static Instant nextExecution(String timeTrigger) {
    return nextExecution(timeTrigger, Instant.now());
  }

  /**
   * Next fire time for a Quartz cron, strictly after {@code after}, in {@link #SCHEDULER_ZONE}.
   *
   * @param timeTrigger six-field Quartz cron expression
   * @param after reference instant
   * @return next execution instant
   */
  @VisibleForTesting
  static Instant nextExecution(String timeTrigger, Instant after) {
    CronSchedule schedule = new CronSchedule(timeTrigger, SCHEDULER_ZONE, CronStyle.QUARTZ);
    Instant next = schedule.getNextExecutionTime(ExecutionComplete.simulatedSuccess(after));
    if (next == null || Schedule.NEVER.equals(next)) {
      throw new IllegalArgumentException("timeTrigger has no next execution: " + timeTrigger);
    }
    return next;
  }
}
