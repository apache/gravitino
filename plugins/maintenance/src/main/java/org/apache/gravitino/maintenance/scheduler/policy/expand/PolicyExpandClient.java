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

import com.github.kagkarlsson.scheduler.SchedulerClient;
import com.github.kagkarlsson.scheduler.exceptions.TaskInstanceCurrentlyExecutingException;
import com.github.kagkarlsson.scheduler.task.ExecutionComplete;
import com.github.kagkarlsson.scheduler.task.TaskInstance;
import com.github.kagkarlsson.scheduler.task.TaskInstanceId;
import com.github.kagkarlsson.scheduler.task.schedule.CronSchedule;
import com.github.kagkarlsson.scheduler.task.schedule.CronStyle;
import com.github.kagkarlsson.scheduler.task.schedule.Schedule;
import com.google.common.base.Preconditions;
import java.time.Instant;
import java.time.ZoneId;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import javax.sql.DataSource;
import org.apache.gravitino.maintenance.policy.MaintenancePolicyDefinitions;
import org.apache.gravitino.maintenance.scheduler.AutocommitDataSources;
import org.apache.gravitino.maintenance.scheduler.MaintenanceSchedulerPools;
import org.apache.gravitino.maintenance.scheduler.MaintenanceTaskIds;
import org.apache.gravitino.meta.PolicyEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Expand-pool counterpart of {@code CronJobClient} / {@code CommitJobClient}: writes {@code
 * policy-expand} lease rows into {@link MaintenanceSchedulerPools#EXPAND_TASKS_TABLE}.
 *
 * <p>The instance id is the policy id. {@link #sync} / {@link #align} decide from policy content
 * whether the row should exist; callers can also {@link #remove} by id. Fan-out into {@code
 * cron-job} rows is {@link PolicyExpandPlanner} on pick (via {@link
 * MaintenanceExpandSubmitPipeline}).
 *
 * <p>{@code timeTrigger} is a six-field Quartz cron. This class decides whether the expression has
 * a next fire time; policy content does not parse cron itself.
 *
 * <p>{@link #joiningCurrentTransaction()} writes through the open MyBatis session. The client must
 * not commit or close that connection, so the policy transaction and the expand row commit
 * together.
 */
public final class PolicyExpandClient {

  private static final Logger LOG = LoggerFactory.getLogger(PolicyExpandClient.class);

  /** Attempts while a pick still owns the expand row. */
  private static final int EXECUTING_ATTEMPTS = 10;

  /** Pause between attempts, so a short pick can finish inside the policy transaction. */
  private static final long EXECUTING_WAIT_MS = 50L;

  private final DataSource dataSource;

  private PolicyExpandClient(DataSource dataSource) {
    this.dataSource = dataSource;
  }

  /**
   * Opens a client that writes on the current thread's open SQL session.
   *
   * <p>The db-scheduler client is created on each call, because building it borrows a connection.
   * That connection is the open session, so this client must be used inside a policy transaction.
   *
   * @return client that inserts, moves, and deletes expand rows in the caller's transaction
   */
  public static PolicyExpandClient joiningCurrentTransaction() {
    return new PolicyExpandClient(AutocommitDataSources.joiningCurrentSession());
  }

  /**
   * Opens a client against the entity-store pool with JDBC autocommit forced on.
   *
   * <p>Used for post-commit reads and overdue writes such as {@link #scheduleAt}. The entity-store
   * pool often disables autocommit; without this wrap, SchedulerClient mutations would not commit
   * and the expand pool would never observe them.
   *
   * @param dataSource relational JDBC pool shared with the entity store
   * @return client that can read and write expand rows after the policy transaction commits
   */
  public static PolicyExpandClient forReads(DataSource dataSource) {
    return new PolicyExpandClient(AutocommitDataSources.wrap(dataSource));
  }

  private SchedulerClient client() {
    return SchedulerClient.Builder.create(dataSource, MaintenanceSchedulerPools.policyExpandTask())
        .tableName(MaintenanceSchedulerPools.EXPAND_TASKS_TABLE)
        .build();
  }

  /**
   * Aligns the expand row with a policy write (create / alter).
   *
   * <p>Enabled maintenance policies with a non-blank {@code timeTrigger} are scheduled at the next
   * fire. Disable, blank trigger, and non-maintenance content remove the row. An invalid cron
   * throws so the policy transaction rolls back.
   *
   * @param policy persisted policy
   */
  public void sync(PolicyEntity policy) {
    Preconditions.checkNotNull(policy, "policy");
    if (!MaintenancePolicyDefinitions.isMaintenance(policy.content())) {
      remove(policy.id());
      return;
    }
    String trigger = MaintenancePolicyDefinitions.timeTrigger(policy.content());
    if (!policy.enabled() || trigger == null || trigger.isBlank()) {
      remove(policy.id());
      return;
    }
    scheduleAt(policy.id(), nextExecution(trigger));
  }

  /**
   * Aligns the expand row during startup reconcile.
   *
   * <p>Same keep/remove rules as {@link #sync}, but preserves an overdue due so the expand pool can
   * catch up, skips when a future due already matches the cron, and removes the expand row (never
   * the policy) when the cron is invalid.
   *
   * @param policy persisted policy
   */
  public void align(PolicyEntity policy) {
    Preconditions.checkNotNull(policy, "policy");
    if (!MaintenancePolicyDefinitions.isMaintenance(policy.content())) {
      remove(policy.id());
      return;
    }
    String trigger = MaintenancePolicyDefinitions.timeTrigger(policy.content());
    if (!policy.enabled() || trigger == null || trigger.isBlank()) {
      remove(policy.id());
      return;
    }
    Instant expectedNext;
    try {
      expectedNext = nextExecution(trigger);
    } catch (IllegalArgumentException e) {
      remove(policy.id());
      LOG.error(
          "Cleared policy-expand task for policy id {} name {} with invalid timeTrigger; fix the policy content",
          policy.id(),
          policy.name(),
          e);
      return;
    }
    Optional<Instant> current = scheduledExecutionTime(policy.id());
    Instant now = Instant.now();
    if (current.isPresent() && current.get().isBefore(now)) {
      // Overdue: keep so db-scheduler can catch up after a restart.
      return;
    }
    if (current.isPresent() && current.get().equals(expectedNext)) {
      return;
    }
    scheduleAt(policy.id(), expectedNext);
  }

  /**
   * Inserts or moves the expand row to an explicit due time.
   *
   * @param policyId persisted policy id
   * @param due execution time to store
   */
  public void scheduleAt(long policyId, Instant due) {
    runDespiteExecution(
        () -> {
          TaskInstanceId instanceId = instanceId(policyId);
          boolean scheduled =
              client()
                  .schedule(
                      new TaskInstance<>(
                          MaintenanceSchedulerPools.POLICY_EXPAND, instanceId.getId()),
                      due,
                      SchedulerClient.ScheduleOptions.WHEN_EXISTS_RESCHEDULE);
          if (!scheduled) {
            throw new IllegalStateException(
                "Failed to schedule policy-expand instance " + instanceId.getId() + " at " + due);
          }
        });
  }

  /**
   * Deletes the expand row when it is present.
   *
   * @param policyId persisted policy id
   */
  public void remove(long policyId) {
    TaskInstanceId instanceId = instanceId(policyId);
    runDespiteExecution(
        () -> {
          SchedulerClient scheduler = client();
          if (scheduler.getScheduledExecution(instanceId).isPresent()) {
            scheduler.cancel(instanceId);
          }
        });
  }

  /**
   * @param policyId persisted policy id
   * @return next execution time of the expand row, or empty when the row is absent
   */
  public Optional<Instant> scheduledExecutionTime(long policyId) {
    return client()
        .getScheduledExecution(instanceId(policyId))
        .map(execution -> execution.getExecutionTime());
  }

  /**
   * @return policy ids that currently have a {@code policy-expand} row
   */
  public Set<Long> scheduledPolicyIds() {
    Set<Long> ids = new HashSet<>();
    client()
        .fetchScheduledExecutionsForTask(
            MaintenanceSchedulerPools.POLICY_EXPAND,
            Void.class,
            execution -> {
              MaintenanceTaskIds.parseExpandPolicy(execution.getTaskInstance().getId())
                  .ifPresent(ids::add);
            });
    return ids;
  }

  /**
   * Next fire time for a Quartz cron, strictly after now.
   *
   * @param timeTrigger six-field Quartz cron expression
   * @return next execution instant in the system default zone
   */
  public static Instant nextExecution(String timeTrigger) {
    CronSchedule schedule = new CronSchedule(timeTrigger, ZoneId.systemDefault(), CronStyle.QUARTZ);
    Instant next = schedule.getNextExecutionTime(ExecutionComplete.simulatedSuccess(Instant.now()));
    if (next == null || Schedule.NEVER.equals(next)) {
      throw new IllegalArgumentException("timeTrigger has no next execution: " + timeTrigger);
    }
    return next;
  }

  private static TaskInstanceId instanceId(long policyId) {
    return TaskInstanceId.of(
        MaintenanceSchedulerPools.POLICY_EXPAND, MaintenanceTaskIds.forExpandPolicy(policyId));
  }

  /**
   * Retries while db-scheduler reports the expand row as picked.
   *
   * <p>A pick rejects both reschedule and cancel. Failing the attempt here would roll the policy
   * transaction back. After the retry window the in-flight expand pick applies the committed policy
   * on its own completion path.
   *
   * @param action one scheduler write
   */
  private static void runDespiteExecution(Runnable action) {
    for (int attempt = 1; ; attempt++) {
      try {
        action.run();
        return;
      } catch (TaskInstanceCurrentlyExecutingException e) {
        if (attempt >= EXECUTING_ATTEMPTS) {
          LOG.warn(
              "policy-expand row is still executing; leaving the in-flight pick to apply the policy",
              e);
          return;
        }
        try {
          Thread.sleep(EXECUTING_WAIT_MS);
        } catch (InterruptedException interrupted) {
          Thread.currentThread().interrupt();
          e.addSuppressed(interrupted);
          throw e;
        }
      }
    }
  }
}
