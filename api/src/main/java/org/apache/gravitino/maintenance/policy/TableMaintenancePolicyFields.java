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
package org.apache.gravitino.maintenance.policy;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;

/**
 * TMS scheduling and non-sensitive Spark / IRC client options stored in policy content beside type-
 * specific thresholds.
 */
public final class TableMaintenancePolicyFields {

  @Nullable private final TableMaintenanceSchedule schedule;
  @Nullable private final Long minIntervalMs;
  private final Map<String, String> jobOptions;

  /**
   * Creates TMS policy fields.
   *
   * @param schedule evaluate triggers, or null when unset
   * @param minIntervalMs cooldown after last finished job for this policy type, or null
   * @param jobOptions non-sensitive Spark / IRC client properties
   */
  public TableMaintenancePolicyFields(
      @Nullable TableMaintenanceSchedule schedule,
      @Nullable Long minIntervalMs,
      @Nullable Map<String, String> jobOptions) {
    this.schedule = schedule;
    this.minIntervalMs = minIntervalMs;
    if (jobOptions == null || jobOptions.isEmpty()) {
      this.jobOptions = Collections.emptyMap();
    } else {
      this.jobOptions = Collections.unmodifiableMap(new LinkedHashMap<>(jobOptions));
    }
  }

  /**
   * Returns whether any TMS field is set.
   *
   * @return {@code true} when schedule, interval, or job options are present
   */
  public boolean isPresent() {
    return schedule != null || minIntervalMs != null || !jobOptions.isEmpty();
  }

  /**
   * @return schedule when set
   */
  @Nullable
  public TableMaintenanceSchedule schedule() {
    return schedule;
  }

  /**
   * @return policy-level minimum interval when set
   */
  @Nullable
  public Long minIntervalMs() {
    return minIntervalMs;
  }

  /**
   * @return non-sensitive job options
   */
  public Map<String, String> jobOptions() {
    return jobOptions;
  }

  /**
   * Validates TMS fields for a maintenance task type.
   *
   * @param taskType task type for this policy
   */
  public void validate(TableMaintenanceTaskType taskType) {
    Preconditions.checkNotNull(taskType, "taskType cannot be null");
    if (!isPresent()) {
      return;
    }
    if (schedule != null) {
      schedule.validate(taskType);
    }
    if (minIntervalMs != null) {
      Preconditions.checkArgument(minIntervalMs >= 0, "minIntervalMs must be >= 0");
    }
    MaintenanceSensitiveOptionKeys.validateNonSensitive(jobOptions, "jobOptions");
    jobOptions.forEach(
        (key, value) -> {
          Preconditions.checkArgument(StringUtils.isNotBlank(key), "jobOptions key is blank");
          Preconditions.checkArgument(
              StringUtils.isNotBlank(value), "jobOptions '%s' must have non-empty value", key);
        });
  }

  /**
   * Merges job options from a nearer policy over a farther policy (farther first, nearer wins).
   *
   * @param farther options from a more distant attachment
   * @param nearer options from a nearer attachment
   * @return merged map
   */
  public static Map<String, String> mergeJobOptions(
      Map<String, String> farther, Map<String, String> nearer) {
    if ((farther == null || farther.isEmpty()) && (nearer == null || nearer.isEmpty())) {
      return Collections.emptyMap();
    }
    Map<String, String> merged = new LinkedHashMap<>();
    if (farther != null) {
      merged.putAll(farther);
    }
    if (nearer != null) {
      merged.putAll(nearer);
    }
    return ImmutableMap.copyOf(merged);
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof TableMaintenancePolicyFields)) {
      return false;
    }
    TableMaintenancePolicyFields that = (TableMaintenancePolicyFields) o;
    return Objects.equals(schedule, that.schedule)
        && Objects.equals(minIntervalMs, that.minIntervalMs)
        && Objects.equals(jobOptions, that.jobOptions);
  }

  @Override
  public int hashCode() {
    return Objects.hash(schedule, minIntervalMs, jobOptions);
  }

  @Override
  public String toString() {
    return "TableMaintenancePolicyFields{"
        + "schedule="
        + schedule
        + ", minIntervalMs="
        + minIntervalMs
        + ", jobOptions="
        + jobOptions
        + '}';
  }
}
