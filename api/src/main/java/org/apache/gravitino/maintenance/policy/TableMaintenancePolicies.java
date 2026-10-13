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
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.policy.Policy;
import org.apache.gravitino.policy.PolicyContent;
import org.apache.gravitino.policy.TableMaintenanceContent;

/**
 * Maps built-in policy types to TMS task kinds and validates optional TMS peer fields on policy
 * content.
 *
 * <p>Validation of {@code schedule} / {@code minIntervalMs} / {@code jobOptions} / {@code
 * updateOptions} on {@link TableMaintenanceContent} is centralized here and invoked from {@link
 * PolicyContent#validate()}, {@link org.apache.gravitino.meta.PolicyEntity}, and maintenance
 * PolicyContentDTO {@code validate()}.
 */
public final class TableMaintenancePolicies {

  /** JSON property for {@link MaintenanceTrigger}. */
  public static final String SCHEDULE_JSON_KEY = "schedule";

  /** JSON property for minimum interval between finished jobs. */
  public static final String MIN_INTERVAL_MS_JSON_KEY = "minIntervalMs";

  /** JSON property for Spark / IRC client options. */
  public static final String JOB_OPTIONS_JSON_KEY = "jobOptions";

  /** JSON property for Gravitino updater options. */
  public static final String UPDATE_OPTIONS_JSON_KEY = "updateOptions";

  private TableMaintenancePolicies() {}

  /**
   * Returns the maintenance task type for a built-in policy, if any.
   *
   * @param policyType built-in policy type
   * @return task type or {@code null}
   */
  @Nullable
  public static TableMaintenanceTaskType taskTypeFor(Policy.BuiltInType policyType) {
    if (policyType == null) {
      return null;
    }
    return TableMaintenanceTaskType.fromPolicyTypeString(policyType.policyType());
  }

  /**
   * Validates optional TMS peer fields on table maintenance policy content.
   *
   * @param policyType policy built-in type
   * @param content policy content
   */
  public static void validate(Policy.BuiltInType policyType, PolicyContent content) {
    if (content instanceof TableMaintenanceContent) {
      TableMaintenanceContent maintenanceContent = (TableMaintenanceContent) content;
      validate(
          policyType,
          maintenanceContent.schedule(),
          maintenanceContent.minIntervalMs(),
          maintenanceContent.jobOptions(),
          maintenanceContent.updateOptions());
    }
  }

  /**
   * Validates optional TMS peer fields for supported built-in policy types.
   *
   * @param policyType policy built-in type
   * @param schedule optional evaluate trigger
   * @param minIntervalMs optional cooldown ms
   * @param jobOptions Spark / IRC client options (may be empty)
   * @param updateOptions updater options (may be empty)
   */
  public static void validate(
      Policy.BuiltInType policyType,
      @Nullable MaintenanceTrigger schedule,
      @Nullable Long minIntervalMs,
      Map<String, String> jobOptions,
      Map<String, String> updateOptions) {
    if (!hasAnyMaintenanceField(schedule, minIntervalMs, jobOptions, updateOptions)) {
      return;
    }
    TableMaintenanceTaskType taskType = taskTypeFor(policyType);
    if (taskType == null) {
      return;
    }
    if (schedule != null) {
      schedule.validate(taskType);
    }
    if (minIntervalMs != null) {
      Preconditions.checkArgument(minIntervalMs >= 0, "minIntervalMs must be >= 0");
    }
    validateOptionMap(jobOptions, "jobOptions");
    validateOptionMap(updateOptions, "updateOptions");
  }

  /**
   * Whether any TMS maintenance field is set.
   *
   * @param schedule evaluate trigger
   * @param minIntervalMs cooldown ms
   * @param jobOptions job options map
   * @param updateOptions update options map
   * @return {@code true} when any field is present
   */
  public static boolean hasAnyMaintenanceField(
      @Nullable MaintenanceTrigger schedule,
      @Nullable Long minIntervalMs,
      @Nullable Map<String, String> jobOptions,
      @Nullable Map<String, String> updateOptions) {
    return schedule != null
        || minIntervalMs != null
        || (jobOptions != null && !jobOptions.isEmpty())
        || (updateOptions != null && !updateOptions.isEmpty());
  }

  /**
   * Merges option maps from a nearer policy over a farther policy (farther first, nearer wins).
   *
   * <p>Used for both {@code jobOptions} and {@code updateOptions}.
   *
   * @param farther options from a more distant attachment
   * @param nearer options from a nearer attachment
   * @return merged map
   */
  public static Map<String, String> mergeOptions(
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

  private static void validateOptionMap(Map<String, String> options, String mapName) {
    if (options == null || options.isEmpty()) {
      return;
    }
    options.forEach(
        (key, value) -> {
          Preconditions.checkArgument(StringUtils.isNotBlank(key), "%s key is blank", mapName);
          Preconditions.checkArgument(
              StringUtils.isNotBlank(value), "%s '%s' must have non-empty value", mapName, key);
        });
  }
}
