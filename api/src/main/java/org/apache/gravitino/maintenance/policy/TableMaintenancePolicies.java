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

import java.util.Map;
import javax.annotation.Nullable;
import org.apache.gravitino.policy.EventTrigger;
import org.apache.gravitino.policy.Policy;
import org.apache.gravitino.policy.PolicyContent;
import org.apache.gravitino.policy.PolicyExecutionFields;
import org.apache.gravitino.policy.TableMaintenanceContent;
import org.apache.gravitino.policy.TimeTrigger;

/**
 * Maps built-in policy types to TMS task kinds and applies maintenance-specific peer validation.
 *
 * <p>Shared execution peers ({@code timeTrigger} / {@code eventTrigger} / {@code minIntervalMs} /
 * {@code jobOptions} / {@code updateOptions}) are validated by {@link PolicyExecutionFields}. This
 * class adds TMS rules such as rejecting {@code eventTrigger.onCommit} for orphan-cleanup.
 */
public final class TableMaintenancePolicies {

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
   * Validates optional execution peer fields on table maintenance policy content.
   *
   * @param policyType policy built-in type
   * @param content policy content
   */
  public static void validate(Policy.BuiltInType policyType, PolicyContent content) {
    if (content instanceof TableMaintenanceContent) {
      TableMaintenanceContent maintenanceContent = (TableMaintenanceContent) content;
      validate(
          policyType,
          maintenanceContent.timeTrigger(),
          maintenanceContent.eventTrigger(),
          maintenanceContent.minIntervalMs(),
          maintenanceContent.jobOptions(),
          maintenanceContent.updateOptions());
    }
  }

  /**
   * Validates optional execution peer fields for supported built-in maintenance policy types.
   *
   * @param policyType policy built-in type
   * @param timeTrigger optional time trigger
   * @param eventTrigger optional event trigger
   * @param minIntervalMs optional cooldown ms
   * @param jobOptions job options (may be empty)
   * @param updateOptions updater options (may be empty)
   */
  public static void validate(
      Policy.BuiltInType policyType,
      @Nullable TimeTrigger timeTrigger,
      @Nullable EventTrigger eventTrigger,
      @Nullable Long minIntervalMs,
      Map<String, String> jobOptions,
      Map<String, String> updateOptions) {
    if (!PolicyExecutionFields.hasAny(
        timeTrigger, eventTrigger, minIntervalMs, jobOptions, updateOptions)) {
      return;
    }
    TableMaintenanceTaskType taskType = taskTypeFor(policyType);
    if (taskType == null) {
      return;
    }
    PolicyExecutionFields.validate(
        timeTrigger, eventTrigger, minIntervalMs, jobOptions, updateOptions);
    if (eventTrigger != null && eventTrigger.onCommit() && !taskType.onCommitAllowed()) {
      throw new IllegalArgumentException(
          String.format(
              "eventTrigger.onCommit=true is not allowed for maintenance task type %s",
              taskType.configKey()));
    }
  }
}
