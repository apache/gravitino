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

/**
 * Maps built-in policy types to TMS task kinds and validates optional TMS peer fields on policy
 * content.
 *
 * <p>Validation of {@code schedule} / {@code minIntervalMs} / {@code jobOptions} / {@code
 * updateOptions} lives here — called from PolicyManager, PolicyEntity, and maintenance
 * PolicyContentDTO {@code validate()} — not inside the four task content {@code validate()} methods
 * (those cover task-specific thresholds only).
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
   * Validates optional TMS peer fields for supported built-in policy types.
   *
   * @param policyType policy built-in type
   * @param peerFields optional TMS peer fields from JSON / DTO / entity
   */
  public static void validate(
      Policy.BuiltInType policyType, @Nullable MaintenancePeerFields peerFields) {
    if (peerFields == null || !peerFields.hasAnyField()) {
      return;
    }
    TableMaintenanceTaskType taskType = taskTypeFor(policyType);
    if (taskType == null) {
      return;
    }
    MaintenanceTrigger schedule = peerFields.schedule();
    Long minIntervalMs = peerFields.minIntervalMs();
    if (schedule != null) {
      schedule.validate(taskType);
    }
    if (minIntervalMs != null) {
      Preconditions.checkArgument(minIntervalMs >= 0, "minIntervalMs must be >= 0");
    }
    validateOptionMap(peerFields.jobOptions(), "jobOptions");
    validateOptionMap(peerFields.updateOptions(), "updateOptions");
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
    options.forEach(
        (key, value) -> {
          Preconditions.checkArgument(StringUtils.isNotBlank(key), "%s key is blank", mapName);
          Preconditions.checkArgument(
              StringUtils.isNotBlank(value), "%s '%s' must have non-empty value", mapName, key);
        });
  }
}
