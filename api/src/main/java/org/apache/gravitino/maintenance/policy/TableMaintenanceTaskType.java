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
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.policy.Policy;

/** Maintenance task kinds driven by TMS policy content. */
public enum TableMaintenanceTaskType {
  /** Iceberg data file compaction. */
  COMPACTION("compaction", Policy.BuiltInType.ICEBERG_COMPACTION, 3_600_000L, true),
  /** Iceberg manifest rewrite (future built-in policy type). */
  MANIFEST_REWRITE("manifest-rewrite", null, 3_600_000L, true),
  /** Iceberg snapshot expiration (future built-in policy type). */
  SNAPSHOT_EXPIRY("snapshot-expiry", null, 3_600_000L, true),
  /** Orphan file cleanup (future built-in policy type). */
  ORPHAN_CLEANUP("orphan-cleanup", null, 86_400_000L, false);

  private final String configKey;
  private final Policy.BuiltInType builtInType;
  private final long defaultMinIntervalMs;
  private final boolean onCommitAllowed;

  TableMaintenanceTaskType(
      String configKey,
      Policy.BuiltInType builtInType,
      long defaultMinIntervalMs,
      boolean onCommitAllowed) {
    this.configKey = configKey;
    this.builtInType = builtInType;
    this.defaultMinIntervalMs = defaultMinIntervalMs;
    this.onCommitAllowed = onCommitAllowed;
  }

  /**
   * Returns the configuration key segment used in {@code gravitino.maintenance.task.<type>.*}.
   *
   * @return task type key
   */
  public String configKey() {
    return configKey;
  }

  /**
   * Returns the built-in policy type that carries this maintenance task, if registered.
   *
   * @return built-in policy type or {@code null} when not yet wired
   */
  public Policy.BuiltInType builtInType() {
    return builtInType;
  }

  /**
   * Returns the code-default minimum interval between finished runs for this task type.
   *
   * @return interval in milliseconds
   */
  public long defaultMinIntervalMs() {
    return defaultMinIntervalMs;
  }

  /**
   * Returns whether {@code schedule.onCommit} is allowed for this task type.
   *
   * @return {@code true} when onCommit is supported
   */
  public boolean onCommitAllowed() {
    return onCommitAllowed;
  }

  /**
   * Maps a built-in policy type to a maintenance task type.
   *
   * @param builtInType built-in policy type
   * @return matching task type
   * @throws IllegalArgumentException when the policy type is not a TMS maintenance type
   */
  public static TableMaintenanceTaskType fromBuiltInType(Policy.BuiltInType builtInType) {
    Preconditions.checkNotNull(builtInType, "builtInType cannot be null");
    for (TableMaintenanceTaskType type : values()) {
      if (builtInType.equals(type.builtInType)) {
        return type;
      }
    }
    throw new IllegalArgumentException(
        String.format(
            "Policy type %s is not a table maintenance task type", builtInType.policyType()));
  }

  /**
   * Returns the task type for a built-in policy type string, or {@code null} if unknown.
   *
   * @param policyType policy type string
   * @return task type or {@code null}
   */
  public static TableMaintenanceTaskType fromPolicyTypeString(String policyType) {
    if (StringUtils.isBlank(policyType)) {
      return null;
    }
    for (TableMaintenanceTaskType type : values()) {
      if (type.builtInType != null && type.builtInType.policyType().equalsIgnoreCase(policyType)) {
        return type;
      }
    }
    return null;
  }
}
