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
import javax.annotation.Nullable;
import org.apache.gravitino.Config;
import org.apache.gravitino.config.ConfigBuilder;
import org.apache.gravitino.config.ConfigConstants;
import org.apache.gravitino.config.ConfigEntry;
import org.apache.gravitino.policy.Policy;

/**
 * Server config entries for per-type {@code minIntervalMs} cooldowns in {@code gravitino.conf}.
 *
 * <p>Used when a maintenance policy omits {@code minIntervalMs}. Resolution order is policy content
 * → these server keys → the code defaults below.
 */
public final class MaintenanceMinIntervalConfigs {

  /** Default cooldown for compaction / manifest-rewrite / snapshot-expiry (1 hour). */
  public static final long DEFAULT_HOURLY_MIN_INTERVAL_MS = 3_600_000L;

  /** Default cooldown for orphan-cleanup (1 day). */
  public static final long DEFAULT_ORPHAN_CLEANUP_MIN_INTERVAL_MS = 86_400_000L;

  /** Config key for compaction cooldown. */
  public static final String COMPACTION_MIN_INTERVAL_MS_KEY =
      "gravitino.maintenance.task.compaction.minIntervalMs";

  /** Config key for manifest-rewrite cooldown. */
  public static final String MANIFEST_REWRITE_MIN_INTERVAL_MS_KEY =
      "gravitino.maintenance.task.manifest-rewrite.minIntervalMs";

  /** Config key for snapshot-expiry cooldown. */
  public static final String SNAPSHOT_EXPIRY_MIN_INTERVAL_MS_KEY =
      "gravitino.maintenance.task.snapshot-expiry.minIntervalMs";

  /** Config key for orphan-cleanup cooldown. */
  public static final String ORPHAN_CLEANUP_MIN_INTERVAL_MS_KEY =
      "gravitino.maintenance.task.orphan-cleanup.minIntervalMs";

  /** Cooldown between finished compaction runs when the policy omits {@code minIntervalMs}. */
  public static final ConfigEntry<Long> MAINTENANCE_TASK_COMPACTION_MIN_INTERVAL_MS =
      new ConfigBuilder(COMPACTION_MIN_INTERVAL_MS_KEY)
          .doc(
              "Minimum interval in milliseconds between finished compaction runs when the "
                  + "policy omits minIntervalMs")
          .version(ConfigConstants.VERSION_2_0_0)
          .longConf()
          .checkValue(v -> v != null && v >= 0, "minIntervalMs must be >= 0")
          .createWithDefault(DEFAULT_HOURLY_MIN_INTERVAL_MS);

  /**
   * Cooldown between finished manifest-rewrite runs when the policy omits {@code minIntervalMs}.
   */
  public static final ConfigEntry<Long> MAINTENANCE_TASK_MANIFEST_REWRITE_MIN_INTERVAL_MS =
      new ConfigBuilder(MANIFEST_REWRITE_MIN_INTERVAL_MS_KEY)
          .doc(
              "Minimum interval in milliseconds between finished manifest-rewrite runs when the "
                  + "policy omits minIntervalMs")
          .version(ConfigConstants.VERSION_2_0_0)
          .longConf()
          .checkValue(v -> v != null && v >= 0, "minIntervalMs must be >= 0")
          .createWithDefault(DEFAULT_HOURLY_MIN_INTERVAL_MS);

  /** Cooldown between finished snapshot-expiry runs when the policy omits {@code minIntervalMs}. */
  public static final ConfigEntry<Long> MAINTENANCE_TASK_SNAPSHOT_EXPIRY_MIN_INTERVAL_MS =
      new ConfigBuilder(SNAPSHOT_EXPIRY_MIN_INTERVAL_MS_KEY)
          .doc(
              "Minimum interval in milliseconds between finished snapshot-expiry runs when the "
                  + "policy omits minIntervalMs")
          .version(ConfigConstants.VERSION_2_0_0)
          .longConf()
          .checkValue(v -> v != null && v >= 0, "minIntervalMs must be >= 0")
          .createWithDefault(DEFAULT_HOURLY_MIN_INTERVAL_MS);

  /** Cooldown between finished orphan-cleanup runs when the policy omits {@code minIntervalMs}. */
  public static final ConfigEntry<Long> MAINTENANCE_TASK_ORPHAN_CLEANUP_MIN_INTERVAL_MS =
      new ConfigBuilder(ORPHAN_CLEANUP_MIN_INTERVAL_MS_KEY)
          .doc(
              "Minimum interval in milliseconds between finished orphan-cleanup runs when the "
                  + "policy omits minIntervalMs")
          .version(ConfigConstants.VERSION_2_0_0)
          .longConf()
          .checkValue(v -> v != null && v >= 0, "minIntervalMs must be >= 0")
          .createWithDefault(DEFAULT_ORPHAN_CLEANUP_MIN_INTERVAL_MS);

  private MaintenanceMinIntervalConfigs() {}

  /**
   * Returns the {@code minIntervalMs} config entry for a built-in maintenance policy type.
   *
   * @param builtInType built-in policy type
   * @return config entry
   */
  public static ConfigEntry<Long> forBuiltInType(Policy.BuiltInType builtInType) {
    Preconditions.checkNotNull(builtInType, "builtInType cannot be null");
    switch (builtInType) {
      case ICEBERG_COMPACTION:
        return MAINTENANCE_TASK_COMPACTION_MIN_INTERVAL_MS;
      case ICEBERG_REWRITE_MANIFESTS:
        return MAINTENANCE_TASK_MANIFEST_REWRITE_MIN_INTERVAL_MS;
      case ICEBERG_SNAPSHOT_EXPIRATION:
        return MAINTENANCE_TASK_SNAPSHOT_EXPIRY_MIN_INTERVAL_MS;
      case ICEBERG_ORPHAN_FILE_REMOVAL:
        return MAINTENANCE_TASK_ORPHAN_CLEANUP_MIN_INTERVAL_MS;
      default:
        throw new IllegalArgumentException(
            "No minIntervalMs config for built-in policy type: " + builtInType);
    }
  }

  /**
   * Resolves minimum interval: policy content, then server config entry (with code default).
   *
   * @param config gravitino server config
   * @param builtInType built-in policy type
   * @param policyMinIntervalMs value from effective policy content, or null
   * @return resolved interval in milliseconds
   */
  public static long resolve(
      Config config, Policy.BuiltInType builtInType, @Nullable Long policyMinIntervalMs) {
    Preconditions.checkNotNull(config, "config cannot be null");
    Preconditions.checkNotNull(builtInType, "builtInType cannot be null");
    if (policyMinIntervalMs != null) {
      return policyMinIntervalMs;
    }
    return config.get(forBuiltInType(builtInType));
  }
}
