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
import java.util.Collection;
import java.util.EnumMap;
import java.util.Map;
import org.apache.gravitino.config.ConfigEntry;
import org.apache.gravitino.policy.Policy;

/**
 * Core registry of Iceberg maintenance policy definitions.
 *
 * <p>Keyed only by {@link Policy.BuiltInType} (policy / activity identity). This is
 * <strong>not</strong> a scheduler task-kind registry — {@code policy-expand} / {@code cron-job} /
 * {@code commit-job} stay separate. Non-maintenance built-ins ({@code ROW_FILTER}, {@code
 * COLUMN_MASK}, {@code CUSTOM}) are absent from this map.
 */
public final class MaintenancePolicyDefinitions {

  private static final Map<Policy.BuiltInType, MaintenancePolicyDefinition> BY_TYPE;

  static {
    EnumMap<Policy.BuiltInType, MaintenancePolicyDefinition> map =
        new EnumMap<>(Policy.BuiltInType.class);
    register(
        map,
        Policy.BuiltInType.ICEBERG_COMPACTION,
        MaintenanceMinIntervalConfigs.COMPACTION_MIN_INTERVAL_MS);
    register(
        map,
        Policy.BuiltInType.ICEBERG_REWRITE_MANIFESTS,
        MaintenanceMinIntervalConfigs.MANIFEST_REWRITE_MIN_INTERVAL_MS);
    register(
        map,
        Policy.BuiltInType.ICEBERG_SNAPSHOT_EXPIRATION,
        MaintenanceMinIntervalConfigs.SNAPSHOT_EXPIRY_MIN_INTERVAL_MS);
    register(
        map,
        Policy.BuiltInType.ICEBERG_ORPHAN_FILE_REMOVAL,
        MaintenanceMinIntervalConfigs.ORPHAN_CLEANUP_MIN_INTERVAL_MS);
    BY_TYPE = ImmutableMap.copyOf(map);
  }

  private MaintenancePolicyDefinitions() {}

  /**
   * Returns the maintenance definition for a built-in type.
   *
   * @param builtInType built-in policy type
   * @return definition
   * @throws IllegalArgumentException if the type is not a registered maintenance policy
   */
  public static MaintenancePolicyDefinition require(Policy.BuiltInType builtInType) {
    Preconditions.checkNotNull(builtInType, "builtInType cannot be null");
    MaintenancePolicyDefinition definition = BY_TYPE.get(builtInType);
    Preconditions.checkArgument(
        definition != null, "No maintenance definition for built-in policy type: %s", builtInType);
    return definition;
  }

  /**
   * @return all registered maintenance definitions
   */
  public static Collection<MaintenancePolicyDefinition> all() {
    return BY_TYPE.values();
  }

  /**
   * @return whether {@code builtInType} is a registered Iceberg maintenance policy
   */
  public static boolean isMaintenance(Policy.BuiltInType builtInType) {
    return builtInType != null && BY_TYPE.containsKey(builtInType);
  }

  private static void register(
      EnumMap<Policy.BuiltInType, MaintenancePolicyDefinition> map,
      Policy.BuiltInType builtInType,
      ConfigEntry<Long> minIntervalMs) {
    map.put(builtInType, new MaintenancePolicyDefinition(builtInType, minIntervalMs));
  }
}
