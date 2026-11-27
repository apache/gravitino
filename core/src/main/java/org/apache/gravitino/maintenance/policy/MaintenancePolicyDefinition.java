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
import org.apache.gravitino.config.ConfigEntry;
import org.apache.gravitino.policy.Policy;

/**
 * Per-type maintenance metadata keyed by {@link Policy.BuiltInType}.
 *
 * <p>{@link Policy.BuiltInType} is the single policy/activity identity (not a db-scheduler task
 * kind). On-write server defaults for {@code minIntervalMs} are looked up from {@link
 * MaintenancePolicyDefinitions} rather than parallel switches or a second activity enum. Types that
 * cannot use On write (orphan cleanup) have a null cooldown entry.
 */
public final class MaintenancePolicyDefinition {

  private final Policy.BuiltInType builtInType;
  @Nullable private final ConfigEntry<Long> minIntervalMs;

  MaintenancePolicyDefinition(
      Policy.BuiltInType builtInType, @Nullable ConfigEntry<Long> minIntervalMs) {
    this.builtInType = Preconditions.checkNotNull(builtInType, "builtInType cannot be null");
    this.minIntervalMs = minIntervalMs;
  }

  /**
   * @return built-in policy type identity
   */
  public Policy.BuiltInType builtInType() {
    return builtInType;
  }

  /**
   * @return server config entry for On-write cooldown when policy content omits {@code
   *     minIntervalMs}, or {@code null} when this type has no cooldown config
   */
  @Nullable
  public ConfigEntry<Long> minIntervalMs() {
    return minIntervalMs;
  }

  /**
   * @return {@code gravitino.conf} key for this type's On-write cooldown
   * @throws IllegalStateException if this type has no cooldown config
   */
  public String minIntervalMsKey() {
    Preconditions.checkState(
        minIntervalMs != null, "No On-write minIntervalMs config for %s", builtInType);
    return minIntervalMs.getKey();
  }
}
