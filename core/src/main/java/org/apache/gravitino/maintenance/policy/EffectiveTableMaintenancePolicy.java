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
import java.util.Optional;
import javax.annotation.Nullable;
import org.apache.gravitino.Config;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.meta.PolicyEntity;
import org.apache.gravitino.policy.Policy;
import org.apache.gravitino.policy.PolicyContent;

/**
 * Effective scheduling view for a table and built-in maintenance policy type.
 *
 * <p>Carries the nearest policy plus execution triggers and resolved {@code minIntervalMs} used by
 * the TMS scheduler. Job submit options stay on the policy content and are read at {@code runJob}
 * time.
 */
public final class EffectiveTableMaintenancePolicy {

  private final PolicyEntity policy;
  private final Policy.BuiltInType builtInType;
  @Nullable private final String timeTrigger;
  @Nullable private final Boolean commitTrigger;
  private final long minIntervalMs;

  /**
   * Creates an effective policy view.
   *
   * @param policy nearest policy entity
   * @param builtInType built-in maintenance policy type
   * @param timeTrigger cron expression from policy content, or null when unset
   * @param commitTrigger commit-trigger flag from policy content, or null when unset
   * @param minIntervalMs resolved minimum interval
   */
  public EffectiveTableMaintenancePolicy(
      PolicyEntity policy,
      Policy.BuiltInType builtInType,
      @Nullable String timeTrigger,
      @Nullable Boolean commitTrigger,
      long minIntervalMs) {
    this.policy = Preconditions.checkNotNull(policy, "policy");
    this.builtInType = Preconditions.checkNotNull(builtInType, "builtInType");
    this.timeTrigger = timeTrigger;
    this.commitTrigger = commitTrigger;
    this.minIntervalMs = minIntervalMs;
  }

  /**
   * @return nearest policy entity
   */
  public PolicyEntity policy() {
    return policy;
  }

  /**
   * @return built-in maintenance policy type
   */
  public Policy.BuiltInType builtInType() {
    return builtInType;
  }

  /**
   * @return cron expression from policy content, or null when unset
   */
  @Nullable
  public String timeTrigger() {
    return timeTrigger;
  }

  /**
   * @return commit-trigger flag from policy content, or null when unset
   */
  @Nullable
  public Boolean commitTrigger() {
    return commitTrigger;
  }

  /**
   * @return resolved minimum interval between runs
   */
  public long minIntervalMs() {
    return minIntervalMs;
  }

  /**
   * Resolves effective TMS policy for a metadata object.
   *
   * @param resolver nearest-policy resolver
   * @param metalake metalake name
   * @param metadataObject metadata object
   * @param builtInType built-in maintenance policy type
   * @param config gravitino server configuration
   * @return effective policy when a matching policy exists
   */
  public static Optional<EffectiveTableMaintenancePolicy> resolve(
      NearestMaintenancePolicyResolver resolver,
      String metalake,
      MetadataObject metadataObject,
      Policy.BuiltInType builtInType,
      Config config) {
    return resolver
        .resolveNearest(metalake, metadataObject, builtInType)
        .map(
            policy -> {
              PolicyContent content = policy.content();
              long interval =
                  MaintenanceMinIntervalConfigs.resolve(
                      config, builtInType, content.minIntervalMs());
              return new EffectiveTableMaintenancePolicy(
                  policy, builtInType, content.timeTrigger(), content.commitTrigger(), interval);
            });
  }
}
