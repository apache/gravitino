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
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.gravitino.meta.PolicyEntity;

/** Effective TMS policy content for a table and maintenance task type. */
public final class EffectiveTableMaintenancePolicy {

  private final PolicyEntity policy;
  private final TableMaintenanceTaskType taskType;
  @Nullable private final TableMaintenancePolicyFields maintenanceFields;
  private final long minIntervalMs;

  /**
   * Creates an effective policy view.
   *
   * @param policy nearest policy entity
   * @param taskType maintenance task type
   * @param maintenanceFields parsed TMS fields from policy content
   * @param minIntervalMs resolved minimum interval
   */
  public EffectiveTableMaintenancePolicy(
      PolicyEntity policy,
      TableMaintenanceTaskType taskType,
      @Nullable TableMaintenancePolicyFields maintenanceFields,
      long minIntervalMs) {
    this.policy = Preconditions.checkNotNull(policy, "policy");
    this.taskType = Preconditions.checkNotNull(taskType, "taskType");
    this.maintenanceFields = maintenanceFields;
    this.minIntervalMs = minIntervalMs;
  }

  /**
   * @return nearest policy entity
   */
  public PolicyEntity policy() {
    return policy;
  }

  /**
   * @return maintenance task type
   */
  public TableMaintenanceTaskType taskType() {
    return taskType;
  }

  /**
   * @return TMS fields from policy content, or null
   */
  @Nullable
  public TableMaintenancePolicyFields maintenanceFields() {
    return maintenanceFields;
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
   * @param taskType task type
   * @param serverConfig server configuration map
   * @return effective policy when a matching policy exists
   */
  public static java.util.Optional<EffectiveTableMaintenancePolicy> resolve(
      NearestMaintenancePolicyResolver resolver,
      String metalake,
      org.apache.gravitino.MetadataObject metadataObject,
      TableMaintenanceTaskType taskType,
      Map<String, String> serverConfig) {
    return resolver
        .resolveNearest(metalake, metadataObject, taskType)
        .map(
            policy -> {
              TableMaintenancePolicyFields fields =
                  TableMaintenancePolicyContentSupport.maintenanceFields(policy.content());
              Long policyInterval = fields == null ? null : fields.minIntervalMs();
              long interval = MinIntervalMsResolver.resolve(taskType, policyInterval, serverConfig);
              return new EffectiveTableMaintenancePolicy(policy, taskType, fields, interval);
            });
  }
}
