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

import javax.annotation.Nullable;
import org.apache.gravitino.policy.Policy;
import org.apache.gravitino.policy.PolicyContent;

/**
 * Maps built-in policy types to TMS task kinds.
 *
 * <p>Extracting {@link TableMaintenancePolicyFields} from concrete policy content is deferred until
 * those fields are attached to the four built-in content classes.
 */
public final class TableMaintenancePolicyContentSupport {

  private TableMaintenancePolicyContentSupport() {}

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
   * Extracts TMS fields from policy content when present.
   *
   * <p>Currently always returns {@code null}; content classes do not carry TMS fields yet.
   *
   * @param content policy content
   * @return TMS fields or {@code null}
   */
  @Nullable
  public static TableMaintenancePolicyFields maintenanceFields(PolicyContent content) {
    return null;
  }

  /**
   * Validates TMS fields on supported policy content.
   *
   * @param policyType policy built-in type
   * @param content policy content
   */
  public static void validateMaintenanceFields(
      Policy.BuiltInType policyType, PolicyContent content) {
    if (content == null) {
      return;
    }
    TableMaintenanceTaskType taskType = taskTypeFor(policyType);
    if (taskType == null) {
      return;
    }
    TableMaintenancePolicyFields fields = maintenanceFields(content);
    if (fields != null) {
      fields.validate(taskType);
    }
  }
}
