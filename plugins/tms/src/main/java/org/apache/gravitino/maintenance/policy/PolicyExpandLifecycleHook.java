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
import org.apache.gravitino.meta.PolicyEntity;
import org.apache.gravitino.policy.ExecutablePolicyContent;
import org.apache.gravitino.policy.Policy;
import org.apache.gravitino.policy.PolicyContent;
import org.apache.gravitino.policy.PolicyLifecycleHook;

/**
 * Keeps the expand-pool row aligned with a maintenance policy inside the policy transaction.
 *
 * <p>An enabled maintenance policy with a non-blank {@code timeTrigger} is scheduled. Disable, a
 * blank trigger, and a non-maintenance write remove the row. Delete removes the row by policy id.
 * An invalid cron throws, so the policy write rolls back with the expand change.
 */
public final class PolicyExpandLifecycleHook implements PolicyLifecycleHook {

  private final PolicyExpandRowWriter rowWriter;

  /**
   * @param rowWriter expand-row writer bound to the current SQL session
   */
  public PolicyExpandLifecycleHook(PolicyExpandRowWriter rowWriter) {
    this.rowWriter = rowWriter;
  }

  @Override
  public void afterPersist(PolicyEntity policy) {
    syncExpandRow(policy);
  }

  @Override
  public void afterUpdate(PolicyEntity policy) {
    syncExpandRow(policy);
  }

  @Override
  public void beforeRemove(long policyId) {
    rowWriter.remove(policyId);
  }

  private void syncExpandRow(PolicyEntity policy) {
    if (!isMaintenance(policy.content())) {
      rowWriter.remove(policy.id());
      return;
    }
    rowWriter.sync(policy.id(), policy.enabled(), timeTrigger(policy.content()));
  }

  private static boolean isMaintenance(@Nullable PolicyContent content) {
    if (content == null) {
      return false;
    }
    for (Policy.BuiltInType type : Policy.BuiltInType.values()) {
      if (MaintenancePolicyDefinitions.isMaintenance(type)
          && type.contentClass().isInstance(content)) {
        return true;
      }
    }
    return false;
  }

  @Nullable
  private static String timeTrigger(@Nullable PolicyContent content) {
    if (!(content instanceof ExecutablePolicyContent)) {
      return null;
    }
    return ((ExecutablePolicyContent) content).executionSpec().timeTrigger();
  }
}
