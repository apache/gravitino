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
import org.apache.gravitino.policy.hook.PolicyLifecycleHook;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Keeps the expand-pool row aligned with a maintenance policy inside the policy transaction.
 *
 * <p>An enabled maintenance policy with a non-blank {@code timeTrigger} is scheduled. Disable, a
 * blank trigger, and a non-maintenance write remove the row. Delete removes the row by policy id.
 * An invalid cron throws on write so the policy change rolls back; on startup reconcile the expand
 * row is removed so scheduler state is corrected while the bad policy content remains to be edited.
 */
public final class PolicyExpandLifecycleHook implements PolicyLifecycleHook {

  private static final Logger LOG = LoggerFactory.getLogger(PolicyExpandLifecycleHook.class);

  private final PolicyExpandRegistrar registrar;

  /**
   * @param registrar registers policy-expand task rows on the current SQL session
   */
  public PolicyExpandLifecycleHook(PolicyExpandRegistrar registrar) {
    this.registrar = registrar;
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
    registrar.remove(policyId);
  }

  /**
   * Aligns the {@code policy-expand} task for a stored policy (startup reconcile).
   *
   * <p>Only mutates expand tasks: inserts when missing, reschedules when the due time is wrong,
   * removes the expand task when the policy must not run or the cron is invalid. Never deletes the
   * policy itself.
   *
   * @param policy persisted policy
   */
  public void align(PolicyEntity policy) {
    if (!isMaintenance(policy.content())) {
      registrar.remove(policy.id());
      return;
    }
    String trigger = timeTrigger(policy.content());
    if (!policy.enabled() || trigger == null || trigger.isBlank()) {
      registrar.remove(policy.id());
      return;
    }
    try {
      // Insert if missing; WHEN_EXISTS_RESCHEDULE corrects a stale due time.
      registrar.sync(policy.id(), true, trigger);
    } catch (IllegalArgumentException e) {
      registrar.remove(policy.id());
      LOG.error(
          "Cleared policy-expand task for policy id {} name {} with invalid timeTrigger; fix the policy content",
          policy.id(),
          policy.name(),
          e);
    }
  }

  private void syncExpandRow(PolicyEntity policy) {
    if (!isMaintenance(policy.content())) {
      registrar.remove(policy.id());
      return;
    }
    registrar.sync(policy.id(), policy.enabled(), timeTrigger(policy.content()));
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
