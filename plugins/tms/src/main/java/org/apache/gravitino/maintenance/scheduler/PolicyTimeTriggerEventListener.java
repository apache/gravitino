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
package org.apache.gravitino.maintenance.scheduler;

import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.listener.api.EventListenerPlugin;
import org.apache.gravitino.listener.api.event.Event;
import org.apache.gravitino.listener.api.event.PreEvent;
import org.apache.gravitino.listener.api.event.policy.AlterPolicyEvent;
import org.apache.gravitino.listener.api.event.policy.CreatePolicyEvent;
import org.apache.gravitino.listener.api.event.policy.DeletePolicyEvent;
import org.apache.gravitino.listener.api.event.policy.DeletePolicyFailureEvent;
import org.apache.gravitino.listener.api.event.policy.DeletePolicyPreEvent;
import org.apache.gravitino.listener.api.event.policy.DisablePolicyEvent;
import org.apache.gravitino.listener.api.event.policy.EnablePolicyEvent;
import org.apache.gravitino.maintenance.policy.MaintenancePolicyDefinitions;
import org.apache.gravitino.policy.ExecutablePolicyContent;
import org.apache.gravitino.policy.Policy;
import org.apache.gravitino.policy.PolicyContent;

/**
 * Keeps the expand-pool row aligned with maintenance policies that carry {@code timeTrigger}.
 *
 * <p>Create, alter, enable, and disable reload the persisted policy and insert, move, or delete the
 * row. Delete snapshots the policy id on the pre-event, because the post-event has no content, and
 * removes the row only after a successful delete. Disable deletes the row so a disabled policy does
 * not stay scheduled.
 *
 * <p>The schedule supplier may return {@code null} while TMS pools are stopped. Events in that
 * window are ignored.
 */
public final class PolicyTimeTriggerEventListener implements EventListenerPlugin {

  private final Supplier<PolicyExpandSchedule> schedules;
  private final PolicyLoader policyLoader;
  private final ThreadLocal<Long> pendingDeletePolicyId = new ThreadLocal<>();

  /**
   * @param schedules live expand schedule, or {@code null} when TMS scheduling is stopped
   * @param policyLoader loads the persisted policy id, enabled flag, and content
   */
  public PolicyTimeTriggerEventListener(
      Supplier<PolicyExpandSchedule> schedules, PolicyLoader policyLoader) {
    this.schedules = schedules;
    this.policyLoader = policyLoader;
  }

  @Override
  public void init(Map<String, String> properties) {}

  @Override
  public void start() {}

  @Override
  public void stop() {}

  @Override
  public void onPreEvent(PreEvent preEvent) {
    if (!(preEvent instanceof DeletePolicyPreEvent)) {
      return;
    }
    pendingDeletePolicyId.remove();
    load(preEvent.identifier())
        .filter(snapshot -> isMaintenance(snapshot.content()))
        .ifPresent(snapshot -> pendingDeletePolicyId.set(snapshot.id()));
  }

  @Override
  public void onPostEvent(Event postEvent) {
    try {
      if (postEvent instanceof CreatePolicyEvent
          || postEvent instanceof AlterPolicyEvent
          || postEvent instanceof EnablePolicyEvent
          || postEvent instanceof DisablePolicyEvent) {
        reconcile(postEvent.identifier());
      } else if (postEvent instanceof DeletePolicyEvent) {
        Long policyId = pendingDeletePolicyId.get();
        if (policyId != null && ((DeletePolicyEvent) postEvent).isExists()) {
          remove(policyId);
        }
      }
    } finally {
      if (postEvent instanceof DeletePolicyEvent || postEvent instanceof DeletePolicyFailureEvent) {
        pendingDeletePolicyId.remove();
      }
    }
  }

  private void reconcile(NameIdentifier identifier) {
    PolicyExpandSchedule schedule = schedules.get();
    if (schedule == null) {
      return;
    }
    load(identifier)
        .filter(snapshot -> isMaintenance(snapshot.content()))
        .ifPresent(
            snapshot ->
                schedule.sync(snapshot.id(), snapshot.enabled(), timeTrigger(snapshot.content())));
  }

  private void remove(long policyId) {
    PolicyExpandSchedule schedule = schedules.get();
    if (schedule != null) {
      schedule.remove(policyId);
    }
  }

  private Optional<PolicySnapshot> load(NameIdentifier identifier) {
    if (identifier == null
        || identifier.name() == null
        || identifier.namespace() == null
        || identifier.namespace().levels().length == 0) {
      return Optional.empty();
    }
    return policyLoader.load(identifier.namespace().level(0), identifier.name());
  }

  private static boolean isMaintenance(PolicyContent content) {
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
  private static String timeTrigger(PolicyContent content) {
    if (!(content instanceof ExecutablePolicyContent)) {
      return null;
    }
    return ((ExecutablePolicyContent) content).executionSpec().timeTrigger();
  }

  /** Loads a persisted policy for schedule registration. */
  @FunctionalInterface
  public interface PolicyLoader {

    /**
     * @param metalake metalake that owns the policy
     * @param policyName policy name
     * @return the persisted snapshot, or empty when the policy is already gone
     */
    Optional<PolicySnapshot> load(String metalake, String policyName);
  }

  /** Id, enabled flag, and content needed to register an expand row. */
  public static final class PolicySnapshot {
    private final long id;
    private final boolean enabled;
    private final PolicyContent content;

    /**
     * @param id persisted policy id
     * @param enabled whether the policy is enabled
     * @param content policy content, including {@code timeTrigger} when present
     */
    public PolicySnapshot(long id, boolean enabled, PolicyContent content) {
      this.id = id;
      this.enabled = enabled;
      this.content = content;
    }

    /**
     * @return persisted policy id
     */
    public long id() {
      return id;
    }

    /**
     * @return whether the policy is enabled
     */
    public boolean enabled() {
      return enabled;
    }

    /**
     * @return policy content
     */
    public PolicyContent content() {
      return content;
    }
  }
}
