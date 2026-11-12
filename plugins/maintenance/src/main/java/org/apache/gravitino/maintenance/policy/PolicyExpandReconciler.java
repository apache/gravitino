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
import java.util.HashSet;
import java.util.Set;
import org.apache.gravitino.maintenance.scheduler.policy.expand.PolicyExpandClient;
import org.apache.gravitino.meta.BaseMetalake;
import org.apache.gravitino.meta.PolicyEntity;
import org.apache.gravitino.storage.relational.service.MetalakeMetaService;
import org.apache.gravitino.storage.relational.service.PolicyMetaService;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.apache.gravitino.utils.NamespaceUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reconciles {@code policy-expand} tasks when starts.
 *
 * <p>Only {@code scheduled_expand_tasks} rows are touched — never policy rows or other scheduler
 * tables. Lifecycle hooks cover later writes; startup must still converge existing state:
 *
 * <ul>
 *   <li><b>missing</b> — enabled maintenance policy with a valid {@code timeTrigger} but no expand
 *       task → insert the next due after now
 *   <li><b>wrong future due</b> — expand due is still in the future but does not match the policy
 *       cron → reschedule; invalid cron → remove that expand task only
 *   <li><b>overdue</b> — expand due is before now → leave unchanged so the pool can catch up
 *   <li><b>surplus</b> — expand task for disabled / blank / non-maintenance / deleted policy →
 *       delete that expand task only
 * </ul>
 */
public final class PolicyExpandReconciler {

  private static final Logger LOG = LoggerFactory.getLogger(PolicyExpandReconciler.class);

  private final PolicyExpandClient expandClient;
  private final MetalakeMetaService metalakeMetaService;
  private final PolicyMetaService policyMetaService;

  /**
   * @param expandClient expand writer used for {@link PolicyExpandClient#align} and orphan cleanup
   * @param metalakeMetaService metalake listing
   * @param policyMetaService policy listing
   */
  public PolicyExpandReconciler(
      PolicyExpandClient expandClient,
      MetalakeMetaService metalakeMetaService,
      PolicyMetaService policyMetaService) {
    this.expandClient = Preconditions.checkNotNull(expandClient);
    this.metalakeMetaService = Preconditions.checkNotNull(metalakeMetaService);
    this.policyMetaService = Preconditions.checkNotNull(policyMetaService);
  }

  /**
   * For each policy, insert or correct its expand task; then delete orphan expand tasks.
   *
   * @return number of policies walked
   */
  public int reconcileAll() {
    Set<Long> knownPolicyIds = new HashSet<>();
    int aligned = 0;
    // Missing → insert; wrong due / invalid cron → correct or clear expand task only.
    for (BaseMetalake metalake : metalakeMetaService.listMetalakes()) {
      for (PolicyEntity policy :
          policyMetaService.listPoliciesByNamespace(NamespaceUtil.ofPolicy(metalake.name()))) {
        knownPolicyIds.add(policy.id());
        SessionUtils.doMultipleWithCommit(() -> expandClient.align(policy));
        aligned++;
      }
    }
    // Surplus expand tasks with no policy left → delete expand only.
    int[] removedOrphans = {0};
    SessionUtils.doMultipleWithCommit(
        () -> {
          for (long scheduledId : expandClient.scheduledPolicyIds()) {
            if (!knownPolicyIds.contains(scheduledId)) {
              expandClient.remove(scheduledId);
              removedOrphans[0]++;
            }
          }
        });
    LOG.info(
        "Reconciled expand tasks for {} policies; removed {} orphan expand tasks",
        aligned,
        removedOrphans[0]);
    return aligned;
  }
}
