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

import java.util.HashSet;
import java.util.Set;
import org.apache.gravitino.meta.BaseMetalake;
import org.apache.gravitino.meta.PolicyEntity;
import org.apache.gravitino.storage.relational.service.MetalakeMetaService;
import org.apache.gravitino.storage.relational.service.PolicyMetaService;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.apache.gravitino.utils.NamespaceUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reconciles {@code policy-expand} rows when TMS starts.
 *
 * <p>Lifecycle hooks only run on later policy writes. Startup walks stored policies and the expand
 * table so upgrade / first-enable paths converge without an edit:
 *
 * <ul>
 *   <li>redundant expand rows (disabled, blank trigger, non-maintenance, or orphan) are deleted
 *   <li>valid schedules are upserted to the next due time
 *   <li>invalid cron clears the expand row (scheduler fixed; policy content still needs an edit)
 * </ul>
 */
public final class PolicyExpandReconciler {

  private static final Logger LOG = LoggerFactory.getLogger(PolicyExpandReconciler.class);

  private final PolicyExpandLifecycleHook aligner;
  private final PolicyExpandRegistrar registrar;
  private final MetalakeMetaService metalakeMetaService;
  private final PolicyMetaService policyMetaService;

  /**
   * @param aligner same expand rules as the lifecycle hook
   * @param registrar writer used for orphan cleanup; both must use {@link
   *     PolicyExpandRegistrar#joiningCurrentTransaction()}
   */
  public PolicyExpandReconciler(
      PolicyExpandLifecycleHook aligner, PolicyExpandRegistrar registrar) {
    this(aligner, registrar, MetalakeMetaService.getInstance(), PolicyMetaService.getInstance());
  }

  /**
   * @param aligner expand aligner
   * @param registrar expand registrar for orphan deletes
   * @param metalakeMetaService metalake listing
   * @param policyMetaService policy listing
   */
  public PolicyExpandReconciler(
      PolicyExpandLifecycleHook aligner,
      PolicyExpandRegistrar registrar,
      MetalakeMetaService metalakeMetaService,
      PolicyMetaService policyMetaService) {
    this.aligner = aligner;
    this.registrar = registrar;
    this.metalakeMetaService = metalakeMetaService;
    this.policyMetaService = policyMetaService;
  }

  /**
   * Aligns every stored policy, then deletes expand rows that no longer have a policy.
   *
   * @return number of policies walked
   */
  public int reconcileAll() {
    Set<Long> knownPolicyIds = new HashSet<>();
    int aligned = 0;
    for (BaseMetalake metalake : metalakeMetaService.listMetalakes()) {
      for (PolicyEntity policy :
          policyMetaService.listPoliciesByNamespace(NamespaceUtil.ofPolicy(metalake.name()))) {
        knownPolicyIds.add(policy.id());
        SessionUtils.doMultipleWithCommit(() -> aligner.align(policy));
        aligned++;
      }
    }
    int[] removedOrphans = {0};
    SessionUtils.doMultipleWithCommit(
        () -> {
          for (long scheduledId : registrar.scheduledPolicyIds()) {
            if (!knownPolicyIds.contains(scheduledId)) {
              registrar.remove(scheduledId);
              removedOrphans[0]++;
            }
          }
        });
    LOG.info(
        "Reconciled expand rows for {} policies; removed {} orphan expand rows",
        aligned,
        removedOrphans[0]);
    return aligned;
  }
}
