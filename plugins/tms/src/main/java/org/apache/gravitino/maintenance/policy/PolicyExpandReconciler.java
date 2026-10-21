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

import org.apache.gravitino.meta.BaseMetalake;
import org.apache.gravitino.meta.PolicyEntity;
import org.apache.gravitino.storage.relational.service.MetalakeMetaService;
import org.apache.gravitino.storage.relational.service.PolicyMetaService;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.apache.gravitino.utils.NamespaceUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Backfills {@code policy-expand} rows for policies that already exist when TMS starts.
 *
 * <p>Lifecycle hooks only run on later policy writes. Upgrade and first-enable paths need a scan so
 * enabled maintenance policies with a {@code timeTrigger} get a matching expand row without an
 * edit.
 */
public final class PolicyExpandReconciler {

  private static final Logger LOG = LoggerFactory.getLogger(PolicyExpandReconciler.class);

  private final PolicyExpandLifecycleHook aligner;
  private final MetalakeMetaService metalakeMetaService;
  private final PolicyMetaService policyMetaService;

  /**
   * @param aligner same expand rules as the lifecycle hook; must use {@link
   *     PolicyExpandRegistrar#joiningCurrentTransaction()} so each align commits with {@link
   *     SessionUtils#doMultipleWithCommit}
   */
  public PolicyExpandReconciler(PolicyExpandLifecycleHook aligner) {
    this(aligner, MetalakeMetaService.getInstance(), PolicyMetaService.getInstance());
  }

  /**
   * @param aligner expand aligner
   * @param metalakeMetaService metalake listing
   * @param policyMetaService policy listing
   */
  public PolicyExpandReconciler(
      PolicyExpandLifecycleHook aligner,
      MetalakeMetaService metalakeMetaService,
      PolicyMetaService policyMetaService) {
    this.aligner = aligner;
    this.metalakeMetaService = metalakeMetaService;
    this.policyMetaService = policyMetaService;
  }

  /**
   * Scans every metalake and aligns expand rows. A bad cron on one policy is logged and skipped so
   * TMS still starts.
   *
   * @return number of policies successfully aligned
   */
  public int reconcileAll() {
    int aligned = 0;
    for (BaseMetalake metalake : metalakeMetaService.listMetalakes()) {
      for (PolicyEntity policy :
          policyMetaService.listPoliciesByNamespace(NamespaceUtil.ofPolicy(metalake.name()))) {
        try {
          SessionUtils.doMultipleWithCommit(() -> aligner.align(policy));
          aligned++;
        } catch (RuntimeException e) {
          LOG.warn(
              "Skipping expand reconcile for policy {} under metalake {}: {}",
              policy.name(),
              metalake.name(),
              e.toString());
        }
      }
    }
    LOG.info("Reconciled expand rows for {} policies", aligned);
    return aligned;
  }
}
