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
import org.apache.gravitino.maintenance.scheduler.policy.expand.PolicyExpandClient;
import org.apache.gravitino.meta.PolicyEntity;
import org.apache.gravitino.policy.hook.PolicyLifecycleHook;

/**
 * Forwards policy create / alter / delete onto {@link PolicyExpandClient} inside the policy
 * transaction.
 *
 * <p>{@link PolicyExpandClient#sync} schedules or removes the expand row from policy content.
 * Delete removes the row by policy id. Invalid cron on write throws so the policy change rolls
 * back.
 */
public final class PolicyExpandLifecycleHook implements PolicyLifecycleHook {

  private final PolicyExpandClient expandClient;

  /**
   * @param expandClient writes policy-expand rows on the current SQL session
   */
  public PolicyExpandLifecycleHook(PolicyExpandClient expandClient) {
    this.expandClient = Preconditions.checkNotNull(expandClient);
  }

  @Override
  public void afterPersist(PolicyEntity policy) {
    expandClient.sync(policy);
  }

  @Override
  public void afterUpdate(PolicyEntity policy) {
    expandClient.sync(policy);
  }

  @Override
  public void beforeRemove(long policyId) {
    expandClient.remove(policyId);
  }
}
