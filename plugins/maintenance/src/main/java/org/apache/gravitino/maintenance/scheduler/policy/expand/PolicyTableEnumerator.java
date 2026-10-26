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
package org.apache.gravitino.maintenance.scheduler.policy.expand;

import java.util.List;
import org.apache.gravitino.meta.PolicyEntity;

/**
 * Lists enqueueable tables that a maintenance policy should expand into cron jobs.
 *
 * <p>Implementations expand tag-bound catalog/schema/table attachments and keep tables for which
 * the policy is the nearest winner of its {@link org.apache.gravitino.policy.Policy.BuiltInType}.
 * {@code limit} is the max number of <em>enqueueable</em> tables after occupancy / cooldown gating.
 * Each candidate carries the winner attachment level for snapshot-expiry batching.
 */
@FunctionalInterface
public interface PolicyTableEnumerator {

  /**
   * Lists up to {@code limit} tables that are nearest-policy winners and pass {@code
   * occupancyGate}.
   *
   * @param policy loaded policy
   * @param limit maximum enqueueable candidates to return
   * @param occupancyGate skip in-flight / cooldown / already-queued tables while scanning
   * @param minIntervalMs resolved cooldown for {@link ExpandOccupancyGate#shouldSkip}
   * @return distinct enqueueable candidates, size at most {@code limit}
   */
  List<ExpandTableCandidate> listTableIds(
      PolicyEntity policy, int limit, ExpandOccupancyGate occupancyGate, long minIntervalMs);
}
