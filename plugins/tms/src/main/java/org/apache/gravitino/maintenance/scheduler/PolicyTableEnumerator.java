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

import java.util.List;
import org.apache.gravitino.meta.PolicyEntity;

/**
 * Lists enqueueable tables for a maintenance policy expand pick.
 *
 * <p>{@code limit} is the max number of enqueueable tables after nearest-policy and occupancy
 * gating. Each candidate carries the winner attachment level for snapshot-expiry batching.
 */
@FunctionalInterface
public interface PolicyTableEnumerator {

  /**
   * @param policy loaded policy
   * @param limit maximum enqueueable tables to return
   * @param occupancyGate skip in-flight / cooldown tables while scanning
   * @param minIntervalMs resolved cooldown for {@link ExpandOccupancyGate#shouldSkip}
   * @return distinct enqueueable candidates, size at most {@code limit}
   */
  List<ExpandTableCandidate> listTableIds(
      PolicyEntity policy, int limit, ExpandOccupancyGate occupancyGate, long minIntervalMs);
}
