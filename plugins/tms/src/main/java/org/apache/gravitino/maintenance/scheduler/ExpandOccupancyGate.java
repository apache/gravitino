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

/**
 * Gate used by expand before inserting a {@code cron-job} row.
 *
 * <p>Production expand wires {@link org.apache.gravitino.maintenance.occupancy.OccupancyGate} and
 * may also skip when a pending {@code cron-job} row already covers the same table/policy pair.
 */
@FunctionalInterface
public interface ExpandOccupancyGate {

  /**
   * @param tableId table entity id
   * @param policyId policy entity id
   * @param minIntervalMs resolved cooldown for the policy type
   * @param nowMs current epoch millis
   * @return true when expand must not enqueue a cron-job for this pair
   */
  boolean shouldSkip(long tableId, long policyId, long minIntervalMs, long nowMs);
}
