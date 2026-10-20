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
package org.apache.gravitino.maintenance.scheduler.cron;

import com.google.common.base.Preconditions;
import javax.sql.DataSource;
import org.apache.gravitino.maintenance.scheduler.ExpandOccupancyGate;

/**
 * Decorates an {@link ExpandOccupancyGate} so tables already covered by a queued {@code cron-job}
 * row are skipped and do not consume the expand batch budget.
 */
public final class PendingCronExpandOccupancyGate implements ExpandOccupancyGate {

  private final ExpandOccupancyGate delegate;
  private final DataSource dataSource;

  /**
   * @param delegate in-flight / minInterval gate
   * @param dataSource entity-store JDBC pool used by the cron scheduler table
   */
  public PendingCronExpandOccupancyGate(ExpandOccupancyGate delegate, DataSource dataSource) {
    this.delegate = Preconditions.checkNotNull(delegate, "delegate");
    this.dataSource = Preconditions.checkNotNull(dataSource, "dataSource");
  }

  @Override
  public boolean shouldSkip(long tableId, long policyId, long minIntervalMs, long nowMs) {
    if (delegate.shouldSkip(tableId, policyId, minIntervalMs, nowMs)) {
      return true;
    }
    return RelationalCronJobScheduler.isQueuedForPolicy(
        RelationalCronJobScheduler.client(dataSource), tableId, policyId);
  }
}
