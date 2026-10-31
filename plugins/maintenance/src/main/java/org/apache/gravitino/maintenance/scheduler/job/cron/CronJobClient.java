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
package org.apache.gravitino.maintenance.scheduler.job.cron;

import com.github.kagkarlsson.scheduler.SchedulerClient;
import com.github.kagkarlsson.scheduler.serializer.JacksonSerializer;
import com.google.common.base.Preconditions;
import java.time.Instant;
import java.util.List;
import javax.sql.DataSource;
import org.apache.gravitino.Config;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.maintenance.scheduler.AutocommitDataSources;
import org.apache.gravitino.maintenance.scheduler.TableMaintenanceSchedulerPools;
import org.apache.gravitino.maintenance.scheduler.policy.expand.ExpandTableCandidate;
import org.apache.gravitino.meta.PolicyEntity;

/**
 * Submits one-shot {@code cron-job} rows to db-scheduler. Schedule planning is {@link
 * CronJobPlanner}.
 *
 * <p>One instance is held by the expand path for the life of the scheduler pools. Prefer {@link
 * #forEntityStore(Config)} when the caller does not already have the entity-store pool.
 */
public class CronJobClient {

  private final SchedulerClient scheduler;
  private final CronJobPlanner planner;

  /**
   * Opens a client against the entity-store pool (SchedulerClient uses autocommit wrap).
   *
   * @param gravitinoConfig server config for jobConf overlay
   * @return client bound to the cron-job table
   */
  public static CronJobClient forEntityStore(Config gravitinoConfig) {
    return new CronJobClient(AutocommitDataSources.entityStore(), gravitinoConfig);
  }

  /**
   * @param dataSource relational JDBC pool shared with the entity store
   * @param gravitinoConfig server config for jobConf overlay
   */
  public CronJobClient(DataSource dataSource, Config gravitinoConfig) {
    Preconditions.checkNotNull(dataSource);
    this.planner = new CronJobPlanner(Preconditions.checkNotNull(gravitinoConfig));
    this.scheduler = schedulerClient(dataSource);
  }

  /**
   * Plans and submits due-now cron jobs for the given expand candidates.
   *
   * @param policy effective maintenance policy
   * @param candidates schedulable tables with winner levels
   * @param batchCapacity max candidates considered for non-snapshot types
   * @return number of newly inserted cron-job rows
   */
  public int scheduleNow(
      PolicyEntity policy, List<ExpandTableCandidate> candidates, int batchCapacity) {
    return planner.plan(scheduler, policy, candidates, batchCapacity, Instant.now());
  }

  /**
   * @param tableId table entity id
   * @param policyId policy entity id
   * @return true when a queued cron-job already covers the table
   */
  public boolean isTableAlreadyQueued(long tableId, long policyId) {
    return planner.isTableAlreadyQueued(scheduler, tableId, policyId);
  }

  /**
   * @param dataSource entity-store pool
   * @return client bound to the cron-job table
   */
  public static SchedulerClient schedulerClient(DataSource dataSource) {
    return SchedulerClient.Builder.create(
            AutocommitDataSources.wrap(dataSource), TableMaintenanceSchedulerPools.cronJobTask())
        .serializer(new JacksonSerializer(JsonUtils.anyFieldMapper()))
        .tableName(TableMaintenanceSchedulerPools.CRON_TASKS_TABLE)
        .build();
  }
}
