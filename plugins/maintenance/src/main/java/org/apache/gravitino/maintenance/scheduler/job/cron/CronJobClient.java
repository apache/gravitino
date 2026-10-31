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
import com.github.kagkarlsson.scheduler.task.TaskInstance;
import com.google.common.base.Preconditions;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import javax.sql.DataSource;
import org.apache.gravitino.Config;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.maintenance.scheduler.AutocommitDataSources;
import org.apache.gravitino.maintenance.scheduler.MaintenanceJobSpec;
import org.apache.gravitino.maintenance.scheduler.MaintenanceJobSpecFactory;
import org.apache.gravitino.maintenance.scheduler.MaintenanceTaskIds;
import org.apache.gravitino.maintenance.scheduler.ResolvedTableRef;
import org.apache.gravitino.maintenance.scheduler.TableMaintenanceSchedulerPools;
import org.apache.gravitino.maintenance.scheduler.job.MaintenanceJobTemplates;
import org.apache.gravitino.maintenance.scheduler.policy.expand.ExpandTableCandidate;
import org.apache.gravitino.meta.PolicyEntity;
import org.apache.gravitino.policy.Policy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Enqueues one-shot {@code cron-job} rows. Snapshot-expiry table merge is handled by {@link
 * CronJobMerger}.
 */
public class CronJobClient {

  private static final Logger LOG = LoggerFactory.getLogger(CronJobClient.class);

  private final DataSource dataSource;
  private final MaintenanceJobSpecFactory specFactory;
  private final CronJobMerger merger;

  /**
   * @param dataSource relational JDBC pool shared with the entity store
   * @param gravitinoConfig server config for jobConf overlay
   */
  public CronJobClient(DataSource dataSource, Config gravitinoConfig) {
    this.dataSource = Preconditions.checkNotNull(dataSource);
    Preconditions.checkNotNull(gravitinoConfig);
    this.specFactory = new MaintenanceJobSpecFactory(gravitinoConfig);
    this.merger = new CronJobMerger(gravitinoConfig);
  }

  /**
   * Enqueues due-now cron jobs for the given expand candidates.
   *
   * @param policy effective maintenance policy
   * @param candidates enqueueable tables with winner levels
   * @param batchCapacity max candidates considered for non-snapshot types
   * @return number of newly inserted cron-job rows
   */
  public int enqueueNow(
      PolicyEntity policy, List<ExpandTableCandidate> candidates, int batchCapacity) {
    if (policy == null || candidates == null || candidates.isEmpty()) {
      return 0;
    }
    if (MaintenanceJobTemplates.templateName(policy.policyType()).isEmpty()) {
      LOG.warn(
          "Skipping cron-job enqueue for policy {} with unsupported type {}",
          policy.id(),
          policy.policyType());
      return 0;
    }
    SchedulerClient client = schedulerClient(dataSource);
    Instant due = Instant.now();
    if (policy.policyType() == Policy.BuiltInType.ICEBERG_SNAPSHOT_EXPIRATION) {
      return merger.merge(client, policy, candidates, due);
    }
    int scheduled = 0;
    int limit = Math.min(candidates.size(), Math.max(1, batchCapacity));
    for (ExpandTableCandidate candidate : candidates.subList(0, limit)) {
      Optional<MaintenanceJobSpec> spec = buildSpec(policy, List.of(candidate));
      if (spec.isEmpty()) {
        continue;
      }
      String id = MaintenanceTaskIds.forCronTable(candidate.tableId(), policy.id());
      if (client.scheduleIfNotExists(
          new TaskInstance<>(TableMaintenanceSchedulerPools.CRON_JOB, id, spec.get()), due)) {
        scheduled++;
      }
    }
    return scheduled;
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

  /**
   * @param tableId table entity id
   * @param policyId policy entity id
   * @return true when a queued cron-job already covers the table
   */
  public boolean isTableAlreadyQueued(long tableId, long policyId) {
    return merger.isTableAlreadyQueued(schedulerClient(dataSource), tableId, policyId);
  }

  private Optional<MaintenanceJobSpec> buildSpec(
      PolicyEntity policy, List<ExpandTableCandidate> tables) {
    List<ResolvedTableRef> refs = new ArrayList<>(tables.size());
    for (ExpandTableCandidate table : tables) {
      refs.add(new ResolvedTableRef(table.tableId(), table.catalogName(), table.tableIdentifier()));
    }
    try {
      return Optional.of(specFactory.forCron(policy, refs));
    } catch (RuntimeException e) {
      LOG.warn(
          "Skipping cron-job enqueue for policy {}: failed to build jobConf / MaintenanceJobSpec: {}",
          policy.id(),
          e.toString());
      return Optional.empty();
    }
  }
}
