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
package org.apache.gravitino.maintenance.scheduler.job.commit;

import com.github.kagkarlsson.scheduler.SchedulerClient;
import com.github.kagkarlsson.scheduler.serializer.JacksonSerializer;
import com.github.kagkarlsson.scheduler.task.TaskInstance;
import com.google.common.base.Preconditions;
import java.time.Instant;
import java.util.Optional;
import javax.sql.DataSource;
import org.apache.gravitino.Config;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.maintenance.scheduler.AutocommitDataSources;
import org.apache.gravitino.maintenance.scheduler.MaintenanceJobSpec;
import org.apache.gravitino.maintenance.scheduler.MaintenanceTaskIds;
import org.apache.gravitino.maintenance.scheduler.TableMaintenanceSchedulerPools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Submits one-shot {@code commit-job} rows to db-scheduler. Planning is done by {@link
 * CommitJobPlanner}.
 */
public class CommitJobClient {

  private static final Logger LOG = LoggerFactory.getLogger(CommitJobClient.class);

  private final DataSource dataSource;
  private final CommitJobPlanner planner;

  /**
   * @param dataSource relational JDBC pool shared with the entity store
   * @param gravitinoConfig server config
   */
  public CommitJobClient(DataSource dataSource, Config gravitinoConfig) {
    this.dataSource = Preconditions.checkNotNull(dataSource);
    this.planner = new CommitJobPlanner(Preconditions.checkNotNull(gravitinoConfig));
  }

  /**
   * Plans then schedules a {@code commit-job} for the table in {@code identifier}.
   *
   * @param identifier {@code metalake.catalog.schema.table} from the commit event
   */
  public void scheduleNow(NameIdentifier identifier) {
    if (identifier == null) {
      return;
    }
    SchedulerClient client = schedulerClient(dataSource);
    Optional<MaintenanceJobSpec> spec = planner.plan(client, identifier);
    if (spec.isEmpty()) {
      return;
    }
    long tableId = spec.get().tableIds().get(0);
    String id = MaintenanceTaskIds.forCommitTable(tableId);
    try {
      if (client.scheduleIfNotExists(
          new TaskInstance<>(TableMaintenanceSchedulerPools.COMMIT_JOB, id, spec.get()),
          Instant.now())) {
        LOG.info("Scheduled commit-job for table {} ({})", tableId, identifier);
      } else {
        LOG.debug("commit-job already queued for table {} ({})", tableId, identifier);
      }
    } catch (RuntimeException e) {
      LOG.warn("Failed to schedule commit-job for IRC update {}: {}", identifier, e.toString());
    }
  }

  /**
   * @param dataSource entity-store pool
   * @return client bound to the commit-job table
   */
  public static SchedulerClient schedulerClient(DataSource dataSource) {
    return SchedulerClient.Builder.create(
            AutocommitDataSources.wrap(dataSource), TableMaintenanceSchedulerPools.commitJobTask())
        .serializer(new JacksonSerializer(JsonUtils.anyFieldMapper()))
        .tableName(TableMaintenanceSchedulerPools.COMMIT_TASKS_TABLE)
        .build();
  }
}
