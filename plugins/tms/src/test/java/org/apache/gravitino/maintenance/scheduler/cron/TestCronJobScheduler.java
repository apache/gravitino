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

import com.github.kagkarlsson.scheduler.Scheduler;
import com.github.kagkarlsson.scheduler.SchedulerClient;
import com.github.kagkarlsson.scheduler.task.TaskInstanceId;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import javax.sql.DataSource;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.maintenance.scheduler.TableMaintenanceSchedulerPools;
import org.apache.gravitino.maintenance.scheduler.expand.ExpandTableCandidate;
import org.apache.gravitino.policy.Policy;
import org.apache.gravitino.storage.relational.TestJDBCBackend;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.TestTemplate;

class TestCronJobScheduler extends TestJDBCBackend {

  @TestTemplate
  void testEnqueueIsIdempotentAndRespectsBatchSize() {
    DataSource dataSource =
        SqlSessionFactoryHelper.getInstance()
            .getSqlSessionFactory()
            .getConfiguration()
            .getEnvironment()
            .getDataSource();
    CronJobScheduler scheduler = new RelationalCronJobScheduler(dataSource);

    List<ExpandTableCandidate> three =
        List.of(
            candidate(1L, MetadataObject.Type.TABLE),
            candidate(2L, MetadataObject.Type.TABLE),
            candidate(3L, MetadataObject.Type.TABLE));
    Assertions.assertEquals(
        2, scheduler.enqueueDueNow(Policy.BuiltInType.ICEBERG_COMPACTION, 7L, three, 2));
    Assertions.assertEquals(
        0,
        scheduler.enqueueDueNow(
            Policy.BuiltInType.ICEBERG_COMPACTION,
            7L,
            List.of(
                candidate(1L, MetadataObject.Type.TABLE), candidate(2L, MetadataObject.Type.TABLE)),
            2));

    SchedulerClient client = RelationalCronJobScheduler.client(dataSource);
    Assertions.assertTrue(
        client
            .getScheduledExecution(
                TaskInstanceId.of(
                    TableMaintenanceSchedulerPools.CRON_JOB, CronJobIds.forTable(1L, 7L)))
            .isPresent());
    Assertions.assertTrue(
        client
            .getScheduledExecution(
                TaskInstanceId.of(
                    TableMaintenanceSchedulerPools.CRON_JOB, CronJobIds.forTable(2L, 7L)))
            .isPresent());
    Assertions.assertTrue(
        client
            .getScheduledExecution(
                TaskInstanceId.of(
                    TableMaintenanceSchedulerPools.CRON_JOB, CronJobIds.forTable(3L, 7L)))
            .isEmpty());
  }

  @TestTemplate
  void testSnapshotExpiryFillsIncompleteBatchBeforeAllocatingNextId() {
    DataSource dataSource =
        SqlSessionFactoryHelper.getInstance()
            .getSqlSessionFactory()
            .getConfiguration()
            .getEnvironment()
            .getDataSource();
    CronJobScheduler scheduler = new RelationalCronJobScheduler(dataSource);

    Assertions.assertEquals(
        1,
        scheduler.enqueueDueNow(
            Policy.BuiltInType.ICEBERG_SNAPSHOT_EXPIRATION,
            9L,
            List.of(
                candidate(10L, MetadataObject.Type.SCHEMA),
                candidate(11L, MetadataObject.Type.SCHEMA)),
            10));

    // Same policy, new tables: merge into batch:0 (capacity 10) instead of opening batch:1.
    Assertions.assertEquals(
        0,
        scheduler.enqueueDueNow(
            Policy.BuiltInType.ICEBERG_SNAPSHOT_EXPIRATION,
            9L,
            List.of(
                candidate(10L, MetadataObject.Type.SCHEMA),
                candidate(11L, MetadataObject.Type.SCHEMA),
                candidate(12L, MetadataObject.Type.CATALOG),
                candidate(13L, MetadataObject.Type.SCHEMA)),
            10));

    SchedulerClient client = RelationalCronJobScheduler.client(dataSource);
    TaskInstanceId batch0 =
        TaskInstanceId.of(TableMaintenanceSchedulerPools.CRON_JOB, CronJobIds.forBatch(0L, 9L));
    TaskInstanceId batch1 =
        TaskInstanceId.of(TableMaintenanceSchedulerPools.CRON_JOB, CronJobIds.forBatch(1L, 9L));
    Assertions.assertTrue(client.getScheduledExecution(batch0).isPresent());
    Assertions.assertTrue(client.getScheduledExecution(batch1).isEmpty());
    CronJobData data = (CronJobData) client.getScheduledExecution(batch0).get().getData();
    Assertions.assertEquals(List.of(10L, 11L, 12L, 13L), data.tableIds());
  }

  @TestTemplate
  void testSnapshotExpiryAllocatesNextBatchWhenCapacityFull() {
    DataSource dataSource =
        SqlSessionFactoryHelper.getInstance()
            .getSqlSessionFactory()
            .getConfiguration()
            .getEnvironment()
            .getDataSource();
    CronJobScheduler scheduler = new RelationalCronJobScheduler(dataSource);

    Assertions.assertEquals(
        1,
        scheduler.enqueueDueNow(
            Policy.BuiltInType.ICEBERG_SNAPSHOT_EXPIRATION,
            9L,
            List.of(
                candidate(1L, MetadataObject.Type.SCHEMA),
                candidate(2L, MetadataObject.Type.SCHEMA)),
            2));
    Assertions.assertEquals(
        1,
        scheduler.enqueueDueNow(
            Policy.BuiltInType.ICEBERG_SNAPSHOT_EXPIRATION,
            9L,
            List.of(
                candidate(1L, MetadataObject.Type.SCHEMA),
                candidate(2L, MetadataObject.Type.SCHEMA),
                candidate(3L, MetadataObject.Type.SCHEMA)),
            2));

    SchedulerClient client = RelationalCronJobScheduler.client(dataSource);
    CronJobData batch0 =
        (CronJobData)
            client
                .getScheduledExecution(
                    TaskInstanceId.of(
                        TableMaintenanceSchedulerPools.CRON_JOB, CronJobIds.forBatch(0L, 9L)))
                .get()
                .getData();
    CronJobData batch1 =
        (CronJobData)
            client
                .getScheduledExecution(
                    TaskInstanceId.of(
                        TableMaintenanceSchedulerPools.CRON_JOB, CronJobIds.forBatch(1L, 9L)))
                .get()
                .getData();
    Assertions.assertEquals(List.of(1L, 2L), batch0.tableIds());
    Assertions.assertEquals(List.of(3L), batch1.tableIds());
  }

  @TestTemplate
  void testPendingCronGateSkipsQueuedTables() {
    DataSource dataSource =
        SqlSessionFactoryHelper.getInstance()
            .getSqlSessionFactory()
            .getConfiguration()
            .getEnvironment()
            .getDataSource();
    CronJobScheduler scheduler = new RelationalCronJobScheduler(dataSource);
    Assertions.assertEquals(
        1,
        scheduler.enqueueDueNow(
            Policy.BuiltInType.ICEBERG_SNAPSHOT_EXPIRATION,
            9L,
            List.of(
                candidate(10L, MetadataObject.Type.SCHEMA),
                candidate(11L, MetadataObject.Type.SCHEMA)),
            10));

    PendingCronExpandOccupancyGate gate =
        new PendingCronExpandOccupancyGate((t, p, m, n) -> false, dataSource);
    Assertions.assertTrue(gate.shouldSkip(10L, 9L, 0L, System.currentTimeMillis()));
    Assertions.assertFalse(gate.shouldSkip(12L, 9L, 0L, System.currentTimeMillis()));
  }

  @TestTemplate
  void testCronJobTaskRemovesRowAfterPick() throws Exception {
    DataSource dataSource =
        SqlSessionFactoryHelper.getInstance()
            .getSqlSessionFactory()
            .getConfiguration()
            .getEnvironment()
            .getDataSource();
    CronJobScheduler enqueue = new RelationalCronJobScheduler(dataSource);
    Assertions.assertEquals(
        1,
        enqueue.enqueueDueNow(
            Policy.BuiltInType.ICEBERG_COMPACTION,
            9L,
            List.of(candidate(99L, MetadataObject.Type.TABLE)),
            1));

    TaskInstanceId instanceId =
        TaskInstanceId.of(TableMaintenanceSchedulerPools.CRON_JOB, CronJobIds.forTable(99L, 9L));
    SchedulerClient client = RelationalCronJobScheduler.client(dataSource);
    Assertions.assertTrue(client.getScheduledExecution(instanceId).isPresent());

    Scheduler cronPool =
        Scheduler.create(dataSource, TableMaintenanceSchedulerPools.cronJobTask())
            .threads(1)
            .pollingInterval(Duration.ofMillis(100))
            .tableName(TableMaintenanceSchedulerPools.CRON_TASKS_TABLE)
            .commitWhenAutocommitDisabled(true)
            .build();
    cronPool.start();
    try {
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
      while (client.getScheduledExecution(instanceId).isPresent()) {
        Assertions.assertTrue(
            System.nanoTime() < deadline, "cron-job row was not removed after pick");
        Thread.sleep(100L);
      }
    } finally {
      cronPool.stop();
    }

    Assertions.assertEquals(
        1,
        enqueue.enqueueDueNow(
            Policy.BuiltInType.ICEBERG_COMPACTION,
            9L,
            List.of(candidate(99L, MetadataObject.Type.TABLE)),
            1));
    Assertions.assertTrue(client.getScheduledExecution(instanceId).isPresent());
  }

  private static ExpandTableCandidate candidate(long tableId, MetadataObject.Type level) {
    return new ExpandTableCandidate(tableId, level);
  }
}
