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
import org.apache.gravitino.maintenance.scheduler.TableMaintenanceSchedulerPools;
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

    Assertions.assertEquals(2, scheduler.enqueueDueNow(7L, List.of(1L, 2L, 3L), 2));
    Assertions.assertEquals(0, scheduler.enqueueDueNow(7L, List.of(1L, 2L), 2));

    SchedulerClient client =
        SchedulerClient.Builder.create(dataSource, TableMaintenanceSchedulerPools.cronJobTask())
            .tableName(TableMaintenanceSchedulerPools.CRON_TASKS_TABLE)
            .build();
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
  void testCronJobTaskRemovesRowAfterPick() throws Exception {
    DataSource dataSource =
        SqlSessionFactoryHelper.getInstance()
            .getSqlSessionFactory()
            .getConfiguration()
            .getEnvironment()
            .getDataSource();
    CronJobScheduler enqueue = new RelationalCronJobScheduler(dataSource);
    Assertions.assertEquals(1, enqueue.enqueueDueNow(9L, List.of(99L), 1));

    TaskInstanceId instanceId =
        TaskInstanceId.of(TableMaintenanceSchedulerPools.CRON_JOB, CronJobIds.forTable(99L, 9L));
    SchedulerClient client =
        SchedulerClient.Builder.create(dataSource, TableMaintenanceSchedulerPools.cronJobTask())
            .tableName(TableMaintenanceSchedulerPools.CRON_TASKS_TABLE)
            .build();
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

    // After remove, expand can schedule the same instance id again.
    Assertions.assertEquals(1, enqueue.enqueueDueNow(9L, List.of(99L), 1));
    Assertions.assertTrue(client.getScheduledExecution(instanceId).isPresent());
  }
}
