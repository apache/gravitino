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

import com.github.kagkarlsson.scheduler.SchedulerClient;
import com.github.kagkarlsson.scheduler.task.TaskInstanceId;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import javax.sql.DataSource;
import org.apache.gravitino.maintenance.TableMaintenanceConfig;
import org.apache.gravitino.maintenance.scheduler.TableMaintenanceSchedulerPools;
import org.apache.gravitino.storage.relational.TestJDBCBackend;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.TestTemplate;

class TestPolicyExpandRegistrar extends TestJDBCBackend {

  private static final String CRON = "0 0 2 * * ?";
  private static final String LATER_CRON = "0 30 4 * * ?";

  @TestTemplate
  void testSyncInsertsReschedulesAndRemovesTheExpandRow() {
    DataSource dataSource = entityStoreDataSource();
    PolicyExpandRegistrar writer = PolicyExpandRegistrar.joiningCurrentTransaction();
    PolicyExpandRegistrar reader = PolicyExpandRegistrar.forReads(dataSource);

    SessionUtils.doMultipleWithCommit(() -> writer.sync(42L, true, CRON));
    Instant firstDue = executionTime(reader, 42L);
    Assertions.assertTrue(firstDue.isAfter(Instant.now()));

    SessionUtils.doMultipleWithCommit(() -> writer.sync(42L, true, LATER_CRON));
    Instant secondDue = executionTime(reader, 42L);
    Assertions.assertNotEquals(firstDue, secondDue);

    SessionUtils.doMultipleWithCommit(() -> writer.sync(42L, false, LATER_CRON));
    Assertions.assertTrue(reader.scheduledExecutionTime(42L).isEmpty());

    SessionUtils.doMultipleWithCommit(() -> writer.sync(42L, true, CRON));
    Assertions.assertTrue(reader.scheduledExecutionTime(42L).isPresent());
    SessionUtils.doMultipleWithCommit(() -> writer.remove(42L));
    Assertions.assertTrue(reader.scheduledExecutionTime(42L).isEmpty());
    SessionUtils.doMultipleWithCommit(() -> writer.remove(42L));
  }

  @TestTemplate
  void testFailedTransactionRollsBackTheExpandRow() {
    DataSource dataSource = entityStoreDataSource();
    PolicyExpandRegistrar writer = PolicyExpandRegistrar.joiningCurrentTransaction();
    PolicyExpandRegistrar reader = PolicyExpandRegistrar.forReads(dataSource);

    Assertions.assertThrows(
        IllegalStateException.class,
        () ->
            SessionUtils.doMultipleWithCommit(
                () -> writer.sync(99L, true, CRON),
                () -> {
                  throw new IllegalStateException("boom");
                }));

    Assertions.assertTrue(reader.scheduledExecutionTime(99L).isEmpty());
  }

  @TestTemplate
  void testInvalidCronFailsBeforeAnExpandRowIsWritten() {
    DataSource dataSource = entityStoreDataSource();
    PolicyExpandRegistrar writer = PolicyExpandRegistrar.joiningCurrentTransaction();
    PolicyExpandRegistrar reader = PolicyExpandRegistrar.forReads(dataSource);

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> SessionUtils.doMultipleWithCommit(() -> writer.sync(100L, true, "not a cron")));

    Assertions.assertTrue(reader.scheduledExecutionTime(100L).isEmpty());
  }

  @TestTemplate
  void testRunningPoolsLeaveOverdueExpandRowsUnpicked() throws Exception {
    Instant overdue = Instant.now().minusSeconds(30).truncatedTo(ChronoUnit.MICROS);
    PolicyExpandRegistrar writer = PolicyExpandRegistrar.joiningCurrentTransaction();
    DataSource dataSource = entityStoreDataSource();
    SessionUtils.doMultipleWithCommit(() -> writer.scheduleAt(11L, overdue));

    SchedulerClient client =
        SchedulerClient.Builder.create(
                dataSource, TableMaintenanceSchedulerPools.policyExpandTask())
            .tableName(TableMaintenanceSchedulerPools.EXPAND_TASKS_TABLE)
            .build();
    TaskInstanceId instanceId =
        TaskInstanceId.of(TableMaintenanceSchedulerPools.POLICY_EXPAND, "11");
    Instant stored = client.getScheduledExecution(instanceId).orElseThrow().getExecutionTime();

    TableMaintenanceSchedulerPools pools = new TableMaintenanceSchedulerPools();
    pools.start(dataSource, TableMaintenanceConfig.forTest(1, 1, 1, 100L, 1_000L, 4, 10));
    try {
      Thread.sleep(400L);
      Assertions.assertFalse(client.getScheduledExecution(instanceId).orElseThrow().isPicked());
      Assertions.assertEquals(
          stored, client.getScheduledExecution(instanceId).orElseThrow().getExecutionTime());
    } finally {
      pools.stop();
    }
  }

  private static DataSource entityStoreDataSource() {
    return SqlSessionFactoryHelper.getInstance()
        .getSqlSessionFactory()
        .getConfiguration()
        .getEnvironment()
        .getDataSource();
  }

  private static Instant executionTime(PolicyExpandRegistrar registrar, long policyId) {
    return registrar
        .scheduledExecutionTime(policyId)
        .orElseThrow(() -> new AssertionError("missing expand row " + policyId));
  }
}
