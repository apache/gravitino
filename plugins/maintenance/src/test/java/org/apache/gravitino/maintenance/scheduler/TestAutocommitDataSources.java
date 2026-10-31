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

import com.github.kagkarlsson.scheduler.SchedulerClient;
import com.github.kagkarlsson.scheduler.task.TaskInstanceId;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import javax.sql.DataSource;
import org.apache.gravitino.Config;
import org.apache.gravitino.maintenance.policy.MaintenanceAuthConfigs;
import org.apache.gravitino.maintenance.scheduler.job.cron.CronJobClient;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.PolicyEntity;
import org.apache.gravitino.policy.Policy;
import org.apache.gravitino.policy.PolicyContents;
import org.apache.gravitino.storage.relational.TestJDBCBackend;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.gravitino.utils.NamespaceUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.TestTemplate;

class TestAutocommitDataSources extends TestJDBCBackend {

  @TestTemplate
  void testRunInTransactionCommitsSchedulerClientCancel() throws Exception {
    DataSource dataSource = dataSource();
    SchedulerClient client = CronJobClient.schedulerClient(dataSource);
    TaskInstanceId instanceId = scheduleOneExpireRow(501L, 50L);

    Assertions.assertTrue(client.getScheduledExecution(instanceId).isPresent());
    AutocommitDataSources.runInTransaction(
        dataSource,
        (Function<DataSource, Void>)
            txDs -> {
              CronJobClient.schedulerClientOn(txDs).cancel(instanceId);
              return null;
            });
    Assertions.assertTrue(client.getScheduledExecution(instanceId).isEmpty());
  }

  @TestTemplate
  void testRunInTransactionRollsBackSchedulerClientCancelWhenWorkThrows() {
    DataSource dataSource = dataSource();
    SchedulerClient client = CronJobClient.schedulerClient(dataSource);
    TaskInstanceId instanceId = scheduleOneExpireRow(502L, 51L);

    Assertions.assertTrue(client.getScheduledExecution(instanceId).isPresent());
    Assertions.assertThrows(
        IllegalStateException.class,
        () ->
            AutocommitDataSources.runInTransaction(
                dataSource,
                (Function<DataSource, Void>)
                    txDs -> {
                      CronJobClient.schedulerClientOn(txDs).cancel(instanceId);
                      throw new IllegalStateException("abort after cancel");
                    }));
    Assertions.assertTrue(
        client.getScheduledExecution(instanceId).isPresent(),
        "cancel must roll back when the transactional work throws");
  }

  @TestTemplate
  void testRunInTransactionRollsBackCancelWhenLaterStepThrows() {
    DataSource dataSource = dataSource();
    SchedulerClient client = CronJobClient.schedulerClient(dataSource);
    TaskInstanceId peerId = scheduleOneExpireRow(503L, 52L);

    Assertions.assertThrows(
        IllegalStateException.class,
        () ->
            AutocommitDataSources.runInTransaction(
                dataSource,
                (Function<DataSource, Void>)
                    txDs -> {
                      CronJobClient.schedulerClientOn(txDs).cancel(peerId);
                      throw new IllegalStateException("simulate reschedule failure after cancel");
                    }));

    Assertions.assertTrue(
        client.getScheduledExecution(peerId).isPresent(),
        "peer cancel must roll back when a later step in the same transaction fails");
  }

  private static TaskInstanceId scheduleOneExpireRow(long tableId, long policyId) {
    PolicyEntity policy = expirePolicy(policyId);
    CronJobClient cronJobs = new CronJobClient(dataSource(), configWithUri());
    Assertions.assertEquals(
        1,
        cronJobs.scheduleNow(
            policy,
            List.of(new TableCandidate(tableId, "iceberg", "db.t" + tableId, List.of(policy))),
            10));
    SchedulerClient client = CronJobClient.schedulerClient(dataSource());
    return client
        .getScheduledExecutionsForTask(MaintenanceSchedulerPools.CRON_JOB, MaintenanceJobSpec.class)
        .stream()
        .filter(
            execution -> {
              MaintenanceJobSpec data = execution.getData();
              return data != null && data.tableIds().contains(tableId);
            })
        .map(execution -> execution.getTaskInstance())
        .findFirst()
        .orElseThrow();
  }

  private static DataSource dataSource() {
    return SqlSessionFactoryHelper.getInstance()
        .getSqlSessionFactory()
        .getConfiguration()
        .getEnvironment()
        .getDataSource();
  }

  private static Config configWithUri() {
    Config config = new Config(false) {};
    config.loadFromMap(
        Map.of(
            MaintenanceAuthConfigs.GRAVITINO_MAINTENANCE_GRAVITINO_AUTH_URI.getKey(),
            "http://localhost:8090"),
        t -> true);
    return config;
  }

  private static PolicyEntity expirePolicy(long id) {
    return PolicyEntity.builder()
        .withId(id)
        .withName("e-" + id)
        .withNamespace(NamespaceUtil.ofPolicy("ml"))
        .withPolicyType(Policy.BuiltInType.ICEBERG_SNAPSHOT_EXPIRATION)
        .withEnabled(true)
        .withContent(PolicyContents.icebergSnapshotExpiration())
        .withAuditInfo(
            AuditInfo.builder().withCreator("test").withCreateTime(Instant.EPOCH).build())
        .build();
  }
}
