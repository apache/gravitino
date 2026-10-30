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
package org.apache.gravitino.maintenance.scheduler.policy.expand;

import com.github.kagkarlsson.scheduler.SchedulerClient;
import com.github.kagkarlsson.scheduler.task.TaskInstanceId;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.sql.DataSource;
import org.apache.gravitino.Config;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.maintenance.policy.MaintenanceAuthConfigs;
import org.apache.gravitino.maintenance.scheduler.MaintenanceJobSpec;
import org.apache.gravitino.maintenance.scheduler.MaintenanceTaskIds;
import org.apache.gravitino.maintenance.scheduler.TableMaintenanceSchedulerPools;
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

/**
 * Expand pipeline → real {@link CronJobClient}: snapshot-expiry coalesce across two policy expands
 * (same catalog).
 */
class TestPolicyExpandSnapshotEnqueue extends TestJDBCBackend {

  private static final String GRAVITINO_URI = "http://localhost:8090";
  private static final String CRON = "0 0 2 * * ?";

  @TestTemplate
  void testTwoPolicyExpandsCoalesceSameCatalogTables() {
    DataSource dataSource = dataSource();
    CronJobClient cronJobs = cronScheduler(dataSource);

    PolicyEntity policyA = expirePolicy(9L);
    PolicyEntity policyB = expirePolicy(10L);
    PolicyTableEnumerator tablesA =
        (policy, limit, gate, minIntervalMs) -> List.of(candidate(21L, MetadataObject.Type.TABLE));
    PolicyTableEnumerator tablesB =
        (policy, limit, gate, minIntervalMs) -> List.of(candidate(22L, MetadataObject.Type.TABLE));

    PolicyExpandPipeline expandA =
        new PolicyExpandPipeline(
            id -> id == 9L ? Optional.of(policyA) : Optional.empty(),
            tablesA,
            cronJobs,
            (t, p, m, n) -> false,
            configWithUri(),
            10);
    PolicyExpandPipeline expandB =
        new PolicyExpandPipeline(
            id -> id == 10L ? Optional.of(policyB) : Optional.empty(),
            tablesB,
            cronJobs,
            (t, p, m, n) -> false,
            configWithUri(),
            10);

    Assertions.assertTrue(expandA.expand(9L).isPresent());
    Assertions.assertTrue(expandB.expand(10L).isPresent());

    SchedulerClient client = CronJobClient.schedulerClient(dataSource);
    Assertions.assertTrue(
        client
            .getScheduledExecution(
                TaskInstanceId.of(
                    TableMaintenanceSchedulerPools.CRON_JOB,
                    MaintenanceTaskIds.forCronTable(21L, 9L)))
            .isEmpty());
    TaskInstanceId table22 =
        TaskInstanceId.of(
            TableMaintenanceSchedulerPools.CRON_JOB, MaintenanceTaskIds.forCronTable(22L, 10L));
    Assertions.assertTrue(client.getScheduledExecution(table22).isPresent());
    Assertions.assertTrue(
        client
            .getScheduledExecution(
                TaskInstanceId.of(
                    TableMaintenanceSchedulerPools.CRON_JOB,
                    MaintenanceTaskIds.forCronBatch(0L, 10L)))
            .isEmpty());
    MaintenanceJobSpec data =
        (MaintenanceJobSpec) client.getScheduledExecution(table22).get().getData();
    Assertions.assertEquals(List.of(22L, 21L), data.tableIds());
    Assertions.assertEquals(2, data.getTasks().size());
    Assertions.assertEquals(10L, data.getTasks().get(0).getPolicyId());
    Assertions.assertEquals(9L, data.getTasks().get(1).getPolicyId());
  }

  @TestTemplate
  void testSchemaExpandAbsorbsPriorTableCronJob() {
    DataSource dataSource = dataSource();
    CronJobClient cronJobs = cronScheduler(dataSource);
    PolicyEntity policy = expirePolicy(9L);

    PolicyExpandPipeline tableExpand =
        new PolicyExpandPipeline(
            id -> Optional.of(policy),
            (p, limit, gate, min) -> List.of(candidate(99L, MetadataObject.Type.TABLE)),
            cronJobs,
            (t, p, m, n) -> false,
            configWithUri(),
            10);
    PolicyExpandPipeline schemaExpand =
        new PolicyExpandPipeline(
            id -> Optional.of(policy),
            (p, limit, gate, min) ->
                List.of(
                    candidate(10L, MetadataObject.Type.SCHEMA),
                    candidate(11L, MetadataObject.Type.SCHEMA)),
            cronJobs,
            (t, p, m, n) -> false,
            configWithUri(),
            10);

    Assertions.assertTrue(tableExpand.expand(9L).isPresent());
    Assertions.assertTrue(schemaExpand.expand(9L).isPresent());

    SchedulerClient client = CronJobClient.schedulerClient(dataSource);
    Assertions.assertTrue(
        client
            .getScheduledExecution(
                TaskInstanceId.of(
                    TableMaintenanceSchedulerPools.CRON_JOB,
                    MaintenanceTaskIds.forCronTable(99L, 9L)))
            .isEmpty());
    MaintenanceJobSpec data =
        (MaintenanceJobSpec)
            client
                .getScheduledExecution(
                    TaskInstanceId.of(
                        TableMaintenanceSchedulerPools.CRON_JOB,
                        MaintenanceTaskIds.forCronBatch(0L, 9L)))
                .get()
                .getData();
    Assertions.assertEquals(List.of(10L, 11L, 99L), data.tableIds());
  }

  private static ExpandTableCandidate candidate(long tableId, MetadataObject.Type level) {
    return new ExpandTableCandidate(tableId, level, "iceberg", "db.t" + tableId);
  }

  private static CronJobClient cronScheduler(DataSource dataSource) {
    return new CronJobClient(dataSource, configWithUri());
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
            GRAVITINO_URI),
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
        .withContent(PolicyContents.icebergSnapshotExpiration(CRON, null, null, null, null))
        .withAuditInfo(
            AuditInfo.builder().withCreator("test").withCreateTime(Instant.EPOCH).build())
        .build();
  }
}
