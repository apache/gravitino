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

import com.github.kagkarlsson.scheduler.Scheduler;
import com.github.kagkarlsson.scheduler.SchedulerClient;
import com.github.kagkarlsson.scheduler.serializer.JacksonSerializer;
import com.github.kagkarlsson.scheduler.task.TaskInstanceId;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import javax.sql.DataSource;
import org.apache.gravitino.Config;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.maintenance.policy.MaintenanceAuthConfigs;
import org.apache.gravitino.maintenance.scheduler.MaintenanceJobSpec;
import org.apache.gravitino.maintenance.scheduler.MaintenanceTaskIds;
import org.apache.gravitino.maintenance.scheduler.TableMaintenanceSchedulerPools;
import org.apache.gravitino.maintenance.scheduler.job.cron.MaintenanceTableRefs.ResolvedTableRef;
import org.apache.gravitino.maintenance.scheduler.policy.expand.ExpandTableCandidate;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.PolicyEntity;
import org.apache.gravitino.policy.IcebergDataCompactionContent;
import org.apache.gravitino.policy.Policy;
import org.apache.gravitino.policy.PolicyContents;
import org.apache.gravitino.storage.relational.TestJDBCBackend;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.gravitino.utils.NamespaceUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.TestTemplate;

class TestCronJobClient extends TestJDBCBackend {

  private static final String GRAVITINO_URI = "http://localhost:8090";

  @TestTemplate
  void testEnqueueIsIdempotentAndRespectsBatchSize() {
    DataSource dataSource = dataSource();
    CronJobClient scheduler = scheduler(dataSource);
    PolicyEntity policy = compactionPolicy(7L);

    List<ExpandTableCandidate> three =
        List.of(
            candidate(1L, MetadataObject.Type.TABLE),
            candidate(2L, MetadataObject.Type.TABLE),
            candidate(3L, MetadataObject.Type.TABLE));
    Assertions.assertEquals(2, scheduler.enqueueNow(policy, three, 2));
    Assertions.assertEquals(
        0,
        scheduler.enqueueNow(
            policy,
            List.of(
                candidate(1L, MetadataObject.Type.TABLE), candidate(2L, MetadataObject.Type.TABLE)),
            2));

    SchedulerClient client = CronJobClient.schedulerClient(dataSource);
    Assertions.assertTrue(
        client
            .getScheduledExecution(
                TaskInstanceId.of(
                    TableMaintenanceSchedulerPools.CRON_JOB,
                    MaintenanceTaskIds.forCronTable(1L, 7L)))
            .isPresent());
    Assertions.assertTrue(
        client
            .getScheduledExecution(
                TaskInstanceId.of(
                    TableMaintenanceSchedulerPools.CRON_JOB,
                    MaintenanceTaskIds.forCronTable(2L, 7L)))
            .isPresent());
    Assertions.assertTrue(
        client
            .getScheduledExecution(
                TaskInstanceId.of(
                    TableMaintenanceSchedulerPools.CRON_JOB,
                    MaintenanceTaskIds.forCronTable(3L, 7L)))
            .isEmpty());

    MaintenanceJobSpec spec =
        (MaintenanceJobSpec)
            client
                .getScheduledExecution(
                    TaskInstanceId.of(
                        TableMaintenanceSchedulerPools.CRON_JOB,
                        MaintenanceTaskIds.forCronTable(1L, 7L)))
                .get()
                .getData();
    Assertions.assertTrue(spec.isValid());
    Assertions.assertTrue(spec.getUpdaterOptions() != null && !spec.getUpdaterOptions().isBlank());
    Assertions.assertEquals(List.of(1L), spec.tableIds());
  }

  @TestTemplate
  void testSnapshotExpiryFillsIncompleteBatchBeforeAllocatingNextId() {
    DataSource dataSource = dataSource();
    CronJobClient scheduler = scheduler(dataSource);
    PolicyEntity policy = expirePolicy(9L);

    Assertions.assertEquals(
        1,
        scheduler.enqueueNow(
            policy,
            List.of(
                candidate(10L, MetadataObject.Type.SCHEMA),
                candidate(11L, MetadataObject.Type.SCHEMA)),
            10));

    Assertions.assertEquals(
        0,
        scheduler.enqueueNow(
            policy,
            List.of(
                candidate(10L, MetadataObject.Type.SCHEMA),
                candidate(11L, MetadataObject.Type.SCHEMA),
                candidate(12L, MetadataObject.Type.CATALOG),
                candidate(13L, MetadataObject.Type.SCHEMA)),
            10));

    SchedulerClient client = CronJobClient.schedulerClient(dataSource);
    TaskInstanceId batch0 =
        TaskInstanceId.of(
            TableMaintenanceSchedulerPools.CRON_JOB, MaintenanceTaskIds.forCronBatch(0L, 9L));
    TaskInstanceId batch1 =
        TaskInstanceId.of(
            TableMaintenanceSchedulerPools.CRON_JOB, MaintenanceTaskIds.forCronBatch(1L, 9L));
    Assertions.assertTrue(client.getScheduledExecution(batch0).isPresent());
    Assertions.assertTrue(client.getScheduledExecution(batch1).isEmpty());
    MaintenanceJobSpec data =
        (MaintenanceJobSpec) client.getScheduledExecution(batch0).get().getData();
    Assertions.assertEquals(List.of(10L, 11L, 12L, 13L), data.tableIds());
    Assertions.assertEquals(1, data.getTasks().size());
    Assertions.assertTrue(data.getUpdaterOptions() != null && !data.getUpdaterOptions().isBlank());
  }

  @TestTemplate
  void testSnapshotExpiryAllocatesNextBatchWhenCapacityFull() {
    DataSource dataSource = dataSource();
    CronJobClient scheduler = scheduler(dataSource);
    PolicyEntity policy = expirePolicy(9L);
    int cap = 10; // CronJobClient snapshot-expiry batch capacity

    List<ExpandTableCandidate> full = schemaCandidates(1, cap);
    Assertions.assertEquals(1, scheduler.enqueueNow(policy, full, 100));
    List<ExpandTableCandidate> fullPlusOne = new ArrayList<>(full);
    fullPlusOne.add(candidate(cap + 1L, MetadataObject.Type.SCHEMA));
    Assertions.assertEquals(1, scheduler.enqueueNow(policy, fullPlusOne, 100));

    SchedulerClient client = CronJobClient.schedulerClient(dataSource);
    MaintenanceJobSpec batch0 =
        (MaintenanceJobSpec)
            client
                .getScheduledExecution(
                    TaskInstanceId.of(
                        TableMaintenanceSchedulerPools.CRON_JOB,
                        MaintenanceTaskIds.forCronBatch(0L, 9L)))
                .get()
                .getData();
    MaintenanceJobSpec batch1 =
        (MaintenanceJobSpec)
            client
                .getScheduledExecution(
                    TaskInstanceId.of(
                        TableMaintenanceSchedulerPools.CRON_JOB,
                        MaintenanceTaskIds.forCronBatch(1L, 9L)))
                .get()
                .getData();
    Assertions.assertEquals(cap, batch0.tableIds().size());
    Assertions.assertEquals(List.of(cap + 1L), batch1.tableIds());
  }

  @TestTemplate
  void testSnapshotExpiryIncompleteBatchAbsorbsUnpickedTableJob() {
    DataSource dataSource = dataSource();
    CronJobClient scheduler = scheduler(dataSource);
    PolicyEntity policy = expirePolicy(9L);

    Assertions.assertEquals(
        1, scheduler.enqueueNow(policy, List.of(candidate(99L, MetadataObject.Type.TABLE)), 10));

    SchedulerClient client = CronJobClient.schedulerClient(dataSource);
    TaskInstanceId tableJob =
        TaskInstanceId.of(
            TableMaintenanceSchedulerPools.CRON_JOB, MaintenanceTaskIds.forCronTable(99L, 9L));
    Assertions.assertTrue(client.getScheduledExecution(tableJob).isPresent());

    Assertions.assertEquals(
        1,
        scheduler.enqueueNow(
            policy,
            List.of(
                candidate(10L, MetadataObject.Type.SCHEMA),
                candidate(11L, MetadataObject.Type.SCHEMA)),
            10));

    TaskInstanceId batch0 =
        TaskInstanceId.of(
            TableMaintenanceSchedulerPools.CRON_JOB, MaintenanceTaskIds.forCronBatch(0L, 9L));
    Assertions.assertTrue(client.getScheduledExecution(batch0).isPresent());
    Assertions.assertTrue(
        client.getScheduledExecution(tableJob).isEmpty(),
        "unpicked table: row should be cancelled after merge into batch");
    MaintenanceJobSpec data =
        (MaintenanceJobSpec) client.getScheduledExecution(batch0).get().getData();
    Assertions.assertEquals(List.of(10L, 11L, 99L), data.tableIds());
  }

  @TestTemplate
  void testSnapshotExpiryTableJobsCoalesceIntoBatch() {
    DataSource dataSource = dataSource();
    CronJobClient scheduler = scheduler(dataSource);
    PolicyEntity policy = expirePolicy(9L);

    Assertions.assertEquals(
        1, scheduler.enqueueNow(policy, List.of(candidate(21L, MetadataObject.Type.TABLE)), 10));
    Assertions.assertEquals(
        1, scheduler.enqueueNow(policy, List.of(candidate(22L, MetadataObject.Type.TABLE)), 10));

    SchedulerClient client = CronJobClient.schedulerClient(dataSource);
    TaskInstanceId table21 =
        TaskInstanceId.of(
            TableMaintenanceSchedulerPools.CRON_JOB, MaintenanceTaskIds.forCronTable(21L, 9L));
    TaskInstanceId table22 =
        TaskInstanceId.of(
            TableMaintenanceSchedulerPools.CRON_JOB, MaintenanceTaskIds.forCronTable(22L, 9L));
    TaskInstanceId batch0 =
        TaskInstanceId.of(
            TableMaintenanceSchedulerPools.CRON_JOB, MaintenanceTaskIds.forCronBatch(0L, 9L));
    Assertions.assertTrue(client.getScheduledExecution(table21).isEmpty());
    Assertions.assertTrue(client.getScheduledExecution(table22).isEmpty());
    Assertions.assertTrue(client.getScheduledExecution(batch0).isPresent());
    MaintenanceJobSpec data =
        (MaintenanceJobSpec) client.getScheduledExecution(batch0).get().getData();
    Assertions.assertEquals(List.of(22L, 21L), data.tableIds());
  }

  @TestTemplate
  void testSnapshotExpiryTableMergesIntoExistingIncompleteBatch() {
    DataSource dataSource = dataSource();
    CronJobClient scheduler = scheduler(dataSource);
    PolicyEntity policy = expirePolicy(9L);

    Assertions.assertEquals(
        1,
        scheduler.enqueueNow(
            policy,
            List.of(
                candidate(10L, MetadataObject.Type.SCHEMA),
                candidate(11L, MetadataObject.Type.SCHEMA)),
            10));
    Assertions.assertEquals(
        0, scheduler.enqueueNow(policy, List.of(candidate(99L, MetadataObject.Type.TABLE)), 10));

    SchedulerClient client = CronJobClient.schedulerClient(dataSource);
    TaskInstanceId tableJob =
        TaskInstanceId.of(
            TableMaintenanceSchedulerPools.CRON_JOB, MaintenanceTaskIds.forCronTable(99L, 9L));
    TaskInstanceId batch0 =
        TaskInstanceId.of(
            TableMaintenanceSchedulerPools.CRON_JOB, MaintenanceTaskIds.forCronBatch(0L, 9L));
    Assertions.assertTrue(client.getScheduledExecution(tableJob).isEmpty());
    MaintenanceJobSpec data =
        (MaintenanceJobSpec) client.getScheduledExecution(batch0).get().getData();
    Assertions.assertEquals(List.of(10L, 11L, 99L), data.tableIds());
  }

  @TestTemplate
  void testSnapshotExpiryMergesAcrossPoliciesSameCatalog() {
    DataSource dataSource = dataSource();
    CronJobClient scheduler = scheduler(dataSource);

    Assertions.assertEquals(
        1,
        scheduler.enqueueNow(
            expirePolicy(9L), List.of(candidate(21L, MetadataObject.Type.TABLE)), 10));
    Assertions.assertEquals(
        1,
        scheduler.enqueueNow(
            expirePolicy(10L), List.of(candidate(22L, MetadataObject.Type.TABLE)), 10));

    SchedulerClient client = CronJobClient.schedulerClient(dataSource);
    Assertions.assertTrue(
        client
            .getScheduledExecution(
                TaskInstanceId.of(
                    TableMaintenanceSchedulerPools.CRON_JOB,
                    MaintenanceTaskIds.forCronTable(21L, 9L)))
            .isEmpty());
    Assertions.assertTrue(
        client
            .getScheduledExecution(
                TaskInstanceId.of(
                    TableMaintenanceSchedulerPools.CRON_JOB,
                    MaintenanceTaskIds.forCronTable(22L, 10L)))
            .isEmpty());
    TaskInstanceId batch0 =
        TaskInstanceId.of(
            TableMaintenanceSchedulerPools.CRON_JOB, MaintenanceTaskIds.forCronBatch(0L, 10L));
    Assertions.assertTrue(client.getScheduledExecution(batch0).isPresent());
    MaintenanceJobSpec data =
        (MaintenanceJobSpec) client.getScheduledExecution(batch0).get().getData();
    Assertions.assertEquals(List.of(22L, 21L), data.tableIds());
  }

  @TestTemplate
  void testSnapshotExpiryDoesNotMergeDifferentCatalogs() {
    DataSource dataSource = dataSource();
    CronJobClient scheduler = multiCatalogScheduler(dataSource);

    Assertions.assertEquals(
        1,
        scheduler.enqueueNow(
            expirePolicy(9L), List.of(candidate(1L, MetadataObject.Type.TABLE)), 10));
    Assertions.assertEquals(
        1,
        scheduler.enqueueNow(
            expirePolicy(9L), List.of(candidate(1001L, MetadataObject.Type.TABLE)), 10));

    SchedulerClient client = CronJobClient.schedulerClient(dataSource);
    Assertions.assertTrue(
        client
            .getScheduledExecution(
                TaskInstanceId.of(
                    TableMaintenanceSchedulerPools.CRON_JOB,
                    MaintenanceTaskIds.forCronTable(1L, 9L)))
            .isPresent());
    Assertions.assertTrue(
        client
            .getScheduledExecution(
                TaskInstanceId.of(
                    TableMaintenanceSchedulerPools.CRON_JOB,
                    MaintenanceTaskIds.forCronTable(1001L, 9L)))
            .isPresent());
  }

  @TestTemplate
  void testSnapshotExpirySkipsPeerWhenMergeWouldExceedCapacity() {
    DataSource dataSource = dataSource();
    CronJobClient scheduler = scheduler(dataSource);
    PolicyEntity policy = expirePolicy(9L);
    int cap = 10; // CronJobClient snapshot-expiry batch capacity

    // Full batch at capacity — must not be absorbed into a later incomplete peer.
    Assertions.assertEquals(1, scheduler.enqueueNow(policy, schemaCandidates(1, cap), 100));
    Assertions.assertEquals(
        1,
        scheduler.enqueueNow(
            policy,
            List.of(
                candidate(100L, MetadataObject.Type.SCHEMA),
                candidate(101L, MetadataObject.Type.SCHEMA)),
            100));

    SchedulerClient client = CronJobClient.schedulerClient(dataSource);
    MaintenanceJobSpec batch0 =
        (MaintenanceJobSpec)
            client
                .getScheduledExecution(
                    TaskInstanceId.of(
                        TableMaintenanceSchedulerPools.CRON_JOB,
                        MaintenanceTaskIds.forCronBatch(0L, 9L)))
                .get()
                .getData();
    MaintenanceJobSpec batch1 =
        (MaintenanceJobSpec)
            client
                .getScheduledExecution(
                    TaskInstanceId.of(
                        TableMaintenanceSchedulerPools.CRON_JOB,
                        MaintenanceTaskIds.forCronBatch(1L, 9L)))
                .get()
                .getData();
    Assertions.assertEquals(cap, batch0.tableIds().size());
    Assertions.assertEquals(List.of(100L, 101L), batch1.tableIds());
  }

  @TestTemplate
  void testPendingCronGateSkipsQueuedTables() {
    DataSource dataSource = dataSource();
    CronJobClient scheduler = scheduler(dataSource);
    Assertions.assertEquals(
        1,
        scheduler.enqueueNow(
            expirePolicy(9L),
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
    DataSource dataSource = dataSource();
    CronJobClient enqueue = scheduler(dataSource);
    Assertions.assertEquals(
        1,
        enqueue.enqueueNow(
            compactionPolicy(9L), List.of(candidate(99L, MetadataObject.Type.TABLE)), 1));

    TaskInstanceId instanceId =
        TaskInstanceId.of(
            TableMaintenanceSchedulerPools.CRON_JOB, MaintenanceTaskIds.forCronTable(99L, 9L));
    SchedulerClient client = CronJobClient.schedulerClient(dataSource);
    Assertions.assertTrue(client.getScheduledExecution(instanceId).isPresent());

    Scheduler cronPool =
        Scheduler.create(dataSource, TableMaintenanceSchedulerPools.cronJobTask())
            .threads(1)
            .pollingInterval(Duration.ofMillis(100))
            .serializer(new JacksonSerializer(JsonUtils.anyFieldMapper()))
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
        enqueue.enqueueNow(
            compactionPolicy(9L), List.of(candidate(99L, MetadataObject.Type.TABLE)), 1));
    Assertions.assertTrue(client.getScheduledExecution(instanceId).isPresent());
  }

  private static CronJobClient scheduler(DataSource dataSource) {
    return new CronJobClient(
        dataSource,
        configWithUri(),
        ids ->
            ids.stream()
                .map(id -> new ResolvedTableRef(id, "iceberg", "db.t" + id))
                .collect(Collectors.toList()));
  }

  /** Tables with id &gt;= 1000 resolve to catalog {@code other}; others to {@code iceberg}. */
  private static CronJobClient multiCatalogScheduler(DataSource dataSource) {
    return new CronJobClient(
        dataSource,
        configWithUri(),
        ids ->
            ids.stream()
                .map(id -> new ResolvedTableRef(id, id >= 1000L ? "other" : "iceberg", "db.t" + id))
                .collect(Collectors.toList()));
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

  private static ExpandTableCandidate candidate(long tableId, MetadataObject.Type level) {
    return new ExpandTableCandidate(tableId, level);
  }

  private static List<ExpandTableCandidate> schemaCandidates(long fromInclusive, long toInclusive) {
    return LongStream.rangeClosed(fromInclusive, toInclusive)
        .mapToObj(id -> candidate(id, MetadataObject.Type.SCHEMA))
        .collect(Collectors.toList());
  }

  private static PolicyEntity compactionPolicy(long id) {
    return PolicyEntity.builder()
        .withId(id)
        .withName("p-" + id)
        .withNamespace(NamespaceUtil.ofPolicy("ml"))
        .withPolicyType(Policy.BuiltInType.ICEBERG_COMPACTION)
        .withEnabled(true)
        .withContent(
            PolicyContents.icebergDataCompaction(
                IcebergDataCompactionContent.DEFAULT_MIN_DATA_FILE_MSE,
                IcebergDataCompactionContent.DEFAULT_MIN_DELETE_FILE_NUMBER,
                IcebergDataCompactionContent.DEFAULT_DATA_FILE_MSE_WEIGHT,
                IcebergDataCompactionContent.DEFAULT_DELETE_FILE_NUMBER_WEIGHT,
                IcebergDataCompactionContent.DEFAULT_MAX_PARTITION_NUM,
                "binpack",
                "",
                Map.of(),
                null,
                true,
                null,
                Map.of("spark_executor_memory", "4g"),
                Map.of()))
        .withAuditInfo(
            AuditInfo.builder().withCreator("test").withCreateTime(Instant.EPOCH).build())
        .build();
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
