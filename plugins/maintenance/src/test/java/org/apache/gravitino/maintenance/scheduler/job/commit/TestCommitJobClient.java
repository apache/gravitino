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
import com.github.kagkarlsson.scheduler.task.TaskInstanceId;
import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import javax.sql.DataSource;
import org.apache.gravitino.Config;
import org.apache.gravitino.maintenance.policy.MaintenanceAuthConfigs;
import org.apache.gravitino.maintenance.scheduler.MaintenanceJobSpec;
import org.apache.gravitino.maintenance.scheduler.MaintenanceTaskIds;
import org.apache.gravitino.maintenance.scheduler.TableMaintenanceSchedulerPools;
import org.apache.gravitino.maintenance.scheduler.job.cron.MaintenanceTableRefs.ResolvedTableRef;
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

class TestCommitJobClient extends TestJDBCBackend {

  private static final long TABLE_ID = 101L;
  private static final long POLICY_ID = 202L;
  private static final String GRAVITINO_URI = "http://localhost:8090";

  @TestTemplate
  void testEnqueueSkipsWhenAllWithinMinInterval() {
    CommitJobClient scheduler =
        new CommitJobClient(
            dataSource(),
            configWithUri(),
            fixedCollector(List.of(member(POLICY_ID))),
            coolingGate(POLICY_ID));
    Assertions.assertEquals(CommitJobEnqueueResult.SKIPPED, scheduler.enqueueNow(TABLE_ID));
    Assertions.assertTrue(
        CommitJobClient.schedulerClient(dataSource())
            .getScheduledExecution(
                TaskInstanceId.of(
                    TableMaintenanceSchedulerPools.COMMIT_JOB,
                    MaintenanceTaskIds.forCommitTable(TABLE_ID)))
            .isEmpty());
  }

  @TestTemplate
  void testEnqueueWritesSpecAndIsIdempotent() {
    CommitJobClient scheduler =
        new CommitJobClient(
            dataSource(),
            configWithUri(),
            fixedCollector(List.of(member(POLICY_ID))),
            allowAllGate());

    Assertions.assertEquals(CommitJobEnqueueResult.INSERTED, scheduler.enqueueNow(TABLE_ID));
    Assertions.assertEquals(CommitJobEnqueueResult.ALREADY_QUEUED, scheduler.enqueueNow(TABLE_ID));

    SchedulerClient client = CommitJobClient.schedulerClient(dataSource());
    MaintenanceJobSpec spec =
        (MaintenanceJobSpec)
            client
                .getScheduledExecution(
                    TaskInstanceId.of(
                        TableMaintenanceSchedulerPools.COMMIT_JOB,
                        MaintenanceTaskIds.forCommitTable(TABLE_ID)))
                .get()
                .getData();
    Assertions.assertTrue(spec.isValid());
    Assertions.assertEquals(List.of(TABLE_ID), spec.tableIds());
    Assertions.assertEquals(POLICY_ID, spec.getOperations().get(0).getPolicyId());
    Assertions.assertTrue(spec.getJobConf().containsKey("updater_options"));
    Assertions.assertNull(spec.getJobConf().get("catalog_name"));
    Assertions.assertNull(spec.getJobConf().get("table_identifier"));
    Assertions.assertEquals("iceberg", spec.getTables().get(0).catalogName());
  }

  private static Function<Long, Optional<CollectedCommitChain>> fixedCollector(
      List<CommitChainMember> chain) {
    ResolvedTableRef ref = new ResolvedTableRef(TABLE_ID, "iceberg", "db.t");
    CollectedCommitChain collected = new CollectedCommitChain(chain, ref);
    return tableId -> tableId == TABLE_ID ? Optional.of(collected) : Optional.empty();
  }

  private static CommitChainMember member(long policyId) {
    return new CommitChainMember(
        Policy.BuiltInType.ICEBERG_COMPACTION, policy(policyId), "rewrite_data_files");
  }

  private static CommitOccupancyGate allowAllGate() {
    return new CommitOccupancyGate() {
      @Override
      public boolean isInFlight(long tableId, long policyId) {
        return false;
      }

      @Override
      public boolean withinMinInterval(
          long tableId, long policyId, long minIntervalMs, long nowMs) {
        return false;
      }
    };
  }

  private static CommitOccupancyGate coolingGate(long... policyIds) {
    Set<Long> cooling = new HashSet<>();
    for (long id : policyIds) {
      cooling.add(id);
    }
    return new CommitOccupancyGate() {
      @Override
      public boolean isInFlight(long tableId, long policyId) {
        return false;
      }

      @Override
      public boolean withinMinInterval(
          long tableId, long policyId, long minIntervalMs, long nowMs) {
        return cooling.contains(policyId);
      }
    };
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

  private static PolicyEntity policy(long id) {
    return PolicyEntity.builder()
        .withId(id)
        .withName("p-" + id)
        .withNamespace(NamespaceUtil.ofPolicy("ml"))
        .withPolicyType(Policy.BuiltInType.ICEBERG_COMPACTION)
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
                Map.of("gravitino_uri", GRAVITINO_URI)))
        .withEnabled(true)
        .withAuditInfo(
            AuditInfo.builder().withCreator("test").withCreateTime(Instant.EPOCH).build())
        .build();
  }
}
