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
package org.apache.gravitino.maintenance.scheduler.commit;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.github.kagkarlsson.scheduler.SchedulerClient;
import com.github.kagkarlsson.scheduler.task.TaskInstanceId;
import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import javax.sql.DataSource;
import org.apache.gravitino.Config;
import org.apache.gravitino.maintenance.policy.MaintenanceAuthConfigs;
import org.apache.gravitino.maintenance.scheduler.MaintenanceJobSnapshot;
import org.apache.gravitino.maintenance.scheduler.TableMaintenanceSchedulerPools;
import org.apache.gravitino.maintenance.scheduler.cron.MaintenanceTableRefs.ResolvedTableRef;
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

class TestRelationalCommitJobScheduler extends TestJDBCBackend {

  private static final long TABLE_ID = 101L;
  private static final long POLICY_ID = 202L;
  private static final String GRAVITINO_URI = "http://localhost:8090";

  @TestTemplate
  void testEnqueueSkipsWhenAllWithinMinInterval() {
    CommitChainCollector collector = mockCollector(List.of(member(POLICY_ID)));
    RelationalCommitJobScheduler scheduler =
        new RelationalCommitJobScheduler(
            dataSource(), configWithUri(), collector, coolingGate(POLICY_ID));
    Assertions.assertFalse(scheduler.upsertDueNow(TABLE_ID));
    Assertions.assertTrue(
        RelationalCommitJobScheduler.client(dataSource())
            .getScheduledExecution(
                TaskInstanceId.of(
                    TableMaintenanceSchedulerPools.COMMIT_JOB, Long.toString(TABLE_ID)))
            .isEmpty());
  }

  @TestTemplate
  void testEnqueueWritesSnapshotAndIsIdempotent() {
    CommitChainCollector collector = mockCollector(List.of(member(POLICY_ID)));
    RelationalCommitJobScheduler scheduler =
        new RelationalCommitJobScheduler(dataSource(), configWithUri(), collector, allowAllGate());

    Assertions.assertTrue(scheduler.upsertDueNow(TABLE_ID));
    Assertions.assertFalse(scheduler.upsertDueNow(TABLE_ID));

    SchedulerClient client = RelationalCommitJobScheduler.client(dataSource());
    MaintenanceJobSnapshot snap =
        (MaintenanceJobSnapshot)
            client
                .getScheduledExecution(
                    TaskInstanceId.of(
                        TableMaintenanceSchedulerPools.COMMIT_JOB, Long.toString(TABLE_ID)))
                .get()
                .getData();
    Assertions.assertTrue(snap.isValid());
    Assertions.assertEquals(List.of(TABLE_ID), snap.tableIds());
    Assertions.assertEquals(POLICY_ID, snap.getSteps().get(0).getPolicyId());
    Assertions.assertTrue(snap.getJobConf().containsKey("updater_options"));
    Assertions.assertEquals("iceberg", snap.getJobConf().get("catalog_name"));
  }

  private static CommitChainCollector mockCollector(List<CommitChainMember> chain) {
    CommitChainCollector collector = mock(CommitChainCollector.class);
    when(collector.collect(TABLE_ID)).thenReturn(chain);
    when(collector.resolveTableRef(TABLE_ID))
        .thenReturn(Optional.of(new ResolvedTableRef(TABLE_ID, "iceberg", "db.t")));
    return collector;
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
        Map.of(MaintenanceAuthConfigs.GRAVITINO_MAINTENANCE_GRAVITINO_URI.getKey(), GRAVITINO_URI),
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
