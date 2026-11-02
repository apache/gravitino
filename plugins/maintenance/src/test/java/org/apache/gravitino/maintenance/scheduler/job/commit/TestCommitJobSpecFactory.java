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

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.Config;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.maintenance.optimizer.common.util.IcebergSparkConfigUtils;
import org.apache.gravitino.maintenance.policy.MaintenanceAuthConfigs;
import org.apache.gravitino.maintenance.scheduler.MaintenanceJobSpec;
import org.apache.gravitino.maintenance.scheduler.MaintenanceJobSpecFactory;
import org.apache.gravitino.maintenance.scheduler.job.cron.MaintenanceJobTemplates;
import org.apache.gravitino.maintenance.scheduler.job.cron.MaintenanceTableRefs.ResolvedTableRef;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.PolicyEntity;
import org.apache.gravitino.policy.IcebergDataCompactionContent;
import org.apache.gravitino.policy.Policy;
import org.apache.gravitino.policy.PolicyContents;
import org.apache.gravitino.utils.NamespaceUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TestCommitJobSpecFactory {

  private static final String GRAVITINO_URI = "http://localhost:8090";

  @Test
  void testForCommitBuildsPerTypeTasksAndSharedBags() {
    CommitJobSpecFactory factory = new CommitJobSpecFactory(configWithUri());
    CommitChainMember compaction =
        new CommitChainMember(
            Policy.BuiltInType.ICEBERG_COMPACTION, compaction(202L), "rewrite_data_files");
    CommitChainMember expire =
        new CommitChainMember(
            Policy.BuiltInType.ICEBERG_SNAPSHOT_EXPIRATION, expire(303L), "expire_snapshots");
    MaintenanceJobSpec spec =
        factory.forCommit(
            List.of(compaction, expire), new ResolvedTableRef(101L, "iceberg", "db.t"));

    Assertions.assertTrue(spec.isValid());
    Assertions.assertEquals(2, spec.getTasks().size());
    Assertions.assertEquals(101L, spec.getTasks().get(0).getTableId());
    Assertions.assertEquals(202L, spec.getTasks().get(0).getPolicyId());
    Assertions.assertEquals("rewrite_data_files", spec.getTasks().get(0).getMaintenanceType());
    Assertions.assertEquals("expire_snapshots", spec.getTasks().get(1).getMaintenanceType());
    Assertions.assertEquals("7d", spec.getTasks().get(1).getParameters().get("older_than"));
    Assertions.assertEquals("10", spec.getTasks().get(1).getParameters().get("retain_last"));
    Assertions.assertTrue(spec.getUpdaterOptions().contains("metalake"));
    Assertions.assertTrue(spec.getSparkConf().contains("spark.executor.memory"));
    Map<String, String> spark =
        IcebergSparkConfigUtils.parseFlatJsonMap(spec.getSparkConf(), "spark_conf");
    Assertions.assertEquals("4g", spark.get("spark.executor.memory"));

    Map<String, String> runJob = MaintenanceJobSpecFactory.jobConfForCommitChain(spec);
    Assertions.assertEquals(
        "rewrite_data_files,expire_snapshots",
        runJob.get(MaintenanceJobTemplates.MAINTENANCE_TYPES_KEY));
    Assertions.assertEquals("202,303", runJob.get(MaintenanceJobTemplates.POLICY_IDS_KEY));
    Assertions.assertEquals("7d", runJob.get("older_than"));
    Assertions.assertEquals("10", runJob.get("retain_last"));
    Assertions.assertEquals("4g", runJob.get("spark_executor_memory"));
    String rulesByType = runJob.get(MaintenanceJobTemplates.RULES_KEY);
    Assertions.assertNotNull(rulesByType);
    Assertions.assertTrue(rulesByType.contains("\"rewrite_data_files\""));
    Assertions.assertTrue(rulesByType.contains("\"expire_snapshots\""));
    Assertions.assertTrue(rulesByType.contains("trigger-expr"));
  }

  @Test
  void testMergeSparkConfMapsMergesByKey() {
    Map<String, String> into = new HashMap<>();
    into.put("spark.executor.memory", "4g");
    into.put("shared", "first");
    CommitJobSpecFactory.mergeSparkConfMaps(
        into,
        "{\"spark.executor.cores\":\"2\",\"shared\":\"later\",\"spark.driver.memory\":\"2g\"}");

    Assertions.assertEquals("4g", into.get("spark.executor.memory"));
    Assertions.assertEquals("first", into.get("shared"));
    Assertions.assertEquals("2", into.get("spark.executor.cores"));
    Assertions.assertEquals("2g", into.get("spark.driver.memory"));
  }

  @Test
  void testMergeSparkConfMapsAddsWhenMissing() throws Exception {
    Map<String, String> into = new HashMap<>();
    CommitJobSpecFactory.mergeSparkConfMaps(
        into, JsonUtils.anyFieldMapper().writeValueAsString(Map.of("k", "v")));
    Assertions.assertEquals("v", into.get("k"));
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

  private static PolicyEntity compaction(long id) {
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

  private static PolicyEntity expire(long id) {
    return PolicyEntity.builder()
        .withId(id)
        .withName("e-" + id)
        .withNamespace(NamespaceUtil.ofPolicy("ml"))
        .withPolicyType(Policy.BuiltInType.ICEBERG_SNAPSHOT_EXPIRATION)
        .withEnabled(true)
        .withContent(
            PolicyContents.icebergSnapshotExpiration(
                null,
                true,
                null,
                Map.of("older_than", "7d", "retain_last", "10", "spark_executor_memory", "2g"),
                Map.of()))
        .withAuditInfo(
            AuditInfo.builder().withCreator("test").withCreateTime(Instant.EPOCH).build())
        .build();
  }
}
