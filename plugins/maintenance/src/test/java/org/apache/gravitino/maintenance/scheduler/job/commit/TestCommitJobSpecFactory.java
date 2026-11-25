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
import org.apache.gravitino.maintenance.model.TableCandidate;
import org.apache.gravitino.maintenance.optimizer.common.util.IcebergSparkConfigUtils;
import org.apache.gravitino.maintenance.policy.MaintenanceAuthConfigs;
import org.apache.gravitino.maintenance.scheduler.MaintenanceJobSpec;
import org.apache.gravitino.maintenance.scheduler.MaintenanceJobSpecFactory;
import org.apache.gravitino.maintenance.scheduler.job.MaintenanceJobTemplates;
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
    MaintenanceJobSpecFactory factory = new MaintenanceJobSpecFactory(configWithUri());
    MaintenanceJobSpec spec =
        factory.forCommit(
            List.of(
                new TableCandidate(
                    101L, "iceberg", "db.t", List.of(compaction(202L), expire(303L)))));

    Assertions.assertTrue(spec.isValid());
    Assertions.assertEquals(2, spec.getTasks().size());
    Assertions.assertEquals(101L, spec.getTasks().get(0).getTableId());
    Assertions.assertEquals(202L, spec.getTasks().get(0).getPolicyId());
    Assertions.assertEquals("system_iceberg_compaction", spec.getTasks().get(0).getPolicyType());
    Assertions.assertEquals(
        "system_iceberg_snapshot_expiration", spec.getTasks().get(1).getPolicyType());
    String olderThan = spec.getTasks().get(1).getParameters().get("older_than");
    Assertions.assertEquals("7", olderThan);
    Assertions.assertEquals("10", spec.getTasks().get(1).getParameters().get("retain_last"));
    Assertions.assertTrue(spec.getUpdaterOptions().contains("metalake"));
    Assertions.assertTrue(spec.getSparkConf().contains("spark.executor.memory"));
    Map<String, String> spark =
        IcebergSparkConfigUtils.parseFlatJsonMap(spec.getSparkConf(), "spark_conf");
    Assertions.assertEquals("4g", spark.get("spark.executor.memory"));

    Map<String, String> runJob = MaintenanceJobSpecFactory.jobConfForSpec(spec);
    String tasksJson = runJob.get(MaintenanceJobTemplates.TASKS_KEY);
    Assertions.assertNotNull(tasksJson);
    Assertions.assertTrue(tasksJson.contains("system_iceberg_compaction"));
    Assertions.assertTrue(tasksJson.contains("system_iceberg_snapshot_expiration"));
    Assertions.assertTrue(tasksJson.contains("\"policy_id\":202"));
    Assertions.assertTrue(tasksJson.contains("\"policy_id\":303"));
    Assertions.assertTrue(tasksJson.contains("\"older_than\":\"" + olderThan + "\""));
    Assertions.assertTrue(tasksJson.contains("\"retain_last\":\"10\""));
    Assertions.assertFalse(tasksJson.contains("\"action_id\""));
    Assertions.assertTrue(tasksJson.contains("trigger-expr"));
    Assertions.assertNull(runJob.get("older_than"));
    Assertions.assertFalse(runJob.containsKey("spark_executor_memory"));
    Assertions.assertTrue(runJob.get("spark_conf").contains("spark.executor.memory"));
  }

  @Test
  void testMergeSparkConfMaps() throws Exception {
    Map<String, String> into = new HashMap<>();
    into.put("spark.executor.memory", "4g");
    into.put("shared", "first");
    MaintenanceJobSpecFactory.mergeSparkConfMaps(
        into,
        "{\"spark.executor.cores\":\"2\",\"shared\":\"later\",\"spark.driver.memory\":\"2g\"}");
    Assertions.assertEquals("4g", into.get("spark.executor.memory"));
    Assertions.assertEquals("first", into.get("shared"));
    Assertions.assertEquals("2", into.get("spark.executor.cores"));
    Assertions.assertEquals("2g", into.get("spark.driver.memory"));

    Map<String, String> empty = new HashMap<>();
    MaintenanceJobSpecFactory.mergeSparkConfMaps(
        empty, JsonUtils.anyFieldMapper().writeValueAsString(Map.of("k", "v")));
    Assertions.assertEquals("v", empty.get("k"));
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
                7L, 10L, true, null, true, null, Map.of("spark_executor_memory", "2g"), Map.of()))
        .withAuditInfo(
            AuditInfo.builder().withCreator("test").withCreateTime(Instant.EPOCH).build())
        .build();
  }
}
