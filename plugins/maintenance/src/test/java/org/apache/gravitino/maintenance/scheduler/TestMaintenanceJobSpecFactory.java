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

import com.github.kagkarlsson.scheduler.serializer.JacksonSerializer;
import com.github.kagkarlsson.scheduler.serializer.Serializer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.Config;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.maintenance.policy.MaintenanceAuthConfigs;
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

class TestMaintenanceJobSpecFactory {

  private static final String GRAVITINO_URI = "http://localhost:8090";

  @Test
  void testForCronBuildsTasksAndSharedBags() {
    MaintenanceJobSpecFactory factory = new MaintenanceJobSpecFactory(configWithUri());
    PolicyEntity policy = compaction(202L);
    MaintenanceJobSpec spec =
        factory.forCron(
            policy,
            List.of(
                new ResolvedTableRef(10L, "iceberg", "db.t1"),
                new ResolvedTableRef(11L, "iceberg", "db.t2")));

    Assertions.assertTrue(spec.isValid());
    Assertions.assertEquals("ml", spec.metalake());
    Assertions.assertEquals(List.of(10L, 11L), spec.tableIds());
    Assertions.assertEquals(2, spec.getTasks().size());
    Assertions.assertEquals(202L, spec.getTasks().get(0).getPolicyId());
    Assertions.assertEquals("rewrite_data_files", spec.getTasks().get(0).getMaintenanceType());
    Assertions.assertEquals("iceberg", spec.getTasks().get(0).catalogName());
    Assertions.assertEquals("db.t1", spec.getTasks().get(0).tableIdentifier());
    Assertions.assertEquals("db.t2", spec.getTasks().get(1).tableIdentifier());
    Assertions.assertFalse(spec.getTasks().get(0).getParameters().containsKey("updater_options"));
    Assertions.assertFalse(spec.getTasks().get(0).getParameters().containsKey("spark_conf"));
    Assertions.assertFalse(
        spec.getTasks().get(0).getParameters().containsKey("spark_executor_memory"));
    Assertions.assertTrue(
        spec.getUpdaterOptions().contains("gravitino.maintenance.gravitinoAuth.uri"));
    Assertions.assertFalse(spec.getUpdaterOptions().contains(GRAVITINO_URI));
    Assertions.assertTrue(spec.getSparkConf().contains("spark.executor.memory"));

    Map<String, String> perTable =
        MaintenanceJobSpecFactory.jobConfForTask(spec, spec.getTasks().get(0));
    Assertions.assertEquals("iceberg", perTable.get("catalog_name"));
    Assertions.assertNull(perTable.get("table_identifier"));
    Assertions.assertNull(perTable.get("table_id"));
    Assertions.assertEquals(
        "rewrite_data_files", perTable.get(MaintenanceJobTemplates.MAINTENANCE_TYPES_KEY));
    Assertions.assertEquals("202", perTable.get(MaintenanceJobTemplates.POLICY_IDS_KEY));
    Assertions.assertTrue(perTable.get("tables").contains("\"table_id\":10"));
    Assertions.assertTrue(perTable.get("tables").contains("\"table_identifier\":\"db.t1\""));
    Assertions.assertTrue(perTable.containsKey("updater_options"));
    Assertions.assertEquals("4g", perTable.get("spark_executor_memory"));
  }

  @Test
  void testForCronRejectsMixedCatalogs() {
    MaintenanceJobSpecFactory factory = new MaintenanceJobSpecFactory(configWithUri());
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            factory.forCron(
                compaction(1L),
                List.of(
                    new ResolvedTableRef(10L, "iceberg", "db.t1"),
                    new ResolvedTableRef(11L, "other", "db.t2"))));
  }

  @Test
  void testJacksonRoundTrip() {
    MaintenanceJobSpecFactory factory = new MaintenanceJobSpecFactory(configWithUri());
    MaintenanceJobSpec original =
        factory.forCron(
            compaction(1L), List.of(new ResolvedTableRef(9L, "iceberg", "schema.table")));
    Serializer serializer = new JacksonSerializer(JsonUtils.anyFieldMapper());
    byte[] bytes = serializer.serialize(original);
    MaintenanceJobSpec restored = serializer.deserialize(MaintenanceJobSpec.class, bytes);
    Assertions.assertEquals(original, restored);
    String json = new String(bytes, StandardCharsets.UTF_8);
    Assertions.assertFalse(json.contains("\"version\""));
    Assertions.assertTrue(json.contains("\"tasks\""));
    Assertions.assertTrue(json.contains("\"parameters\""));
    Assertions.assertTrue(json.contains("updater_options"));
    Assertions.assertTrue(json.contains("spark_conf"));
  }

  @Test
  void testMergeResourceKeysTakesMax() {
    Map<String, String> into =
        MaintenanceJobSpecFactory.newLinkedMap(
            "spark_executor_memory", "4g", "spark_executor_cores", "2", "catalog_uri", "http://a");
    Map<String, String> from =
        MaintenanceJobSpecFactory.newLinkedMap(
            "spark_executor_memory", "7g", "spark_executor_cores", "1", "catalog_uri", "http://b");
    MaintenanceJobSpecFactory.mergeResourceKeys(into, from);
    Assertions.assertEquals("7g", into.get("spark_executor_memory"));
    Assertions.assertEquals("2", into.get("spark_executor_cores"));
    Assertions.assertEquals("http://a", into.get("catalog_uri"));
  }

  @Test
  void testMergeResourceKeysComparesMemoryUnits() {
    Map<String, String> into =
        MaintenanceJobSpecFactory.newLinkedMap("spark_executor_memory", "1g");
    Map<String, String> from =
        MaintenanceJobSpecFactory.newLinkedMap("spark_executor_memory", "512m");
    MaintenanceJobSpecFactory.mergeResourceKeys(into, from);
    Assertions.assertEquals("1g", into.get("spark_executor_memory"));

    Map<String, String> mixed =
        MaintenanceJobSpecFactory.newLinkedMap("spark_executor_memory", "512m");
    MaintenanceJobSpecFactory.mergeResourceKeys(
        mixed, MaintenanceJobSpecFactory.newLinkedMap("spark_executor_memory", "1500m"));
    Assertions.assertEquals("1500m", mixed.get("spark_executor_memory"));
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
}
