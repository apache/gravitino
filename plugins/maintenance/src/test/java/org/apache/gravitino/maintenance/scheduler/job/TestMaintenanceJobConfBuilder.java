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
package org.apache.gravitino.maintenance.scheduler.job;

import java.time.Instant;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.Config;
import org.apache.gravitino.maintenance.model.OccupancyClaim;
import org.apache.gravitino.maintenance.policy.MaintenanceAuthConfigs;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.PolicyEntity;
import org.apache.gravitino.policy.IcebergDataCompactionContent;
import org.apache.gravitino.policy.Policy;
import org.apache.gravitino.policy.PolicyContents;
import org.apache.gravitino.utils.NamespaceUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TestMaintenanceJobConfBuilder {

  private static final long POLICY_ID = 9L;
  private static final String GRAVITINO_URI = "http://localhost:8090";

  @Test
  void testCompactionMapsTypedFieldsAndIrcIntoSparkConf() {
    Config config = configWithUri();
    config.loadFromMap(
        Map.of(
            MaintenanceAuthConfigs.GRAVITINO_MAINTENANCE_GRAVITINO_AUTH_URI.getKey(),
            GRAVITINO_URI,
            "gravitino.maintenance.ircAuth.rest.auth.type",
            "basic"),
        t -> true);

    PolicyEntity policy =
        PolicyEntity.builder()
            .withId(POLICY_ID)
            .withName("c")
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
                    IcebergDataCompactionContent.REWRITE_STRATEGY_SORT,
                    "id ASC NULLS LAST",
                    Map.of("min-input-files", "4"),
                    null,
                    null,
                    null,
                    Map.of("catalog_type", "rest", "catalog_uri", "http://localhost:9001/iceberg"),
                    Map.of()))
            .withAuditInfo(
                AuditInfo.builder().withCreator("t").withCreateTime(Instant.now()).build())
            .build();

    MaintenanceJobConfBuilder.ScheduleConf built =
        new MaintenanceJobConfBuilder(config).build(policy, "iceberg");
    Map<String, String> conf = built.asMap();

    Assertions.assertEquals("sort", conf.get("strategy"));
    Assertions.assertEquals("id ASC NULLS LAST", conf.get("sort_order"));
    Assertions.assertTrue(conf.get("options").contains("min-input-files"));
    Assertions.assertNull(conf.get("where_clause"));
    Assertions.assertFalse(conf.containsKey("catalog_type"));
    Assertions.assertTrue(
        built.sparkConf().contains("\"spark.sql.catalog.iceberg.type\":\"rest\""));
    Assertions.assertTrue(
        conf.get(MaintenanceJobTemplates.RULES_KEY).contains("\"system_iceberg_compaction\""));
    Assertions.assertTrue(conf.get(MaintenanceJobTemplates.RULES_KEY).contains("trigger-expr"));
    Assertions.assertTrue(
        conf.get("spark_conf")
            .contains(
                "\"spark.sql.catalog.iceberg.rest.auth.type\":\"gravitino.maintenance.ircAuth.rest.auth.type\""));
    Assertions.assertFalse(conf.get("spark_conf").contains("\"basic\""));
    Assertions.assertFalse(conf.get("spark_conf").contains("\"rest.auth.type\""));
    Assertions.assertTrue(conf.get("updater_options").contains("\"metalake\":\"ml\""));
    Assertions.assertTrue(
        conf.get("updater_options")
            .contains("\"gravitino_uri\":\"gravitino.maintenance.gravitinoAuth.uri\""));
    Assertions.assertFalse(conf.get("updater_options").contains(GRAVITINO_URI));

    Map<String, String> resolved = new MaintenanceJobConfBuilder(config).resolveForSubmit(conf);
    Assertions.assertTrue(
        resolved.get("updater_options").contains("\"gravitino_uri\":\"" + GRAVITINO_URI + "\""));
    Assertions.assertTrue(
        resolved
            .get("spark_conf")
            .contains("\"spark.sql.catalog.iceberg.rest.auth.type\":\"basic\""));
  }

  @Test
  void testSparkResourceKeysFlattenToPlaceholdersNotCatalogProps() {
    PolicyEntity policy =
        PolicyEntity.builder()
            .withId(POLICY_ID)
            .withName("c")
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
                    IcebergDataCompactionContent.DEFAULT_REWRITE_STRATEGY,
                    IcebergDataCompactionContent.DEFAULT_SORT_ORDER,
                    null,
                    null,
                    null,
                    null,
                    Map.of(
                        "spark.executor.memory",
                        "7g",
                        "spark_executor_cores",
                        "4",
                        "catalog_type",
                        "rest"),
                    Map.of()))
            .withAuditInfo(
                AuditInfo.builder().withCreator("t").withCreateTime(Instant.now()).build())
            .build();

    Map<String, String> conf =
        new MaintenanceJobConfBuilder(configWithUri()).build(policy, "iceberg").asMap();

    Assertions.assertFalse(conf.containsKey("spark_executor_memory"));
    Assertions.assertTrue(conf.get("spark_conf").contains("\"spark.executor.memory\":\"7g\""));
    Assertions.assertTrue(conf.get("spark_conf").contains("\"spark.executor.cores\":\"4\""));
    Assertions.assertTrue(
        conf.get("spark_conf").contains("\"spark.sql.catalog.iceberg.type\":\"rest\""));
    Assertions.assertFalse(
        conf.get("spark_conf").contains("spark.sql.catalog.iceberg.spark.executor.memory"));
  }

  @Test
  void testRewriteManifestsMapsSpecIdAndUseCaching() {
    PolicyEntity policy =
        PolicyEntity.builder()
            .withId(POLICY_ID)
            .withName("m")
            .withNamespace(NamespaceUtil.ofPolicy("ml"))
            .withPolicyType(Policy.BuiltInType.ICEBERG_REWRITE_MANIFESTS)
            .withEnabled(true)
            .withContent(PolicyContents.icebergRewriteManifests(100L, 0, false))
            .withAuditInfo(
                AuditInfo.builder().withCreator("t").withCreateTime(Instant.now()).build())
            .build();

    Map<String, String> conf =
        new MaintenanceJobConfBuilder(configWithUri()).build(policy, "iceberg").asMap();

    Assertions.assertEquals("0", conf.get("spec_id"));
    Assertions.assertEquals("false", conf.get("use_caching"));
    Assertions.assertTrue(
        conf.get(MaintenanceJobTemplates.RULES_KEY)
            .contains("\"system_iceberg_rewrite_manifests\""));
    Assertions.assertTrue(
        conf.get(MaintenanceJobTemplates.RULES_KEY).contains("manifest_count_threshold"));
  }

  @Test
  void testRewriteManifestsOmitsSpecIdWhenPolicyOmitsIt() {
    PolicyEntity policy =
        PolicyEntity.builder()
            .withId(POLICY_ID)
            .withName("m-default-spec")
            .withNamespace(NamespaceUtil.ofPolicy("ml"))
            .withPolicyType(Policy.BuiltInType.ICEBERG_REWRITE_MANIFESTS)
            .withEnabled(true)
            .withContent(PolicyContents.icebergRewriteManifests())
            .withAuditInfo(
                AuditInfo.builder().withCreator("t").withCreateTime(Instant.now()).build())
            .build();

    Map<String, String> conf =
        new MaintenanceJobConfBuilder(configWithUri()).build(policy, "iceberg").asMap();

    Assertions.assertFalse(conf.containsKey("spec_id"));
    Assertions.assertFalse(conf.containsKey("use_caching"));
  }

  @Test
  void testOrphanMapsRetentionCutoff() {
    PolicyEntity policy =
        PolicyEntity.builder()
            .withId(POLICY_ID)
            .withName("o")
            .withNamespace(NamespaceUtil.ofPolicy("ml"))
            .withPolicyType(Policy.BuiltInType.ICEBERG_ORPHAN_FILE_REMOVAL)
            .withEnabled(true)
            .withContent(PolicyContents.icebergOrphanFileRemoval(7L, null, false))
            .withAuditInfo(
                AuditInfo.builder().withCreator("t").withCreateTime(Instant.now()).build())
            .build();

    Map<String, String> conf =
        new MaintenanceJobConfBuilder(configWithUri()).build(policy, "iceberg").asMap();

    Assertions.assertEquals("false", conf.get("dry_run"));
    Assertions.assertEquals("7", conf.get("older_than"));
  }

  @Test
  void testMissingGravitinoUriFailsClosed() {
    PolicyEntity policy =
        PolicyEntity.builder()
            .withId(POLICY_ID)
            .withName("c")
            .withNamespace(NamespaceUtil.ofPolicy("ml"))
            .withPolicyType(Policy.BuiltInType.ICEBERG_COMPACTION)
            .withEnabled(true)
            .withContent(PolicyContents.icebergDataCompaction())
            .withAuditInfo(
                AuditInfo.builder().withCreator("t").withCreateTime(Instant.now()).build())
            .build();

    IllegalArgumentException thrown =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> new MaintenanceJobConfBuilder(new Config(false) {}).build(policy, "iceberg"));
    Assertions.assertTrue(thrown.getMessage().contains("gravitino_uri"));
  }

  @Test
  void testPolicyUpdaterOptionsOverrideClusterUriButNotMetalake() {
    Config config = configWithUri();
    PolicyEntity policy =
        PolicyEntity.builder()
            .withId(POLICY_ID)
            .withName("c")
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
                    IcebergDataCompactionContent.DEFAULT_REWRITE_STRATEGY,
                    IcebergDataCompactionContent.DEFAULT_SORT_ORDER,
                    null,
                    null,
                    null,
                    null,
                    Map.of(),
                    Map.of("gravitino_uri", "http://policy-uri:8090", "metalake", "policy-ml")))
            .withAuditInfo(
                AuditInfo.builder().withCreator("t").withCreateTime(Instant.now()).build())
            .build();

    MaintenanceJobConfBuilder.ScheduleConf built =
        new MaintenanceJobConfBuilder(config).build(policy, "iceberg");
    Map<String, String> conf = built.asMap();

    Assertions.assertTrue(
        conf.get("updater_options").contains("\"gravitino_uri\":\"http://policy-uri:8090\""));
    // Path metalake always comes from the policy namespace, never policy updaterOptions.
    Assertions.assertTrue(conf.get("updater_options").contains("\"metalake\":\"ml\""));
    Assertions.assertFalse(conf.get("updater_options").contains("policy-ml"));
  }

  @Test
  void testFlatResourceKeysWinOverNestedSparkConf() {
    Map<String, String> jobOptions = new HashMap<>();
    jobOptions.put("spark_conf", "{\"spark.executor.memory\":\"4g\"}");
    jobOptions.put("spark_executor_memory", "7g");
    PolicyEntity policy =
        PolicyEntity.builder()
            .withId(POLICY_ID)
            .withName("c")
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
                    IcebergDataCompactionContent.DEFAULT_REWRITE_STRATEGY,
                    IcebergDataCompactionContent.DEFAULT_SORT_ORDER,
                    null,
                    null,
                    null,
                    null,
                    jobOptions,
                    Map.of()))
            .withAuditInfo(
                AuditInfo.builder().withCreator("t").withCreateTime(Instant.now()).build())
            .build();
    Map<String, String> conf =
        new MaintenanceJobConfBuilder(configWithUri()).build(policy, "iceberg").asMap();
    Assertions.assertTrue(conf.get("spark_conf").contains("\"spark.executor.memory\":\"7g\""));
    Assertions.assertFalse(conf.get("spark_conf").contains("\"spark.executor.memory\":\"4g\""));
  }

  @Test
  void testApplyActionIdsFromClaimsStampsTasks() {
    Map<String, String> jobConf = new LinkedHashMap<>();
    jobConf.put(
        MaintenanceJobTemplates.TASKS_KEY,
        "[{\"table_id\":1,\"policy_type\":\"system_iceberg_compaction\",\"policy_id\":1,"
            + "\"params\":{\"catalog_name\":\"c\"},\"rules\":{}}]");
    Map<String, String> stamped =
        MaintenanceJobConfBuilder.applyActionIdsFromClaims(
            jobConf, List.of(new OccupancyClaim(42L, 100L)));
    Assertions.assertTrue(
        stamped.get(MaintenanceJobTemplates.TASKS_KEY).contains("\"action_id\":42"));
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
}
