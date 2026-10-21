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

import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.Config;
import org.apache.gravitino.maintenance.policy.MaintenanceAuthConfigs;
import org.apache.gravitino.maintenance.scheduler.MaintenanceJobSnapshot;
import org.apache.gravitino.maintenance.scheduler.cron.MaintenanceJobTemplates;
import org.apache.gravitino.maintenance.scheduler.cron.MaintenanceTableRefs.ResolvedTableRef;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.PolicyEntity;
import org.apache.gravitino.policy.IcebergDataCompactionContent;
import org.apache.gravitino.policy.Policy;
import org.apache.gravitino.policy.PolicyContents;
import org.apache.gravitino.utils.NamespaceUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TestCommitJobSnapshotFactory {

  private static final String GRAVITINO_URI = "http://localhost:8090";

  @Test
  void testForCommitMergesChainAndSetsMaintenanceTypes() {
    CommitJobSnapshotFactory factory = new CommitJobSnapshotFactory(configWithUri());
    CommitChainMember compaction =
        new CommitChainMember(
            Policy.BuiltInType.ICEBERG_COMPACTION, compaction(202L), "rewrite_data_files");
    CommitChainMember expire =
        new CommitChainMember(
            Policy.BuiltInType.ICEBERG_SNAPSHOT_EXPIRATION, expire(303L), "expire_snapshots");
    MaintenanceJobSnapshot snap =
        factory.forCommit(
            List.of(compaction, expire), new ResolvedTableRef(101L, "iceberg", "db.t"));

    Assertions.assertTrue(snap.isValid());
    Assertions.assertEquals(2, snap.getSteps().size());
    Assertions.assertEquals(
        "rewrite_data_files,expire_snapshots",
        snap.getJobConf().get(MaintenanceJobTemplates.MAINTENANCE_TYPES_KEY));
    Assertions.assertEquals("101", snap.getJobConf().get(MaintenanceJobTemplates.TMS_TABLE_ID_KEY));
    Assertions.assertEquals(
        "202", snap.getJobConf().get(MaintenanceJobTemplates.TMS_POLICY_ID_KEY));
  }

  private static Config configWithUri() {
    Config config = new Config(false) {};
    config.loadFromMap(
        Map.of(MaintenanceAuthConfigs.GRAVITINO_MAINTENANCE_GRAVITINO_URI.getKey(), GRAVITINO_URI),
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
        .withContent(PolicyContents.icebergSnapshotExpiration())
        .withAuditInfo(
            AuditInfo.builder().withCreator("test").withCreateTime(Instant.EPOCH).build())
        .build();
  }
}
