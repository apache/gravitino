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
package org.apache.gravitino.maintenance.scheduler.cron;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.gravitino.Config;
import org.apache.gravitino.maintenance.policy.MaintenanceAuthConfigs;
import org.apache.gravitino.maintenance.scheduler.ExpandOccupancyGate;
import org.apache.gravitino.maintenance.scheduler.PolicyLookup;
import org.apache.gravitino.maintenance.scheduler.cron.MaintenanceTableRefs.ResolvedTableRef;
import org.apache.gravitino.maintenance.storage.service.OccupancyClaim;
import org.apache.gravitino.maintenance.storage.service.TableMaintenanceActionMetaService;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.PolicyEntity;
import org.apache.gravitino.policy.IcebergDataCompactionContent;
import org.apache.gravitino.policy.Policy;
import org.apache.gravitino.policy.PolicyContents;
import org.apache.gravitino.utils.NamespaceUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class TestMaintenanceSparkSubmitPipeline {

  private static final long TABLE_ID = 101L;
  private static final long POLICY_ID = 202L;
  private static final String INSTANCE = CronJobIds.forTable(TABLE_ID, POLICY_ID);
  private static final long CLAIM_TIMEOUT_MS = 60_000L;
  private static final String GRAVITINO_URI = "http://localhost:8090";

  @Test
  void testUnparseableInstanceSkipsSubmit() {
    MaintenanceJobSubmitter submitter = mock(MaintenanceJobSubmitter.class);
    TableMaintenanceActionMetaService actions = mock(TableMaintenanceActionMetaService.class);
    pipeline(mockLookupEmpty(), mockGate(false), actions, submitter).submit("not-a-cron-id");
    verify(submitter, never()).runJob(anyString(), anyString(), anyMap());
    verify(actions, never()).tryAcquireInFlight(anyLong(), anyLong(), anyLong());
  }

  @Test
  void testMissingPolicySkipsSubmit() {
    MaintenanceJobSubmitter submitter = mock(MaintenanceJobSubmitter.class);
    TableMaintenanceActionMetaService actions = mock(TableMaintenanceActionMetaService.class);
    pipeline(mockLookupEmpty(), mockGate(false), actions, submitter).submit(INSTANCE);
    verify(submitter, never()).runJob(anyString(), anyString(), anyMap());
  }

  @Test
  void testShouldSkipDoesNotClaimOrSubmit() {
    MaintenanceJobSubmitter submitter = mock(MaintenanceJobSubmitter.class);
    TableMaintenanceActionMetaService actions = mock(TableMaintenanceActionMetaService.class);
    pipeline(mockLookup(policy(true)), mockGate(true), actions, submitter).submit(INSTANCE);
    verify(actions, never()).tryAcquireInFlight(anyLong(), anyLong(), anyLong());
    verify(submitter, never()).runJob(anyString(), anyString(), anyMap());
  }

  @Test
  void testLostClaimRaceSkipsSubmit() {
    MaintenanceJobSubmitter submitter = mock(MaintenanceJobSubmitter.class);
    TableMaintenanceActionMetaService actions = mock(TableMaintenanceActionMetaService.class);
    when(actions.tryAcquireInFlight(TABLE_ID, POLICY_ID, CLAIM_TIMEOUT_MS))
        .thenReturn(Optional.empty());

    pipeline(mockLookup(policy(true)), mockGate(false), actions, submitter).submit(INSTANCE);

    verify(submitter, never()).runJob(anyString(), anyString(), anyMap());
  }

  @Test
  void testSuccessfulSubmitBindsJobId() {
    MaintenanceJobSubmitter submitter = mock(MaintenanceJobSubmitter.class);
    when(submitter.runJob(
            eq("metalake"), eq(MaintenanceJobTemplates.TMS_ICEBERG_MAINTENANCE_TEMPLATE), anyMap()))
        .thenReturn(999L);
    TableMaintenanceActionMetaService actions = mock(TableMaintenanceActionMetaService.class);
    OccupancyClaim claim = new OccupancyClaim(7L, 1_000L);
    when(actions.tryAcquireInFlight(TABLE_ID, POLICY_ID, CLAIM_TIMEOUT_MS))
        .thenReturn(Optional.of(claim));

    pipeline(mockLookup(policy(true)), mockGate(false), actions, submitter).submit(INSTANCE);

    verify(actions).bindJobId(claim, 999L);
    verify(actions, never()).deleteById(anyLong());
  }

  @Test
  void testSubmitFailureReleasesClaim() {
    MaintenanceJobSubmitter submitter = mock(MaintenanceJobSubmitter.class);
    when(submitter.runJob(anyString(), anyString(), anyMap()))
        .thenThrow(new RuntimeException("submit failed"));
    TableMaintenanceActionMetaService actions = mock(TableMaintenanceActionMetaService.class);
    OccupancyClaim claim = new OccupancyClaim(8L, 2_000L);
    when(actions.tryAcquireInFlight(TABLE_ID, POLICY_ID, CLAIM_TIMEOUT_MS))
        .thenReturn(Optional.of(claim));

    Assertions.assertDoesNotThrow(
        () ->
            pipeline(mockLookup(policy(true)), mockGate(false), actions, submitter)
                .submit(INSTANCE));

    verify(actions).deleteById(8L);
    verify(actions, never()).bindJobId(any(), anyLong());
    verify(submitter, never()).cancelJob(anyString(), anyLong());
  }

  @Test
  void testBindFailureCancelsJobAndReleasesClaim() {
    MaintenanceJobSubmitter submitter = mock(MaintenanceJobSubmitter.class);
    when(submitter.runJob(
            eq("metalake"), eq(MaintenanceJobTemplates.TMS_ICEBERG_MAINTENANCE_TEMPLATE), anyMap()))
        .thenReturn(777L);
    TableMaintenanceActionMetaService actions = mock(TableMaintenanceActionMetaService.class);
    OccupancyClaim claim = new OccupancyClaim(9L, 3_000L);
    when(actions.tryAcquireInFlight(TABLE_ID, POLICY_ID, CLAIM_TIMEOUT_MS))
        .thenReturn(Optional.of(claim));
    org.mockito.Mockito.doThrow(new RuntimeException("bind failed"))
        .when(actions)
        .bindJobId(claim, 777L);

    Assertions.assertDoesNotThrow(
        () ->
            pipeline(mockLookup(policy(true)), mockGate(false), actions, submitter)
                .submit(INSTANCE));

    verify(submitter).cancelJob("metalake", 777L);
    verify(actions).deleteById(9L);
  }

  @Test
  void testBatchSubmitOneSparkJobPerTable() {
    MaintenanceJobSubmitter submitter = mock(MaintenanceJobSubmitter.class);
    when(submitter.runJob(
            eq("metalake"), eq(MaintenanceJobTemplates.TMS_ICEBERG_MAINTENANCE_TEMPLATE), anyMap()))
        .thenReturn(555L, 556L);
    TableMaintenanceActionMetaService actions = mock(TableMaintenanceActionMetaService.class);
    OccupancyClaim claimA = new OccupancyClaim(1L, 10L);
    OccupancyClaim claimB = new OccupancyClaim(2L, 20L);
    when(actions.tryAcquireInFlight(10L, POLICY_ID, CLAIM_TIMEOUT_MS))
        .thenReturn(Optional.of(claimA));
    when(actions.tryAcquireInFlight(11L, POLICY_ID, CLAIM_TIMEOUT_MS))
        .thenReturn(Optional.of(claimB));

    PolicyEntity snapshotPolicy =
        PolicyEntity.builder()
            .withId(POLICY_ID)
            .withName("snap")
            .withNamespace(NamespaceUtil.ofPolicy("metalake"))
            .withPolicyType(Policy.BuiltInType.ICEBERG_SNAPSHOT_EXPIRATION)
            .withEnabled(true)
            .withContent(PolicyContents.icebergSnapshotExpiration())
            .withAuditInfo(
                AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build())
            .build();

    new MaintenanceSparkSubmitPipeline(
            mockLookup(snapshotPolicy),
            mockGate(false),
            actions,
            submitter,
            configWithUri(),
            CLAIM_TIMEOUT_MS,
            ids ->
                List.of(
                    new ResolvedTableRef(10L, "iceberg", "db.t1"),
                    new ResolvedTableRef(11L, "iceberg", "db.t2")))
        .submit(CronJobIds.forBatch(0L, POLICY_ID), new CronJobData(List.of(10L, 11L)));

    @SuppressWarnings("unchecked")
    ArgumentCaptor<Map<String, String>> confCaptor = ArgumentCaptor.forClass(Map.class);
    verify(submitter, times(2))
        .runJob(
            eq("metalake"),
            eq(MaintenanceJobTemplates.TMS_ICEBERG_MAINTENANCE_TEMPLATE),
            confCaptor.capture());
    List<Map<String, String>> confs = confCaptor.getAllValues();
    Assertions.assertEquals("iceberg", confs.get(0).get("catalog_name"));
    Assertions.assertEquals("db.t1", confs.get(0).get("table_identifier"));
    Assertions.assertEquals("10", confs.get(0).get("tms_table_id"));
    Assertions.assertEquals("expire_snapshots", confs.get(0).get("maintenance_type"));
    Assertions.assertEquals("iceberg", confs.get(1).get("catalog_name"));
    Assertions.assertEquals("db.t2", confs.get(1).get("table_identifier"));
    Assertions.assertEquals("11", confs.get(1).get("tms_table_id"));
    Assertions.assertNull(confs.get(0).get("table_identifiers"));
    Assertions.assertEquals(Long.toString(POLICY_ID), confs.get(0).get("tms_policy_id"));
    verify(actions).bindJobId(claimA, 555L);
    verify(actions).bindJobId(claimB, 556L);
  }

  @Test
  void testJobConfOverlaysAuthAndCarriesIds() {
    MaintenanceJobSubmitter submitter = mock(MaintenanceJobSubmitter.class);
    when(submitter.runJob(anyString(), anyString(), anyMap())).thenReturn(1L);
    TableMaintenanceActionMetaService actions = mock(TableMaintenanceActionMetaService.class);
    when(actions.tryAcquireInFlight(TABLE_ID, POLICY_ID, CLAIM_TIMEOUT_MS))
        .thenReturn(Optional.of(new OccupancyClaim(1L, 1L)));

    Config config = configWithUri();
    config.loadFromMap(
        Map.of(
            MaintenanceAuthConfigs.GRAVITINO_MAINTENANCE_GRAVITINO_URI.getKey(),
            GRAVITINO_URI,
            "gravitino.maintenance.ircAuth.rest.auth.type",
            "basic",
            "gravitino.maintenance.gravitinoAuth.authType",
            "simple",
            "gravitino.maintenance.gravitinoAuth.username",
            "admin"),
        t -> true);

    PolicyEntity policy =
        policy(true, Map.of("catalog_name", "iceberg", "table_identifier", "db.t"), Map.of());

    new MaintenanceSparkSubmitPipeline(
            mockLookup(policy),
            mockGate(false),
            actions,
            submitter,
            config,
            CLAIM_TIMEOUT_MS,
            ids -> List.of(new ResolvedTableRef(TABLE_ID, "iceberg", "db.t")))
        .submit(INSTANCE);

    @SuppressWarnings("unchecked")
    ArgumentCaptor<Map<String, String>> confCaptor = ArgumentCaptor.forClass(Map.class);
    verify(submitter)
        .runJob(
            eq("metalake"),
            eq(MaintenanceJobTemplates.TMS_ICEBERG_MAINTENANCE_TEMPLATE),
            confCaptor.capture());
    Map<String, String> conf = confCaptor.getValue();
    Assertions.assertEquals("iceberg", conf.get("catalog_name"));
    Assertions.assertEquals("db.t", conf.get("table_identifier"));
    Assertions.assertNull(conf.get("rest.auth.type"));
    Assertions.assertTrue(conf.containsKey("spark_conf"));
    Assertions.assertTrue(
        conf.get("spark_conf").contains("spark.sql.catalog.iceberg.rest.auth.type"));
    Assertions.assertTrue(conf.get("spark_conf").contains("basic"));
    Assertions.assertEquals("binpack", conf.get("strategy"));
    Assertions.assertNull(conf.get("where_clause"));
    Assertions.assertEquals(Long.toString(TABLE_ID), conf.get("tms_table_id"));
    Assertions.assertEquals("rewrite_data_files", conf.get("maintenance_type"));
    Assertions.assertEquals(Long.toString(POLICY_ID), conf.get("tms_policy_id"));
    Assertions.assertTrue(conf.containsKey("updater_options"));
    Assertions.assertTrue(conf.get("updater_options").contains("auth_type"));
    Assertions.assertTrue(conf.get("updater_options").contains("\"metalake\":\"metalake\""));
    Assertions.assertTrue(
        conf.get("updater_options").contains("\"gravitino_uri\":\"" + GRAVITINO_URI + "\""));
  }

  private static MaintenanceSparkSubmitPipeline pipeline(
      PolicyLookup lookup,
      ExpandOccupancyGate gate,
      TableMaintenanceActionMetaService actions,
      MaintenanceJobSubmitter submitter) {
    return new MaintenanceSparkSubmitPipeline(
        lookup,
        gate,
        actions,
        submitter,
        configWithUri(),
        CLAIM_TIMEOUT_MS,
        ids -> List.of(new ResolvedTableRef(TABLE_ID, "iceberg", "db.t")));
  }

  private static Config configWithUri() {
    Config config = new Config(false) {};
    config.loadFromMap(
        Map.of(MaintenanceAuthConfigs.GRAVITINO_MAINTENANCE_GRAVITINO_URI.getKey(), GRAVITINO_URI),
        t -> true);
    return config;
  }

  private static ExpandOccupancyGate mockGate(boolean shouldSkip) {
    return (tableId, policyId, minIntervalMs, nowMs) -> shouldSkip;
  }

  private static PolicyLookup mockLookupEmpty() {
    PolicyLookup lookup = mock(PolicyLookup.class);
    when(lookup.findById(anyLong())).thenReturn(Optional.empty());
    return lookup;
  }

  private static PolicyLookup mockLookup(PolicyEntity policy) {
    PolicyLookup lookup = mock(PolicyLookup.class);
    when(lookup.findById(POLICY_ID)).thenReturn(Optional.of(policy));
    return lookup;
  }

  private static PolicyEntity policy(boolean enabled) {
    return policy(enabled, Collections.emptyMap(), Collections.emptyMap());
  }

  private static PolicyEntity policy(
      boolean enabled, Map<String, String> jobOptions, Map<String, String> updaterOptions) {
    return PolicyEntity.builder()
        .withId(POLICY_ID)
        .withName("p")
        .withNamespace(NamespaceUtil.ofPolicy("metalake"))
        .withPolicyType(Policy.BuiltInType.ICEBERG_COMPACTION)
        .withComment("c")
        .withEnabled(enabled)
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
                updaterOptions))
        .withAuditInfo(
            AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build())
        .build();
  }
}
