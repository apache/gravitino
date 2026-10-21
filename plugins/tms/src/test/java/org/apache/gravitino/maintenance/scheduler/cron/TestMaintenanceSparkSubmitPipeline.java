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

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.gravitino.maintenance.scheduler.ExpandOccupancyGate;
import org.apache.gravitino.maintenance.scheduler.MaintenanceJobSnapshot;
import org.apache.gravitino.maintenance.scheduler.MaintenanceJobSnapshot.ChainStep;
import org.apache.gravitino.maintenance.scheduler.MaintenanceJobSnapshot.TableTarget;
import org.apache.gravitino.maintenance.storage.service.OccupancyClaim;
import org.apache.gravitino.maintenance.storage.service.TableMaintenanceActionMetaService;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class TestMaintenanceSparkSubmitPipeline {

  private static final long TABLE_ID = 101L;
  private static final long POLICY_ID = 202L;
  private static final String INSTANCE = CronJobIds.forTable(TABLE_ID, POLICY_ID);
  private static final long CLAIM_TIMEOUT_MS = 60_000L;

  @Test
  void testUnparseableInstanceSkipsSubmit() {
    MaintenanceJobSubmitter submitter = mock(MaintenanceJobSubmitter.class);
    TableMaintenanceActionMetaService actions = mock(TableMaintenanceActionMetaService.class);
    pipeline(mockGate(false), actions, submitter).submit("not-a-cron-id", snapshot());
    verify(submitter, never()).runJob(anyString(), anyString(), anyMap());
    verify(actions, never()).tryAcquireInFlight(anyLong(), anyLong(), anyLong());
  }

  @Test
  void testNullSnapshotSkipsSubmit() {
    MaintenanceJobSubmitter submitter = mock(MaintenanceJobSubmitter.class);
    TableMaintenanceActionMetaService actions = mock(TableMaintenanceActionMetaService.class);
    pipeline(mockGate(false), actions, submitter).submit(INSTANCE, null);
    verify(submitter, never()).runJob(anyString(), anyString(), anyMap());
  }

  @Test
  void testShouldSkipDoesNotClaimOrSubmit() {
    MaintenanceJobSubmitter submitter = mock(MaintenanceJobSubmitter.class);
    TableMaintenanceActionMetaService actions = mock(TableMaintenanceActionMetaService.class);
    pipeline(mockGate(true), actions, submitter).submit(INSTANCE, snapshot());
    verify(actions, never()).tryAcquireInFlight(anyLong(), anyLong(), anyLong());
    verify(submitter, never()).runJob(anyString(), anyString(), anyMap());
  }

  @Test
  void testLostClaimRaceSkipsSubmit() {
    MaintenanceJobSubmitter submitter = mock(MaintenanceJobSubmitter.class);
    TableMaintenanceActionMetaService actions = mock(TableMaintenanceActionMetaService.class);
    when(actions.tryAcquireInFlight(TABLE_ID, POLICY_ID, CLAIM_TIMEOUT_MS))
        .thenReturn(Optional.empty());

    pipeline(mockGate(false), actions, submitter).submit(INSTANCE, snapshot());

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

    pipeline(mockGate(false), actions, submitter).submit(INSTANCE, snapshot());

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
        () -> pipeline(mockGate(false), actions, submitter).submit(INSTANCE, snapshot()));

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
        () -> pipeline(mockGate(false), actions, submitter).submit(INSTANCE, snapshot()));

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

    Map<String, String> shared = new LinkedHashMap<>();
    shared.put(MaintenanceJobTemplates.MAINTENANCE_TYPE_KEY, "expire_snapshots");
    shared.put(MaintenanceJobTemplates.TMS_POLICY_ID_KEY, Long.toString(POLICY_ID));
    shared.put("updater_options", "{\"gravitino_uri\":\"http://localhost:8090\"}");

    MaintenanceJobSnapshot batch =
        new MaintenanceJobSnapshot(
            "metalake",
            List.of(
                new TableTarget(10L, List.of("metalake", "iceberg", "db", "t1")),
                new TableTarget(11L, List.of("metalake", "iceberg", "db", "t2"))),
            List.of(new ChainStep(POLICY_ID, "expire_snapshots")),
            shared);

    pipeline(mockGate(false), actions, submitter).submit(CronJobIds.forBatch(0L, POLICY_ID), batch);

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
    Assertions.assertEquals(Long.toString(POLICY_ID), confs.get(0).get("tms_policy_id"));
    verify(actions).bindJobId(claimA, 555L);
    verify(actions).bindJobId(claimB, 556L);
  }

  @Test
  void testConsumesSnapshotJobConfWithoutRebuild() {
    MaintenanceJobSubmitter submitter = mock(MaintenanceJobSubmitter.class);
    when(submitter.runJob(anyString(), anyString(), anyMap())).thenReturn(1L);
    TableMaintenanceActionMetaService actions = mock(TableMaintenanceActionMetaService.class);
    when(actions.tryAcquireInFlight(TABLE_ID, POLICY_ID, CLAIM_TIMEOUT_MS))
        .thenReturn(Optional.of(new OccupancyClaim(1L, 1L)));

    Map<String, String> jobConf = new LinkedHashMap<>();
    jobConf.put(MaintenanceJobTemplates.MAINTENANCE_TYPE_KEY, "rewrite_data_files");
    jobConf.put(MaintenanceJobTemplates.TMS_POLICY_ID_KEY, Long.toString(POLICY_ID));
    jobConf.put("strategy", "binpack");
    jobConf.put(
        "updater_options",
        "{\"gravitino_uri\":\"http://localhost:8090\",\"metalake\":\"metalake\"}");
    jobConf.put("spark_conf", "{\"spark.sql.catalog.iceberg.rest.auth.type\":\"basic\"}");

    pipeline(mockGate(false), actions, submitter).submit(INSTANCE, snapshot(jobConf));

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
    Assertions.assertEquals(Long.toString(TABLE_ID), conf.get("tms_table_id"));
    Assertions.assertEquals("rewrite_data_files", conf.get("maintenance_type"));
    Assertions.assertEquals(Long.toString(POLICY_ID), conf.get("tms_policy_id"));
    Assertions.assertEquals("binpack", conf.get("strategy"));
    Assertions.assertTrue(conf.get("updater_options").contains("gravitino_uri"));
    Assertions.assertTrue(conf.get("spark_conf").contains("basic"));
  }

  @Test
  void testPolicyIdMismatchSkipsSubmit() {
    MaintenanceJobSubmitter submitter = mock(MaintenanceJobSubmitter.class);
    TableMaintenanceActionMetaService actions = mock(TableMaintenanceActionMetaService.class);
    pipeline(mockGate(false), actions, submitter)
        .submit(CronJobIds.forTable(TABLE_ID, POLICY_ID + 1), snapshot());
    verify(submitter, never()).runJob(anyString(), anyString(), anyMap());
  }

  private static MaintenanceSparkSubmitPipeline pipeline(
      ExpandOccupancyGate gate,
      TableMaintenanceActionMetaService actions,
      MaintenanceJobSubmitter submitter) {
    return new MaintenanceSparkSubmitPipeline(gate, actions, submitter, CLAIM_TIMEOUT_MS);
  }

  private static ExpandOccupancyGate mockGate(boolean shouldSkip) {
    return (tableId, policyId, minIntervalMs, nowMs) -> shouldSkip;
  }

  private static MaintenanceJobSnapshot snapshot() {
    Map<String, String> jobConf = new LinkedHashMap<>();
    jobConf.put(MaintenanceJobTemplates.MAINTENANCE_TYPE_KEY, "rewrite_data_files");
    jobConf.put(MaintenanceJobTemplates.TMS_POLICY_ID_KEY, Long.toString(POLICY_ID));
    jobConf.put("updater_options", "{\"gravitino_uri\":\"http://localhost:8090\"}");
    return snapshot(jobConf);
  }

  private static MaintenanceJobSnapshot snapshot(Map<String, String> jobConf) {
    return new MaintenanceJobSnapshot(
        "metalake",
        List.of(new TableTarget(TABLE_ID, List.of("metalake", "iceberg", "db", "t"))),
        List.of(new ChainStep(POLICY_ID, "rewrite_data_files")),
        jobConf);
  }
}
