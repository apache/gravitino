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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.gravitino.Config;
import org.apache.gravitino.maintenance.MaintenanceJobSubmitService;
import org.apache.gravitino.maintenance.scheduler.MaintenanceJobSpec;
import org.apache.gravitino.maintenance.scheduler.MaintenanceJobSpec.Task;
import org.apache.gravitino.maintenance.scheduler.MaintenanceTaskIds;
import org.apache.gravitino.maintenance.storage.service.OccupancyClaim;
import org.apache.gravitino.maintenance.storage.service.TableMaintenanceActionMetaService;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class TestMaintenanceSparkSubmitPipeline {

  private static final long TABLE_ID = 101L;
  private static final long POLICY_ID = 202L;
  private static final String INSTANCE = MaintenanceTaskIds.forCronTable(TABLE_ID, POLICY_ID);
  private static final long CLAIM_TIMEOUT_MS = 60_000L;

  @Test
  void testUnparseableInstanceSkipsSubmit() {
    MaintenanceJobSubmitService submitter = mock(MaintenanceJobSubmitService.class);
    TableMaintenanceActionMetaService actions = mock(TableMaintenanceActionMetaService.class);
    pipeline(mockGate(false), actions, submitter).submit("not-a-cron-id", spec());
    verify(submitter, never()).runJob(anyString(), anyString(), anyMap());
    verify(actions, never()).tryAcquireInFlight(anyLong(), anyLong(), anyLong());
  }

  @Test
  void testNullSpecSkipsSubmit() {
    MaintenanceJobSubmitService submitter = mock(MaintenanceJobSubmitService.class);
    TableMaintenanceActionMetaService actions = mock(TableMaintenanceActionMetaService.class);
    pipeline(mockGate(false), actions, submitter).submit(INSTANCE, null);
    verify(submitter, never()).runJob(anyString(), anyString(), anyMap());
  }

  @Test
  void testInFlightDoesNotClaimOrSubmit() {
    MaintenanceJobSubmitService submitter = mock(MaintenanceJobSubmitService.class);
    TableMaintenanceActionMetaService actions = mock(TableMaintenanceActionMetaService.class);
    pipeline(mockGate(true), actions, submitter).submit(INSTANCE, spec());
    verify(actions, never()).tryAcquireInFlight(anyLong(), anyLong(), anyLong());
    verify(submitter, never()).runJob(anyString(), anyString(), anyMap());
  }

  @Test
  void testBatchInFlightAbortsEntirePick() {
    MaintenanceJobSubmitService submitter = mock(MaintenanceJobSubmitService.class);
    TableMaintenanceActionMetaService actions = mock(TableMaintenanceActionMetaService.class);
    CronSubmitGate gate = (tableId, policyId) -> tableId == 11L;

    MaintenanceJobSpec batch = batchSpec(10L, 11L, "expire_snapshots");

    pipeline(gate, actions, submitter)
        .submit(MaintenanceTaskIds.forCronBatch(0L, POLICY_ID), batch);

    verify(actions, never()).tryAcquireInFlight(anyLong(), anyLong(), anyLong());
    verify(submitter, never()).runJob(anyString(), anyString(), anyMap());
  }

  @Test
  void testLostClaimRaceSkipsSubmit() {
    MaintenanceJobSubmitService submitter = mock(MaintenanceJobSubmitService.class);
    TableMaintenanceActionMetaService actions = mock(TableMaintenanceActionMetaService.class);
    when(actions.tryAcquireInFlight(TABLE_ID, POLICY_ID, CLAIM_TIMEOUT_MS))
        .thenReturn(Optional.empty());

    pipeline(mockGate(false), actions, submitter).submit(INSTANCE, spec());

    verify(submitter, never()).runJob(anyString(), anyString(), anyMap());
  }

  @Test
  void testSuccessfulSubmitBindsJobId() {
    MaintenanceJobSubmitService submitter = mock(MaintenanceJobSubmitService.class);
    when(submitter.runJob(
            eq("metalake"), eq(MaintenanceJobTemplates.ICEBERG_MAINTENANCE_TEMPLATE), anyMap()))
        .thenReturn(999L);
    TableMaintenanceActionMetaService actions = mock(TableMaintenanceActionMetaService.class);
    OccupancyClaim claim = new OccupancyClaim(7L, 1_000L);
    when(actions.tryAcquireInFlight(TABLE_ID, POLICY_ID, CLAIM_TIMEOUT_MS))
        .thenReturn(Optional.of(claim));

    pipeline(mockGate(false), actions, submitter).submit(INSTANCE, spec());

    verify(actions).bindJobId(claim, 999L);
    verify(actions, never()).deleteUnboundClaim(any());
  }

  @Test
  void testSubmitFailureReleasesClaim() {
    MaintenanceJobSubmitService submitter = mock(MaintenanceJobSubmitService.class);
    when(submitter.runJob(anyString(), anyString(), anyMap()))
        .thenThrow(new RuntimeException("submit failed"));
    TableMaintenanceActionMetaService actions = mock(TableMaintenanceActionMetaService.class);
    OccupancyClaim claim = new OccupancyClaim(8L, 2_000L);
    when(actions.tryAcquireInFlight(TABLE_ID, POLICY_ID, CLAIM_TIMEOUT_MS))
        .thenReturn(Optional.of(claim));
    when(actions.deleteUnboundClaim(claim)).thenReturn(true);

    Assertions.assertDoesNotThrow(
        () -> pipeline(mockGate(false), actions, submitter).submit(INSTANCE, spec()));

    verify(actions).deleteUnboundClaim(claim);
    verify(actions, never()).bindJobId(any(), anyLong());
    verify(submitter, never()).cancelJob(anyString(), anyLong());
  }

  @Test
  void testBindFailureCancelsJobAndReleasesClaim() {
    MaintenanceJobSubmitService submitter = mock(MaintenanceJobSubmitService.class);
    when(submitter.runJob(
            eq("metalake"), eq(MaintenanceJobTemplates.ICEBERG_MAINTENANCE_TEMPLATE), anyMap()))
        .thenReturn(777L);
    TableMaintenanceActionMetaService actions = mock(TableMaintenanceActionMetaService.class);
    OccupancyClaim claim = new OccupancyClaim(9L, 3_000L);
    when(actions.tryAcquireInFlight(TABLE_ID, POLICY_ID, CLAIM_TIMEOUT_MS))
        .thenReturn(Optional.of(claim));
    doThrow(new RuntimeException("bind failed")).when(actions).bindJobId(claim, 777L);
    when(actions.deleteUnboundClaim(claim)).thenReturn(true);

    Assertions.assertDoesNotThrow(
        () -> pipeline(mockGate(false), actions, submitter).submit(INSTANCE, spec()));

    verify(submitter).cancelJob("metalake", 777L);
    verify(actions).deleteUnboundClaim(claim);
  }

  @Test
  void testBindFailureSkipsReleaseWhenClaimAlreadyReclaimed() {
    MaintenanceJobSubmitService submitter = mock(MaintenanceJobSubmitService.class);
    when(submitter.runJob(
            eq("metalake"), eq(MaintenanceJobTemplates.ICEBERG_MAINTENANCE_TEMPLATE), anyMap()))
        .thenReturn(778L);
    TableMaintenanceActionMetaService actions = mock(TableMaintenanceActionMetaService.class);
    OccupancyClaim staleClaim = new OccupancyClaim(10L, 4_000L);
    when(actions.tryAcquireInFlight(TABLE_ID, POLICY_ID, CLAIM_TIMEOUT_MS))
        .thenReturn(Optional.of(staleClaim));
    doThrow(new RuntimeException("bind lost lease")).when(actions).bindJobId(staleClaim, 778L);
    // Concurrent reclaim already moved created_at — fenced delete matches zero rows.
    when(actions.deleteUnboundClaim(staleClaim)).thenReturn(false);

    Assertions.assertDoesNotThrow(
        () -> pipeline(mockGate(false), actions, submitter).submit(INSTANCE, spec()));

    verify(submitter).cancelJob("metalake", 778L);
    verify(actions).deleteUnboundClaim(staleClaim);
    verify(actions, never()).deleteById(anyLong());
  }

  @Test
  void testBatchSubmitOneSparkJobPerTable() {
    MaintenanceJobSubmitService submitter = mock(MaintenanceJobSubmitService.class);
    when(submitter.runJob(
            eq("metalake"), eq(MaintenanceJobTemplates.ICEBERG_MAINTENANCE_TEMPLATE), anyMap()))
        .thenReturn(555L, 556L);
    TableMaintenanceActionMetaService actions = mock(TableMaintenanceActionMetaService.class);
    OccupancyClaim claimA = new OccupancyClaim(1L, 10L);
    OccupancyClaim claimB = new OccupancyClaim(2L, 20L);
    when(actions.tryAcquireInFlight(10L, POLICY_ID, CLAIM_TIMEOUT_MS))
        .thenReturn(Optional.of(claimA));
    when(actions.tryAcquireInFlight(11L, POLICY_ID, CLAIM_TIMEOUT_MS))
        .thenReturn(Optional.of(claimB));

    MaintenanceJobSpec batch = batchSpec(10L, 11L, "expire_snapshots");

    pipeline(mockGate(false), actions, submitter)
        .submit(MaintenanceTaskIds.forCronBatch(0L, POLICY_ID), batch);

    @SuppressWarnings("unchecked")
    ArgumentCaptor<Map<String, String>> confCaptor = ArgumentCaptor.forClass(Map.class);
    verify(submitter, times(2))
        .runJob(
            eq("metalake"),
            eq(MaintenanceJobTemplates.ICEBERG_MAINTENANCE_TEMPLATE),
            confCaptor.capture());
    List<Map<String, String>> confs = confCaptor.getAllValues();
    Assertions.assertEquals("iceberg", confs.get(0).get("catalog_name"));
    Assertions.assertNull(confs.get(0).get("table_identifier"));
    Assertions.assertTrue(confs.get(0).get("tables").contains("\"table_identifier\":\"db.t1\""));
    Assertions.assertTrue(confs.get(0).get("tables").contains("\"table_id\":10"));
    Assertions.assertFalse(confs.get(0).containsKey("tms_table_id"));
    Assertions.assertEquals("expire_snapshots", confs.get(0).get("maintenance_types"));
    Assertions.assertEquals("iceberg", confs.get(1).get("catalog_name"));
    Assertions.assertNull(confs.get(1).get("table_identifier"));
    Assertions.assertTrue(confs.get(1).get("tables").contains("\"table_identifier\":\"db.t2\""));
    Assertions.assertTrue(confs.get(1).get("tables").contains("\"table_id\":11"));
    Assertions.assertFalse(confs.get(1).containsKey("tms_table_id"));
    Assertions.assertFalse(confs.get(0).containsKey("tms_policy_id"));
    verify(actions).bindJobId(claimA, 555L);
    verify(actions).bindJobId(claimB, 556L);
  }

  @Test
  void testConsumesSpecJobConfWithoutRebuild() {
    MaintenanceJobSubmitService submitter = mock(MaintenanceJobSubmitService.class);
    when(submitter.runJob(anyString(), anyString(), anyMap())).thenReturn(1L);
    TableMaintenanceActionMetaService actions = mock(TableMaintenanceActionMetaService.class);
    when(actions.tryAcquireInFlight(TABLE_ID, POLICY_ID, CLAIM_TIMEOUT_MS))
        .thenReturn(Optional.of(new OccupancyClaim(1L, 1L)));

    Map<String, String> parameters = new LinkedHashMap<>();
    parameters.put("strategy", "binpack");
    parameters.put("catalog_name", "iceberg");
    parameters.put("table_identifier", "db.t");

    pipeline(mockGate(false), actions, submitter)
        .submit(
            INSTANCE, spec(parameters, "{\"spark.sql.catalog.iceberg.rest.auth.type\":\"basic\"}"));

    @SuppressWarnings("unchecked")
    ArgumentCaptor<Map<String, String>> confCaptor = ArgumentCaptor.forClass(Map.class);
    verify(submitter)
        .runJob(
            eq("metalake"),
            eq(MaintenanceJobTemplates.ICEBERG_MAINTENANCE_TEMPLATE),
            confCaptor.capture());
    Map<String, String> conf = confCaptor.getValue();
    Assertions.assertEquals("iceberg", conf.get("catalog_name"));
    Assertions.assertNull(conf.get("table_identifier"));
    Assertions.assertTrue(conf.get("tables").contains("\"table_identifier\":\"db.t\""));
    Assertions.assertTrue(conf.get("tables").contains("\"table_id\":" + TABLE_ID));
    Assertions.assertFalse(conf.containsKey("tms_table_id"));
    Assertions.assertEquals("rewrite_data_files", conf.get("maintenance_types"));
    Assertions.assertFalse(conf.containsKey("tms_policy_id"));
    Assertions.assertEquals("binpack", conf.get("strategy"));
    Assertions.assertTrue(conf.get("updater_options").contains("gravitino_uri"));
    Assertions.assertTrue(conf.get("spark_conf").contains("basic"));
  }

  @Test
  void testPolicyIdMismatchSkipsSubmit() {
    MaintenanceJobSubmitService submitter = mock(MaintenanceJobSubmitService.class);
    TableMaintenanceActionMetaService actions = mock(TableMaintenanceActionMetaService.class);
    pipeline(mockGate(false), actions, submitter)
        .submit(MaintenanceTaskIds.forCronTable(TABLE_ID, POLICY_ID + 1), spec());
    verify(submitter, never()).runJob(anyString(), anyString(), anyMap());
  }

  private static MaintenanceSparkSubmitPipeline pipeline(
      CronSubmitGate gate,
      TableMaintenanceActionMetaService actions,
      MaintenanceJobSubmitService submitter) {
    return new MaintenanceSparkSubmitPipeline(
        gate,
        actions,
        submitter,
        new MaintenanceJobConfBuilder(new Config(false) {}),
        CLAIM_TIMEOUT_MS);
  }

  private static CronSubmitGate mockGate(boolean inFlight) {
    return (tableId, policyId) -> inFlight;
  }

  private static MaintenanceJobSpec spec() {
    Map<String, String> parameters = new LinkedHashMap<>();
    parameters.put("catalog_name", "iceberg");
    parameters.put("table_identifier", "db.t");
    return spec(parameters, null);
  }

  private static MaintenanceJobSpec spec(Map<String, String> parameters) {
    return spec(parameters, null);
  }

  private static MaintenanceJobSpec spec(Map<String, String> parameters, String sparkConf) {
    Map<String, String> params = new LinkedHashMap<>(parameters);
    params.putIfAbsent("catalog_name", "iceberg");
    params.putIfAbsent("table_identifier", "db.t");
    return new MaintenanceJobSpec(
        List.of(new Task(TABLE_ID, POLICY_ID, "rewrite_data_files", null, params)),
        "{\"gravitino_uri\":\"http://localhost:8090\",\"metalake\":\"metalake\"}",
        sparkConf);
  }

  private static MaintenanceJobSpec batchSpec(long table1, long table2, String type) {
    Map<String, String> p1 = new LinkedHashMap<>();
    p1.put("catalog_name", "iceberg");
    p1.put("table_identifier", "db.t1");
    Map<String, String> p2 = new LinkedHashMap<>();
    p2.put("catalog_name", "iceberg");
    p2.put("table_identifier", "db.t2");
    return new MaintenanceJobSpec(
        List.of(
            new Task(table1, POLICY_ID, type, null, p1),
            new Task(table2, POLICY_ID, type, null, p2)),
        "{\"gravitino_uri\":\"http://localhost:8090\",\"metalake\":\"metalake\"}",
        null);
  }
}
