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
import static org.mockito.ArgumentMatchers.anyList;
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
import java.util.function.BiPredicate;
import org.apache.gravitino.Config;
import org.apache.gravitino.maintenance.MaintenanceJobManager;
import org.apache.gravitino.maintenance.occupancy.OccupancyClaim;
import org.apache.gravitino.maintenance.occupancy.OccupancyGate;
import org.apache.gravitino.maintenance.scheduler.MaintenanceJobSpec;
import org.apache.gravitino.maintenance.scheduler.MaintenanceJobSpec.Task;
import org.apache.gravitino.maintenance.scheduler.MaintenanceTaskIds;
import org.apache.gravitino.maintenance.scheduler.job.MaintenanceJobConfBuilder;
import org.apache.gravitino.maintenance.storage.service.TableMaintenanceActionMetaService;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class TestMaintenanceCronSubmitPipeline {

  private static final long TABLE_ID = 101L;
  private static final long POLICY_ID = 202L;
  private static final String INSTANCE = MaintenanceTaskIds.forCronTable(TABLE_ID, POLICY_ID);
  private static final long CLAIM_TIMEOUT_MS = 60_000L;
  private static final String UPDATER =
      "{\"gravitino_uri\":\"http://localhost:8090\",\"metalake\":\"metalake\"}";

  @Test
  void testEarlyExitsSkipClaimAndSubmit() {
    MaintenanceJobManager mgr = mock(MaintenanceJobManager.class);
    TableMaintenanceActionMetaService actions = mock(TableMaintenanceActionMetaService.class);

    pipeline(false, actions, mgr).submit("not-a-cron-id", spec());
    pipeline(false, actions, mgr).submit(INSTANCE, null);
    pipeline(false, actions, mgr)
        .submit(MaintenanceTaskIds.forCronTable(TABLE_ID, POLICY_ID + 1), spec());
    pipeline(true, actions, mgr).submit(INSTANCE, spec());
    pipeline((t, p) -> t == 11L, actions, mgr)
        .submit(MaintenanceTaskIds.forCronBatch(0L, POLICY_ID), batchSpec(10L, 11L));

    verify(actions, never()).tryAcquireInFlight(anyLong(), anyLong(), anyLong());
    verify(mgr, never()).submitAndBind(anyString(), anyMap(), anyList());
  }

  @Test
  void testLostClaimRaceSkipsSubmit() {
    MaintenanceJobManager mgr = mock(MaintenanceJobManager.class);
    TableMaintenanceActionMetaService actions = mock(TableMaintenanceActionMetaService.class);
    when(actions.tryAcquireInFlight(TABLE_ID, POLICY_ID, CLAIM_TIMEOUT_MS))
        .thenReturn(Optional.empty());
    pipeline(false, actions, mgr).submit(INSTANCE, spec());
    verify(mgr, never()).submitAndBind(anyString(), anyMap(), anyList());
  }

  @Test
  void testSuccessfulSubmitCallsSubmitAndBind() {
    MaintenanceJobManager mgr = mock(MaintenanceJobManager.class);
    OccupancyClaim claim = new OccupancyClaim(7L, 1_000L);
    when(mgr.submitAndBind(eq("metalake"), anyMap(), eq(List.of(claim))))
        .thenReturn(Optional.of(999L));
    TableMaintenanceActionMetaService actions = mock(TableMaintenanceActionMetaService.class);
    when(actions.tryAcquireInFlight(TABLE_ID, POLICY_ID, CLAIM_TIMEOUT_MS))
        .thenReturn(Optional.of(claim));

    pipeline(false, actions, mgr).submit(INSTANCE, spec());

    verify(mgr).submitAndBind(eq("metalake"), anyMap(), eq(List.of(claim)));
    verify(actions, never()).deleteUnboundClaim(any());
  }

  @Test
  void testConfResolveFailureReleasesClaimWithoutSubmit() {
    MaintenanceJobManager mgr = mock(MaintenanceJobManager.class);
    TableMaintenanceActionMetaService actions = mock(TableMaintenanceActionMetaService.class);
    OccupancyClaim claim = new OccupancyClaim(8L, 2_000L);
    when(actions.tryAcquireInFlight(TABLE_ID, POLICY_ID, CLAIM_TIMEOUT_MS))
        .thenReturn(Optional.of(claim));
    when(actions.deleteUnboundClaim(claim)).thenReturn(true);

    Assertions.assertDoesNotThrow(
        () -> pipeline(false, actions, mgr).submit(INSTANCE, spec(Map.of(), "{not-json")));
    verify(actions).deleteUnboundClaim(claim);
    verify(mgr, never()).submitAndBind(anyString(), anyMap(), anyList());
  }

  @Test
  void testBatchSubmitOneSparkJobPerTable() {
    MaintenanceJobManager mgr = mock(MaintenanceJobManager.class);
    when(mgr.submitAndBind(anyString(), anyMap(), anyList()))
        .thenReturn(Optional.of(555L), Optional.of(556L));
    TableMaintenanceActionMetaService actions = mock(TableMaintenanceActionMetaService.class);
    OccupancyClaim claimA = new OccupancyClaim(1L, 10L);
    OccupancyClaim claimB = new OccupancyClaim(2L, 20L);
    when(actions.tryAcquireInFlight(10L, POLICY_ID, CLAIM_TIMEOUT_MS))
        .thenReturn(Optional.of(claimA));
    when(actions.tryAcquireInFlight(11L, POLICY_ID, CLAIM_TIMEOUT_MS))
        .thenReturn(Optional.of(claimB));

    pipeline(false, actions, mgr)
        .submit(MaintenanceTaskIds.forCronBatch(0L, POLICY_ID), batchSpec(10L, 11L));

    @SuppressWarnings("unchecked")
    ArgumentCaptor<Map<String, String>> confs = ArgumentCaptor.forClass(Map.class);
    @SuppressWarnings("unchecked")
    ArgumentCaptor<List<OccupancyClaim>> claims = ArgumentCaptor.forClass(List.class);
    verify(mgr, times(2)).submitAndBind(eq("metalake"), confs.capture(), claims.capture());
    Assertions.assertTrue(confs.getAllValues().get(0).get("tables").contains("db.t1"));
    Assertions.assertTrue(confs.getAllValues().get(1).get("tables").contains("db.t2"));
    Assertions.assertEquals(List.of(List.of(claimA), List.of(claimB)), claims.getAllValues());
  }

  @Test
  void testTableInstanceSubmitAllowsAbsorbedPeerTables() {
    long peerTable = TABLE_ID + 1;
    long peerPolicy = POLICY_ID + 1;
    MaintenanceJobManager mgr = mock(MaintenanceJobManager.class);
    when(mgr.submitAndBind(anyString(), anyMap(), anyList()))
        .thenReturn(Optional.of(1L), Optional.of(2L));
    TableMaintenanceActionMetaService actions = mock(TableMaintenanceActionMetaService.class);
    when(actions.tryAcquireInFlight(TABLE_ID, POLICY_ID, CLAIM_TIMEOUT_MS))
        .thenReturn(Optional.of(new OccupancyClaim(1L, 10L)));
    when(actions.tryAcquireInFlight(peerTable, peerPolicy, CLAIM_TIMEOUT_MS))
        .thenReturn(Optional.of(new OccupancyClaim(2L, 20L)));

    Map<String, String> p1 = Map.of("catalog_name", "iceberg", "table_identifier", "db.t");
    Map<String, String> p2 = Map.of("catalog_name", "iceberg", "table_identifier", "db.t2");
    MaintenanceJobSpec coalesced =
        new MaintenanceJobSpec(
            List.of(
                new Task(TABLE_ID, POLICY_ID, "expire_snapshots", null, new LinkedHashMap<>(p1)),
                new Task(peerTable, peerPolicy, "expire_snapshots", null, new LinkedHashMap<>(p2))),
            UPDATER,
            null);

    pipeline(false, actions, mgr).submit(INSTANCE, coalesced);

    verify(mgr, times(2)).submitAndBind(eq("metalake"), anyMap(), anyList());
  }

  @Test
  void testBatchSubmitAllowsMixedPolicyIds() {
    long policyB = POLICY_ID + 1;
    MaintenanceJobManager mgr = mock(MaintenanceJobManager.class);
    when(mgr.submitAndBind(anyString(), anyMap(), anyList()))
        .thenReturn(Optional.of(555L), Optional.of(556L));
    TableMaintenanceActionMetaService actions = mock(TableMaintenanceActionMetaService.class);
    OccupancyClaim claimA = new OccupancyClaim(1L, 10L);
    OccupancyClaim claimB = new OccupancyClaim(2L, 20L);
    when(actions.tryAcquireInFlight(10L, POLICY_ID, CLAIM_TIMEOUT_MS))
        .thenReturn(Optional.of(claimA));
    when(actions.tryAcquireInFlight(11L, policyB, CLAIM_TIMEOUT_MS))
        .thenReturn(Optional.of(claimB));

    Map<String, String> p1 = Map.of("catalog_name", "iceberg", "table_identifier", "db.t1");
    Map<String, String> p2 = Map.of("catalog_name", "iceberg", "table_identifier", "db.t2");
    MaintenanceJobSpec mixed =
        new MaintenanceJobSpec(
            List.of(
                new Task(10L, POLICY_ID, "expire_snapshots", null, new LinkedHashMap<>(p1)),
                new Task(11L, policyB, "expire_snapshots", null, new LinkedHashMap<>(p2))),
            UPDATER,
            null);

    pipeline(false, actions, mgr).submit(MaintenanceTaskIds.forCronBatch(0L, POLICY_ID), mixed);

    verify(actions).tryAcquireInFlight(10L, POLICY_ID, CLAIM_TIMEOUT_MS);
    verify(actions).tryAcquireInFlight(11L, policyB, CLAIM_TIMEOUT_MS);
    verify(mgr, times(2)).submitAndBind(eq("metalake"), anyMap(), anyList());
  }

  @Test
  void testBatchInFlightUsesPerTaskPolicyId() {
    long policyB = POLICY_ID + 1;
    MaintenanceJobManager mgr = mock(MaintenanceJobManager.class);
    TableMaintenanceActionMetaService actions = mock(TableMaintenanceActionMetaService.class);

    Map<String, String> p1 = Map.of("catalog_name", "iceberg", "table_identifier", "db.t1");
    Map<String, String> p2 = Map.of("catalog_name", "iceberg", "table_identifier", "db.t2");
    MaintenanceJobSpec mixed =
        new MaintenanceJobSpec(
            List.of(
                new Task(10L, POLICY_ID, "expire_snapshots", null, new LinkedHashMap<>(p1)),
                new Task(11L, policyB, "expire_snapshots", null, new LinkedHashMap<>(p2))),
            UPDATER,
            null);

    // In-flight only for (11, policyB) — must abort whole batch; wrong if gated on POLICY_ID alone.
    pipeline((t, p) -> t == 11L && p == policyB, actions, mgr)
        .submit(MaintenanceTaskIds.forCronBatch(0L, POLICY_ID), mixed);

    verify(actions, never()).tryAcquireInFlight(anyLong(), anyLong(), anyLong());
    verify(mgr, never()).submitAndBind(anyString(), anyMap(), anyList());
  }

  @Test
  void testConsumesSpecJobConfWithoutRebuild() {
    MaintenanceJobManager mgr = mock(MaintenanceJobManager.class);
    when(mgr.submitAndBind(anyString(), anyMap(), anyList())).thenReturn(Optional.of(1L));
    TableMaintenanceActionMetaService actions = mock(TableMaintenanceActionMetaService.class);
    when(actions.tryAcquireInFlight(TABLE_ID, POLICY_ID, CLAIM_TIMEOUT_MS))
        .thenReturn(Optional.of(new OccupancyClaim(1L, 1L)));

    Map<String, String> params = new LinkedHashMap<>();
    params.put("strategy", "binpack");
    pipeline(false, actions, mgr)
        .submit(INSTANCE, spec(params, "{\"spark.sql.catalog.iceberg.rest.auth.type\":\"basic\"}"));

    @SuppressWarnings("unchecked")
    ArgumentCaptor<Map<String, String>> confCaptor = ArgumentCaptor.forClass(Map.class);
    verify(mgr).submitAndBind(eq("metalake"), confCaptor.capture(), anyList());
    Map<String, String> conf = confCaptor.getValue();
    Assertions.assertNull(conf.get("table_identifier"));
    Assertions.assertEquals("binpack", conf.get("strategy"));
    Assertions.assertTrue(conf.get("tables").contains("db.t"));
    Assertions.assertTrue(conf.get("updater_options").contains("gravitino_uri"));
    Assertions.assertTrue(conf.get("spark_conf").contains("basic"));
  }

  private static MaintenanceCronSubmitPipeline pipeline(
      boolean inFlight, TableMaintenanceActionMetaService actions, MaintenanceJobManager mgr) {
    return pipeline((t, p) -> inFlight, actions, mgr);
  }

  private static MaintenanceCronSubmitPipeline pipeline(
      BiPredicate<Long, Long> inFlight,
      TableMaintenanceActionMetaService actions,
      MaintenanceJobManager mgr) {
    return new MaintenanceCronSubmitPipeline(
        occupancyGate(inFlight),
        actions,
        mgr,
        new MaintenanceJobConfBuilder(new Config(false) {}),
        CLAIM_TIMEOUT_MS);
  }

  /** Real gate over a stub meta service — {@link OccupancyGate} is final and not mockable. */
  private static OccupancyGate occupancyGate(BiPredicate<Long, Long> inFlight) {
    TableMaintenanceActionMetaService meta = mock(TableMaintenanceActionMetaService.class);
    when(meta.hasLiveUnboundClaim(anyLong(), anyLong(), anyLong()))
        .thenAnswer(inv -> inFlight.test(inv.getArgument(0), inv.getArgument(1)));
    when(meta.listInFlight(anyLong(), anyLong())).thenReturn(List.of());
    return new OccupancyGate(meta, CLAIM_TIMEOUT_MS);
  }

  private static MaintenanceJobSpec spec() {
    return spec(Map.of(), null);
  }

  private static MaintenanceJobSpec spec(Map<String, String> parameters, String sparkConf) {
    Map<String, String> params = new LinkedHashMap<>(parameters);
    params.putIfAbsent("catalog_name", "iceberg");
    params.putIfAbsent("table_identifier", "db.t");
    return new MaintenanceJobSpec(
        List.of(new Task(TABLE_ID, POLICY_ID, "rewrite_data_files", null, params)),
        UPDATER,
        sparkConf);
  }

  private static MaintenanceJobSpec batchSpec(long table1, long table2) {
    Map<String, String> p1 = Map.of("catalog_name", "iceberg", "table_identifier", "db.t1");
    Map<String, String> p2 = Map.of("catalog_name", "iceberg", "table_identifier", "db.t2");
    return new MaintenanceJobSpec(
        List.of(
            new Task(table1, POLICY_ID, "expire_snapshots", null, new LinkedHashMap<>(p1)),
            new Task(table2, POLICY_ID, "expire_snapshots", null, new LinkedHashMap<>(p2))),
        UPDATER,
        null);
  }
}
