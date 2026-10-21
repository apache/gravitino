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

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.gravitino.maintenance.scheduler.MaintenanceJobSnapshot;
import org.apache.gravitino.maintenance.scheduler.MaintenanceJobSnapshot.ChainStep;
import org.apache.gravitino.maintenance.scheduler.MaintenanceJobSnapshot.TableTarget;
import org.apache.gravitino.maintenance.scheduler.cron.MaintenanceJobSubmitter;
import org.apache.gravitino.maintenance.scheduler.cron.MaintenanceJobTemplates;
import org.apache.gravitino.maintenance.storage.service.OccupancyClaim;
import org.apache.gravitino.maintenance.storage.service.TableMaintenanceActionMetaService;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class TestMaintenanceCommitSubmitPipeline {

  private static final long TABLE_ID = 101L;
  private static final long POLICY_ID = 202L;
  private static final long EXPIRE_POLICY_ID = 303L;
  private static final long CLAIM_TIMEOUT_MS = 60_000L;

  @Test
  void testUnparseableInstanceSkipsSubmit() {
    MaintenanceJobSubmitter submitter = mock(MaintenanceJobSubmitter.class);
    TableMaintenanceActionMetaService actions = mock(TableMaintenanceActionMetaService.class);
    pipeline(allowAllGate(), actions, submitter).submit("not-a-table-id", snapshot());
    verify(submitter, never()).runJob(anyString(), anyString(), anyMap());
  }

  @Test
  void testNullSnapshotSkipsSubmit() {
    MaintenanceJobSubmitter submitter = mock(MaintenanceJobSubmitter.class);
    TableMaintenanceActionMetaService actions = mock(TableMaintenanceActionMetaService.class);
    pipeline(allowAllGate(), actions, submitter).submit(Long.toString(TABLE_ID), null);
    verify(submitter, never()).runJob(anyString(), anyString(), anyMap());
    verify(actions, never()).tryAcquireInFlight(anyLong(), anyLong(), anyLong());
  }

  @Test
  void testInFlightRemainingMemberSkipsSubmit() {
    MaintenanceJobSubmitter submitter = mock(MaintenanceJobSubmitter.class);
    TableMaintenanceActionMetaService actions = mock(TableMaintenanceActionMetaService.class);
    pipeline(inFlightGate(POLICY_ID), actions, submitter)
        .submit(Long.toString(TABLE_ID), snapshot());
    verify(actions, never()).tryAcquireInFlight(anyLong(), anyLong(), anyLong());
    verify(submitter, never()).runJob(anyString(), anyString(), anyMap());
  }

  @Test
  void testSubmitClaimsFirstPolicyAndUsesSnapshotJobConf() {
    MaintenanceJobSubmitter submitter = mock(MaintenanceJobSubmitter.class);
    when(submitter.runJob(anyString(), anyString(), anyMap())).thenReturn(999L);
    TableMaintenanceActionMetaService actions = mock(TableMaintenanceActionMetaService.class);
    OccupancyClaim claim = new OccupancyClaim(1L, 2L);
    when(actions.tryAcquireInFlight(TABLE_ID, POLICY_ID, CLAIM_TIMEOUT_MS))
        .thenReturn(Optional.of(claim));

    Map<String, String> jobConf = new LinkedHashMap<>();
    jobConf.put(
        MaintenanceJobTemplates.MAINTENANCE_TYPES_KEY, "rewrite_data_files,expire_snapshots");
    jobConf.put(MaintenanceJobTemplates.MAINTENANCE_TYPE_KEY, "rewrite_data_files");
    jobConf.put("catalog_name", "iceberg");
    jobConf.put("table_identifier", "db.t");
    jobConf.put("tms_table_id", Long.toString(TABLE_ID));
    jobConf.put("tms_policy_id", Long.toString(POLICY_ID));
    jobConf.put("updater_options", "{\"gravitino_uri\":\"http://localhost:8090\"}");

    MaintenanceJobSnapshot snap =
        new MaintenanceJobSnapshot(
            "ml",
            List.of(new TableTarget(TABLE_ID, List.of("ml", "iceberg", "db", "t"))),
            List.of(
                new ChainStep(POLICY_ID, "rewrite_data_files"),
                new ChainStep(EXPIRE_POLICY_ID, "expire_snapshots")),
            jobConf);

    pipeline(allowAllGate(), actions, submitter).submit(Long.toString(TABLE_ID), snap);

    @SuppressWarnings("unchecked")
    ArgumentCaptor<Map<String, String>> confCaptor = ArgumentCaptor.forClass(Map.class);
    verify(submitter)
        .runJob(
            eq("ml"),
            eq(MaintenanceJobTemplates.TMS_ICEBERG_MAINTENANCE_TEMPLATE),
            confCaptor.capture());
    verify(actions).bindJobId(claim, 999L);

    Map<String, String> conf = confCaptor.getValue();
    Assertions.assertEquals(
        "rewrite_data_files,expire_snapshots",
        conf.get(MaintenanceJobTemplates.MAINTENANCE_TYPES_KEY));
    Assertions.assertEquals(
        "rewrite_data_files", conf.get(MaintenanceJobTemplates.MAINTENANCE_TYPE_KEY));
    Assertions.assertEquals(Long.toString(TABLE_ID), conf.get("tms_table_id"));
    Assertions.assertEquals(Long.toString(POLICY_ID), conf.get("tms_policy_id"));
    Assertions.assertEquals("iceberg", conf.get("catalog_name"));
    Assertions.assertEquals("db.t", conf.get("table_identifier"));
  }

  @Test
  void testMergeResourceKeysTakesMax() {
    Map<String, String> into =
        MaintenanceCommitSubmitPipeline.newLinkedMap(
            "spark_executor_memory", "4g", "spark_executor_cores", "2", "catalog_uri", "http://a");
    Map<String, String> from =
        MaintenanceCommitSubmitPipeline.newLinkedMap(
            "spark_executor_memory", "7g", "spark_executor_cores", "1", "catalog_uri", "http://b");
    MaintenanceCommitSubmitPipeline.mergeResourceKeys(into, from);
    Assertions.assertEquals("7g", into.get("spark_executor_memory"));
    Assertions.assertEquals("2", into.get("spark_executor_cores"));
    Assertions.assertEquals("http://a", into.get("catalog_uri"));
  }

  private static MaintenanceCommitSubmitPipeline pipeline(
      CommitOccupancyGate gate,
      TableMaintenanceActionMetaService actions,
      MaintenanceJobSubmitter submitter) {
    return new MaintenanceCommitSubmitPipeline(gate, actions, submitter, CLAIM_TIMEOUT_MS);
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

  private static CommitOccupancyGate inFlightGate(long... policyIds) {
    Set<Long> blocked = new HashSet<>();
    for (long id : policyIds) {
      blocked.add(id);
    }
    return new CommitOccupancyGate() {
      @Override
      public boolean isInFlight(long tableId, long policyId) {
        return blocked.contains(policyId);
      }

      @Override
      public boolean withinMinInterval(
          long tableId, long policyId, long minIntervalMs, long nowMs) {
        return false;
      }
    };
  }

  private static MaintenanceJobSnapshot snapshot() {
    Map<String, String> jobConf = new LinkedHashMap<>();
    jobConf.put(MaintenanceJobTemplates.MAINTENANCE_TYPES_KEY, "rewrite_data_files");
    jobConf.put(MaintenanceJobTemplates.MAINTENANCE_TYPE_KEY, "rewrite_data_files");
    jobConf.put("catalog_name", "iceberg");
    jobConf.put("table_identifier", "db.t");
    jobConf.put("tms_table_id", Long.toString(TABLE_ID));
    jobConf.put("tms_policy_id", Long.toString(POLICY_ID));
    jobConf.put("updater_options", "{\"gravitino_uri\":\"http://localhost:8090\"}");
    return new MaintenanceJobSnapshot(
        "ml",
        List.of(new TableTarget(TABLE_ID, List.of("ml", "iceberg", "db", "t"))),
        List.of(new ChainStep(POLICY_ID, "rewrite_data_files")),
        jobConf);
  }
}
