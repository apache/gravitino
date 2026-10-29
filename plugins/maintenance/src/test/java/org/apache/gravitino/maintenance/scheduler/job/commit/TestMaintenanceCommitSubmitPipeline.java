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

import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.gravitino.Config;
import org.apache.gravitino.maintenance.MaintenanceJobManager;
import org.apache.gravitino.maintenance.occupancy.OccupancyClaim;
import org.apache.gravitino.maintenance.scheduler.MaintenanceJobSpec;
import org.apache.gravitino.maintenance.scheduler.MaintenanceJobSpec.Task;
import org.apache.gravitino.maintenance.scheduler.MaintenanceTaskIds;
import org.apache.gravitino.maintenance.scheduler.job.MaintenanceJobConfBuilder;
import org.apache.gravitino.maintenance.scheduler.job.MaintenanceJobTemplates;
import org.apache.gravitino.maintenance.storage.service.TableMaintenanceActionMetaService;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class TestMaintenanceCommitSubmitPipeline {

  private static final long TABLE_ID = 101L;
  private static final long POLICY_ID = 202L;
  private static final long EXPIRE_POLICY_ID = 303L;
  private static final long CLAIM_TIMEOUT_MS = 60_000L;
  private static final String UPDATER =
      "{\"gravitino_uri\":\"http://localhost:8090\",\"metalake\":\"ml\"}";

  @Test
  void testEarlyExitsSkipSubmit() {
    MaintenanceJobManager mgr = mock(MaintenanceJobManager.class);
    TableMaintenanceActionMetaService actions = mock(TableMaintenanceActionMetaService.class);
    String instance = MaintenanceTaskIds.forCommitTable(TABLE_ID);

    pipeline(Set.of(), actions, mgr).submit("not-a-table-id", spec());
    pipeline(Set.of(), actions, mgr).submit(instance, null);
    pipeline(Set.of(POLICY_ID), actions, mgr).submit(instance, spec());

    verify(actions, never()).tryAcquireInFlightBatch(anyLong(), anyList(), anyLong());
    verify(mgr, never()).submitAndBind(anyString(), anyMap(), anyList());
  }

  @Test
  void testSubmitClaimsEveryChainPolicyAndUsesSpecJobConf() {
    MaintenanceJobManager mgr = mock(MaintenanceJobManager.class);
    List<OccupancyClaim> claims = List.of(new OccupancyClaim(1L, 2L), new OccupancyClaim(3L, 4L));
    when(mgr.submitAndBind(eq("ml"), anyMap(), eq(claims))).thenReturn(Optional.of(999L));
    TableMaintenanceActionMetaService actions = mock(TableMaintenanceActionMetaService.class);
    when(actions.tryAcquireInFlightBatch(
            eq(TABLE_ID), eq(List.of(POLICY_ID, EXPIRE_POLICY_ID)), eq(CLAIM_TIMEOUT_MS)))
        .thenReturn(Optional.of(claims));

    Map<String, String> params =
        new LinkedHashMap<>(Map.of("catalog_name", "iceberg", "table_identifier", "db.t"));
    MaintenanceJobSpec spec =
        new MaintenanceJobSpec(
            List.of(
                new Task(TABLE_ID, POLICY_ID, "rewrite_data_files", null, params),
                new Task(
                    TABLE_ID,
                    EXPIRE_POLICY_ID,
                    "expire_snapshots",
                    null,
                    new LinkedHashMap<>(params))),
            UPDATER,
            null);

    pipeline(Set.of(), actions, mgr).submit(MaintenanceTaskIds.forCommitTable(TABLE_ID), spec);

    @SuppressWarnings("unchecked")
    ArgumentCaptor<Map<String, String>> confCaptor = ArgumentCaptor.forClass(Map.class);
    verify(mgr).submitAndBind(eq("ml"), confCaptor.capture(), eq(claims));
    Map<String, String> conf = confCaptor.getValue();
    Assertions.assertEquals(
        "rewrite_data_files,expire_snapshots",
        conf.get(MaintenanceJobTemplates.MAINTENANCE_TYPES_KEY));
    Assertions.assertNull(conf.get("table_identifier"));
    Assertions.assertTrue(conf.get("tables").contains("\"table_id\":" + TABLE_ID));
  }

  private static MaintenanceCommitSubmitPipeline pipeline(
      Set<Long> inFlightPolicies,
      TableMaintenanceActionMetaService actions,
      MaintenanceJobManager mgr) {
    CommitOccupancyGate gate =
        new CommitOccupancyGate() {
          @Override
          public boolean isInFlight(long tableId, long policyId) {
            return inFlightPolicies.contains(policyId);
          }

          @Override
          public boolean withinMinInterval(
              long tableId, long policyId, long minIntervalMs, long nowMs) {
            return false;
          }
        };
    return new MaintenanceCommitSubmitPipeline(
        gate, actions, mgr, new MaintenanceJobConfBuilder(new Config(false) {}), CLAIM_TIMEOUT_MS);
  }

  private static MaintenanceJobSpec spec() {
    Map<String, String> parameters =
        new LinkedHashMap<>(Map.of("catalog_name", "iceberg", "table_identifier", "db.t"));
    return new MaintenanceJobSpec(
        List.of(new Task(TABLE_ID, POLICY_ID, "rewrite_data_files", null, parameters)),
        UPDATER,
        null);
  }
}
