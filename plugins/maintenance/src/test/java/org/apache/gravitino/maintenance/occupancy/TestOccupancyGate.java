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
package org.apache.gravitino.maintenance.occupancy;

import static org.apache.gravitino.maintenance.storage.TableMaintenanceActionTestUtils.expireClaim;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.io.IOException;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.maintenance.TableMaintenanceConfig;
import org.apache.gravitino.maintenance.storage.service.OccupancyClaim;
import org.apache.gravitino.maintenance.storage.service.TableMaintenanceActionMetaService;
import org.apache.gravitino.meta.PolicyEntity;
import org.apache.gravitino.meta.TableEntity;
import org.apache.gravitino.policy.PolicyContents;
import org.apache.gravitino.storage.relational.TestJDBCBackend;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.TestTemplate;

class TestOccupancyGate extends TestJDBCBackend {

  @TestTemplate
  void testUnboundClaimAndBoundSlotOccupyUntilMarkFinished() throws IOException {
    String metalakeName = "maint_occ_ml";
    createAndInsertMakeLake(metalakeName);
    createAndInsertCatalog(metalakeName, "cat");
    createAndInsertSchema(metalakeName, "cat", "sch");
    TableEntity table = createAndInsertTableEntity(Namespace.of(metalakeName, "cat", "sch"), "tbl");

    TableMaintenanceActionMetaService actions = TableMaintenanceActionMetaService.getInstance();
    OccupancyGate gate = new OccupancyGate(actions);
    long policyId = insertPolicy(metalakeName, "occ-policy").id();

    Assertions.assertFalse(gate.isInFlight(table.id(), policyId));

    OccupancyClaim claim =
        actions.insertInFlight(
            table.id(), policyId, TableMaintenanceConfig.DEFAULT_CLAIM_TIMEOUT_MS);
    Assertions.assertTrue(gate.isInFlight(table.id(), policyId));

    actions.bindJobId(claim, 7001L);
    Assertions.assertTrue(gate.isInFlight(table.id(), policyId));
    Assertions.assertTrue(
        gate.shouldSkip(table.id(), policyId, 3_600_000L, System.currentTimeMillis()));

    long finishedAt = System.currentTimeMillis();
    actions.markFinished(7001L, table.id(), policyId, 200L, finishedAt);
    Assertions.assertFalse(gate.isInFlight(table.id(), policyId));
    Assertions.assertEquals(
        finishedAt, actions.getByJobIdAndTableId(7001L, table.id()).finishedAt());
  }

  @TestTemplate
  void testMinIntervalUsesActionFinishedAtOnly() throws IOException {
    String metalakeName = "maint_occ_ml2";
    createAndInsertMakeLake(metalakeName);
    createAndInsertCatalog(metalakeName, "cat");
    createAndInsertSchema(metalakeName, "cat", "sch");
    TableEntity table = createAndInsertTableEntity(Namespace.of(metalakeName, "cat", "sch"), "tbl");

    TableMaintenanceActionMetaService actions = TableMaintenanceActionMetaService.getInstance();
    OccupancyGate gate = new OccupancyGate(actions);
    long policyId = insertPolicy(metalakeName, "occ-policy-2").id();

    OccupancyClaim claim =
        actions.insertInFlight(
            table.id(), policyId, TableMaintenanceConfig.DEFAULT_CLAIM_TIMEOUT_MS);
    long jobId = 8001L;
    actions.bindJobId(claim, jobId);
    long finishedAt = 1_700_000_000_000L;
    actions.markFinished(jobId, table.id(), policyId, null, finishedAt);

    Assertions.assertTrue(
        gate.withinMinInterval(table.id(), policyId, 60_000L, finishedAt + 1_000L));
    Assertions.assertFalse(
        gate.withinMinInterval(table.id(), policyId, 60_000L, finishedAt + 120_000L));
  }

  @TestTemplate
  void testExpiredUnboundClaimDoesNotBlock() throws Exception {
    String metalakeName = "maint_occ_ml3";
    createAndInsertMakeLake(metalakeName);
    createAndInsertCatalog(metalakeName, "cat");
    createAndInsertSchema(metalakeName, "cat", "sch");
    TableEntity table = createAndInsertTableEntity(Namespace.of(metalakeName, "cat", "sch"), "tbl");

    TableMaintenanceActionMetaService actions = TableMaintenanceActionMetaService.getInstance();
    long claimTimeoutMs = TableMaintenanceConfig.DEFAULT_CLAIM_TIMEOUT_MS;
    OccupancyGate gate = new OccupancyGate(actions, claimTimeoutMs);
    long policyId = insertPolicy(metalakeName, "occ-policy-3").id();

    OccupancyClaim claim = actions.insertInFlight(table.id(), policyId, claimTimeoutMs);
    Assertions.assertTrue(actions.hasLiveUnboundClaim(table.id(), policyId, claimTimeoutMs));
    Assertions.assertTrue(gate.isInFlight(table.id(), policyId));

    expireClaim(claim, claimTimeoutMs);

    // Unbound TTL is evaluated in the DAO against the database clock, so OccupancyGate agrees with
    // acquire/reclaim even when the JVM clock would disagree (multi-node skew).
    Assertions.assertFalse(actions.hasLiveUnboundClaim(table.id(), policyId, claimTimeoutMs));
    Assertions.assertFalse(gate.isInFlight(table.id(), policyId));
    Assertions.assertTrue(
        actions.tryAcquireInFlight(table.id(), policyId, claimTimeoutMs).isPresent());
    Assertions.assertEquals(claim.id(), actions.getById(claim.id()).id());
  }

  private PolicyEntity insertPolicy(String metalakeName, String policyName) throws IOException {
    return createAndInsertPolicyEntity(
        policyName,
        "comment",
        PolicyContents.custom(
            ImmutableMap.of("k", "v"), ImmutableSet.of(MetadataObject.Type.TABLE), null),
        metalakeName);
  }
}
