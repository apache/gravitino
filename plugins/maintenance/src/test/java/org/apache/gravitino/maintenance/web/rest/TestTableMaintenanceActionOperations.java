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
package org.apache.gravitino.maintenance.web.rest;

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.function.LongFunction;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Response;
import org.apache.gravitino.maintenance.MaintenanceMetricsService;
import org.apache.gravitino.maintenance.dto.requests.MaintenanceActionFinishRequest;
import org.apache.gravitino.maintenance.dto.requests.MaintenanceActionMetricsRequest;
import org.apache.gravitino.maintenance.storage.po.TableMaintenanceActionPO;
import org.apache.gravitino.maintenance.storage.service.TableMaintenanceActionMetaService;
import org.apache.gravitino.maintenance.storage.service.TableSnapshotMetricsMetaService;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link TableMaintenanceActionOperations}. */
class TestTableMaintenanceActionOperations {

  private static final String METALAKE = "ml";
  private static final long JOB_ID = 42L;
  private static final long TABLE_ID = 10L;
  private static final long POLICY_ID = 20L;

  private TableMaintenanceActionMetaService actions;
  private TableSnapshotMetricsMetaService metrics;
  private TableMaintenanceActionOperations ops;

  @BeforeEach
  void setUp() {
    actions = mock(TableMaintenanceActionMetaService.class);
    metrics = mock(TableSnapshotMetricsMetaService.class);
    ops = operations(tableId -> METALAKE);
    ops.setHttpRequest(mock(HttpServletRequest.class));
  }

  @Test
  void testReportBeforePersistsAndRecordsBeforeMetrics() {
    when(actions.getByJobIdAndTableIdAndPolicyId(JOB_ID, TABLE_ID, POLICY_ID))
        .thenReturn(action(TABLE_ID, POLICY_ID));
    when(metrics.insertMetrics(TABLE_ID, POLICY_ID, 100L, "{\"m\":1}")).thenReturn(501L);

    Response response =
        ops.reportBeforeMetrics(
            METALAKE,
            JOB_ID,
            /*finish=*/ false,
            new MaintenanceActionMetricsRequest(TABLE_ID, POLICY_ID, 100L, "{\"m\":1}"));

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    verify(metrics).insertMetrics(TABLE_ID, POLICY_ID, 100L, "{\"m\":1}");
    verify(actions).recordBeforeMetrics(JOB_ID, TABLE_ID, POLICY_ID, 501L);
    verify(actions, never()).markFinished(anyLong(), anyLong(), anyLong(), anyLong());
  }

  @Test
  void testReportBeforeWithFinishClosesSoftSlotWithoutAfterMetrics() {
    when(actions.getByJobIdAndTableIdAndPolicyId(JOB_ID, TABLE_ID, POLICY_ID))
        .thenReturn(action(TABLE_ID, POLICY_ID));
    when(metrics.insertMetrics(TABLE_ID, POLICY_ID, 100L, "{\"m\":1}")).thenReturn(501L);

    Response response =
        ops.reportBeforeMetrics(
            METALAKE,
            JOB_ID,
            /*finish=*/ true,
            new MaintenanceActionMetricsRequest(TABLE_ID, POLICY_ID, 100L, "{\"m\":1}"));

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    verify(actions).markFinished(eq(JOB_ID), eq(TABLE_ID), eq(POLICY_ID), anyLong());
  }

  @Test
  void testReportAfterWithFinishClosesSoftSlotWithAfterMetrics() {
    when(actions.getByJobIdAndTableIdAndPolicyId(JOB_ID, TABLE_ID, POLICY_ID))
        .thenReturn(action(TABLE_ID, POLICY_ID));
    when(metrics.insertMetrics(TABLE_ID, POLICY_ID, 200L, "{\"m\":2}")).thenReturn(502L);

    Response response =
        ops.reportAfterMetrics(
            METALAKE,
            JOB_ID,
            /*finish=*/ true,
            new MaintenanceActionMetricsRequest(TABLE_ID, POLICY_ID, 200L, "{\"m\":2}"));

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    verify(metrics).insertMetrics(TABLE_ID, POLICY_ID, 200L, "{\"m\":2}");
    verify(actions).recordAfterMetrics(JOB_ID, TABLE_ID, POLICY_ID, 502L);
    verify(actions).markFinished(eq(JOB_ID), eq(TABLE_ID), eq(POLICY_ID), anyLong());
  }

  @Test
  void testReportBeforeMissingActionReturns404() {
    when(actions.getByJobIdAndTableIdAndPolicyId(JOB_ID, TABLE_ID, POLICY_ID)).thenReturn(null);
    when(actions.bindJobIdIfUnbound(TABLE_ID, POLICY_ID, JOB_ID)).thenReturn(false);

    Response response =
        ops.reportBeforeMetrics(
            METALAKE,
            JOB_ID,
            /*finish=*/ false,
            new MaintenanceActionMetricsRequest(TABLE_ID, POLICY_ID, 100L, "{\"m\":1}"));

    Assertions.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), response.getStatus());
    verify(metrics, never()).insertMetrics(anyLong(), anyLong(), anyLong(), anyString());
  }

  @Test
  void testReportBeforeLazyBindsUnboundClaim() {
    when(actions.getByJobIdAndTableIdAndPolicyId(JOB_ID, TABLE_ID, POLICY_ID))
        .thenReturn(null)
        .thenReturn(action(TABLE_ID, POLICY_ID));
    when(actions.bindJobIdIfUnbound(TABLE_ID, POLICY_ID, JOB_ID)).thenReturn(true);
    when(metrics.insertMetrics(TABLE_ID, POLICY_ID, 100L, "{\"m\":1}")).thenReturn(501L);

    Response response =
        ops.reportBeforeMetrics(
            METALAKE,
            JOB_ID,
            /*finish=*/ false,
            new MaintenanceActionMetricsRequest(TABLE_ID, POLICY_ID, 100L, "{\"m\":1}"));

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    verify(actions).bindJobIdIfUnbound(TABLE_ID, POLICY_ID, JOB_ID);
    verify(metrics).insertMetrics(TABLE_ID, POLICY_ID, 100L, "{\"m\":1}");
    verify(actions).recordBeforeMetrics(JOB_ID, TABLE_ID, POLICY_ID, 501L);
  }

  @Test
  void testFinishLazyBindsUnboundClaim() {
    when(actions.getByJobIdAndTableIdAndPolicyId(JOB_ID, TABLE_ID, POLICY_ID))
        .thenReturn(null)
        .thenReturn(action(TABLE_ID, POLICY_ID));
    when(actions.bindJobIdIfUnbound(TABLE_ID, POLICY_ID, JOB_ID)).thenReturn(true);

    Response response =
        ops.finishAction(METALAKE, JOB_ID, new MaintenanceActionFinishRequest(TABLE_ID, POLICY_ID));

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    verify(actions).bindJobIdIfUnbound(TABLE_ID, POLICY_ID, JOB_ID);
    verify(actions).markFinished(eq(JOB_ID), eq(TABLE_ID), eq(POLICY_ID), anyLong());
  }

  @Test
  void testReportAfterAlreadyFinishedReturns404() {
    when(actions.getByJobIdAndTableIdAndPolicyId(JOB_ID, TABLE_ID, POLICY_ID))
        .thenReturn(action(TABLE_ID, POLICY_ID, /*finishedAt=*/ 9L));

    Response response =
        ops.reportAfterMetrics(
            METALAKE,
            JOB_ID,
            /*finish=*/ false,
            new MaintenanceActionMetricsRequest(TABLE_ID, POLICY_ID, 100L, "{\"m\":1}"));

    Assertions.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), response.getStatus());
    verify(metrics, never()).insertMetrics(anyLong(), anyLong(), anyLong(), anyString());
  }

  @Test
  void testReportBeforeNullBodyReturns400() {
    Response response = ops.reportBeforeMetrics(METALAKE, JOB_ID, /*finish=*/ false, null);
    Assertions.assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());
  }

  @Test
  void testFinishClosesSoftSlotWithoutMetrics() {
    when(actions.getByJobIdAndTableIdAndPolicyId(JOB_ID, TABLE_ID, POLICY_ID))
        .thenReturn(action(TABLE_ID, POLICY_ID));

    Response response =
        ops.finishAction(METALAKE, JOB_ID, new MaintenanceActionFinishRequest(TABLE_ID, POLICY_ID));

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    verify(metrics, never()).insertMetrics(anyLong(), anyLong(), anyLong(), anyString());
    verify(actions).markFinished(eq(JOB_ID), eq(TABLE_ID), eq(POLICY_ID), anyLong());
  }

  private TableMaintenanceActionOperations operations(LongFunction<String> lookup) {
    return new TableMaintenanceActionOperations(
        new MaintenanceMetricsService(actions, metrics, lookup));
  }

  private static TableMaintenanceActionPO action(long tableId, long policyId) {
    return action(tableId, policyId, 0L);
  }

  private static TableMaintenanceActionPO action(long tableId, long policyId, long finishedAt) {
    return TableMaintenanceActionPO.builder()
        .withId(1L)
        .withJobId(JOB_ID)
        .withTableId(tableId)
        .withPolicyId(policyId)
        .withFinishedAt(finishedAt)
        .withCreatedAt(1L)
        .build();
  }
}
