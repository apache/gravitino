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

import static org.mockito.ArgumentMatchers.any;
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
import org.apache.gravitino.dto.requests.MaintenanceActionFinishRequest;
import org.apache.gravitino.dto.requests.MaintenanceActionSampleRequest;
import org.apache.gravitino.dto.responses.ErrorResponse;
import org.apache.gravitino.dto.responses.MaintenanceActionSampleResponse;
import org.apache.gravitino.maintenance.injob.InJobGateOutcome;
import org.apache.gravitino.maintenance.injob.TableMaintenanceInJobPipeline;
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

  private TableMaintenanceInJobPipeline pipeline;
  private TableMaintenanceActionMetaService actions;
  private TableSnapshotMetricsMetaService metrics;
  private LongFunction<String> metalakeLookup;
  private TableMaintenanceActionOperations ops;

  @BeforeEach
  void setUp() {
    pipeline = mock(TableMaintenanceInJobPipeline.class);
    actions = mock(TableMaintenanceActionMetaService.class);
    metrics = mock(TableSnapshotMetricsMetaService.class);
    metalakeLookup = tableId -> METALAKE;
    ops = new TableMaintenanceActionOperations(pipeline, actions, metrics, metalakeLookup);
    ops.setHttpRequest(mock(HttpServletRequest.class));
  }

  @Test
  void testSampleContinue() {
    when(actions.getByJobId(JOB_ID)).thenReturn(action(TABLE_ID, POLICY_ID));
    when(pipeline.gateCronJob(
            eq(JOB_ID), eq(TABLE_ID), eq(POLICY_ID), eq(100L), anyString(), any()))
        .thenReturn(InJobGateOutcome.continueMaintenance(100L));

    Response response =
        ops.sample(
            METALAKE,
            JOB_ID,
            new MaintenanceActionSampleRequest(TABLE_ID, POLICY_ID, 100L, 100L, "{\"m\":1}"));

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    MaintenanceActionSampleResponse body = (MaintenanceActionSampleResponse) response.getEntity();
    Assertions.assertTrue(body.isContinueMaintenance());
    Assertions.assertNull(body.getStopReason());
  }

  @Test
  void testSampleMissingActionReturns404() {
    when(actions.getByJobId(JOB_ID)).thenReturn(null);

    Response response =
        ops.sample(
            METALAKE,
            JOB_ID,
            new MaintenanceActionSampleRequest(TABLE_ID, POLICY_ID, 100L, 100L, "{\"m\":1}"));

    Assertions.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), response.getStatus());
    verify(pipeline, never())
        .gateCronJob(anyLong(), anyLong(), anyLong(), anyLong(), anyString(), any());
  }

  @Test
  void testSampleAlreadyFinishedReturns404() {
    when(actions.getByJobId(JOB_ID)).thenReturn(action(TABLE_ID, POLICY_ID, /*finishedAt=*/ 9L));

    Response response =
        ops.sample(
            METALAKE,
            JOB_ID,
            new MaintenanceActionSampleRequest(TABLE_ID, POLICY_ID, 100L, 100L, "{\"m\":1}"));

    Assertions.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), response.getStatus());
    verify(pipeline, never())
        .gateCronJob(anyLong(), anyLong(), anyLong(), anyLong(), anyString(), any());
  }

  @Test
  void testSampleWrongMetalakeReturns404() {
    when(actions.getByJobId(JOB_ID)).thenReturn(action(TABLE_ID, POLICY_ID));
    ops = new TableMaintenanceActionOperations(pipeline, actions, metrics, tableId -> "other-ml");
    ops.setHttpRequest(mock(HttpServletRequest.class));

    Response response =
        ops.sample(
            METALAKE,
            JOB_ID,
            new MaintenanceActionSampleRequest(TABLE_ID, POLICY_ID, 100L, 100L, "{\"m\":1}"));

    Assertions.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), response.getStatus());
    verify(pipeline, never())
        .gateCronJob(anyLong(), anyLong(), anyLong(), anyLong(), anyString(), any());
  }

  @Test
  void testSampleMismatchedIdsReturns400() {
    when(actions.getByJobId(JOB_ID)).thenReturn(action(TABLE_ID, POLICY_ID));

    Response response =
        ops.sample(
            METALAKE,
            JOB_ID,
            new MaintenanceActionSampleRequest(999L, POLICY_ID, 100L, 100L, "{\"m\":1}"));

    Assertions.assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());
    ErrorResponse error = (ErrorResponse) response.getEntity();
    Assertions.assertTrue(error.getMessage().contains("do not match"));
  }

  @Test
  void testSampleNullBodyReturns400() {
    Response response = ops.sample(METALAKE, JOB_ID, null);
    Assertions.assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());
  }

  @Test
  void testFinishPersistsMetricsAndMarksFinished() {
    when(actions.getByJobId(JOB_ID)).thenReturn(action(TABLE_ID, POLICY_ID));

    Response response =
        ops.finish(METALAKE, JOB_ID, new MaintenanceActionFinishRequest(200L, "{\"s1\":1}"));

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    verify(metrics).insertMetrics(TABLE_ID, POLICY_ID, 200L, "{\"s1\":1}");
    verify(actions).markFinished(eq(JOB_ID), eq(200L), anyLong());
  }

  @Test
  void testFinishMissingActionReturns404() {
    when(actions.getByJobId(JOB_ID)).thenReturn(null);

    Response response =
        ops.finish(METALAKE, JOB_ID, new MaintenanceActionFinishRequest(200L, null));

    Assertions.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), response.getStatus());
    verify(actions, never()).markFinished(anyLong(), any(), anyLong());
  }

  @Test
  void testFinishAlreadyFinishedReturns404() {
    when(actions.getByJobId(JOB_ID)).thenReturn(action(TABLE_ID, POLICY_ID, /*finishedAt=*/ 9L));

    Response response =
        ops.finish(METALAKE, JOB_ID, new MaintenanceActionFinishRequest(200L, "{\"s1\":1}"));

    Assertions.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), response.getStatus());
    verify(metrics, never()).insertMetrics(anyLong(), anyLong(), anyLong(), anyString());
    verify(actions, never()).markFinished(anyLong(), any(), anyLong());
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
