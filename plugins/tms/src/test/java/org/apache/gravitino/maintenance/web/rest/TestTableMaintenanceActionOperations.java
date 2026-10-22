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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.function.LongFunction;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Response;
import org.apache.gravitino.maintenance.MaintenanceMetricsService;
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
  void testReportMetricsPersists() {
    when(actions.getByJobId(JOB_ID)).thenReturn(action(TABLE_ID, POLICY_ID));

    Response response =
        ops.reportMetrics(METALAKE, JOB_ID, new MaintenanceActionMetricsRequest(100L, "{\"m\":1}"));

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    verify(metrics).insertMetrics(TABLE_ID, POLICY_ID, 100L, "{\"m\":1}");
  }

  @Test
  void testReportMetricsMissingActionReturns404() {
    when(actions.getByJobId(JOB_ID)).thenReturn(null);

    Response response =
        ops.reportMetrics(METALAKE, JOB_ID, new MaintenanceActionMetricsRequest(100L, "{\"m\":1}"));

    Assertions.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), response.getStatus());
    verify(metrics, never()).insertMetrics(anyLong(), anyLong(), anyLong(), anyString());
  }

  @Test
  void testReportMetricsAlreadyFinishedReturns404() {
    when(actions.getByJobId(JOB_ID)).thenReturn(action(TABLE_ID, POLICY_ID, /*finishedAt=*/ 9L));

    Response response =
        ops.reportMetrics(METALAKE, JOB_ID, new MaintenanceActionMetricsRequest(100L, "{\"m\":1}"));

    Assertions.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), response.getStatus());
    verify(metrics, never()).insertMetrics(anyLong(), anyLong(), anyLong(), anyString());
  }

  @Test
  void testReportMetricsWrongMetalakeReturns404() {
    when(actions.getByJobId(JOB_ID)).thenReturn(action(TABLE_ID, POLICY_ID));
    ops = operations(tableId -> "other-ml");
    ops.setHttpRequest(mock(HttpServletRequest.class));

    Response response =
        ops.reportMetrics(METALAKE, JOB_ID, new MaintenanceActionMetricsRequest(100L, "{\"m\":1}"));

    Assertions.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), response.getStatus());
    verify(metrics, never()).insertMetrics(anyLong(), anyLong(), anyLong(), anyString());
  }

  @Test
  void testReportMetricsNullBodyReturns400() {
    Response response = ops.reportMetrics(METALAKE, JOB_ID, null);
    Assertions.assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());
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
