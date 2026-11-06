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

import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.lang.reflect.Field;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Response;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.maintenance.MaintenanceMetricsManager;
import org.apache.gravitino.maintenance.dto.requests.MaintenanceActionFinishRequest;
import org.apache.gravitino.maintenance.dto.requests.MaintenanceActionMetricsRequest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link TableMaintenanceActionOperations}. */
class TestTableMaintenanceActionOperations {

  private static final String METALAKE = "ml";
  private static final long ACTION_ID = 9001L;
  private static final long TABLE_ID = 10L;
  private static final long POLICY_ID = 20L;

  private MaintenanceMetricsManager metricsManager;
  private TableMaintenanceActionOperations ops;

  @BeforeEach
  void setUp() throws Exception {
    metricsManager = mock(MaintenanceMetricsManager.class);
    ops = new TableMaintenanceActionOperations(metricsManager);
    Field httpRequest = TableMaintenanceActionOperations.class.getDeclaredField("httpRequest");
    httpRequest.setAccessible(true);
    httpRequest.set(ops, mock(HttpServletRequest.class));
  }

  @Test
  void testReportBeforeDelegatesToManager() {
    MaintenanceActionMetricsRequest request =
        new MaintenanceActionMetricsRequest(TABLE_ID, POLICY_ID, 100L, "{\"m\":1}");
    Response response = ops.reportBeforeMetrics(METALAKE, ACTION_ID, /*finish=*/ false, request);
    Assertions.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    verify(metricsManager).reportBeforeMetrics(METALAKE, ACTION_ID, false, request);
  }

  @Test
  void testReportAfterDelegatesToManager() {
    MaintenanceActionMetricsRequest request =
        new MaintenanceActionMetricsRequest(TABLE_ID, POLICY_ID, 200L, "{\"m\":2}");
    Response response = ops.reportAfterMetrics(METALAKE, ACTION_ID, /*finish=*/ true, request);
    Assertions.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    verify(metricsManager).reportAfterMetrics(METALAKE, ACTION_ID, true, request);
  }

  @Test
  void testFinishDelegatesToManager() {
    MaintenanceActionFinishRequest request =
        new MaintenanceActionFinishRequest(TABLE_ID, POLICY_ID);
    Response response = ops.finishAction(METALAKE, ACTION_ID, request);
    Assertions.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    verify(metricsManager).finishAction(METALAKE, ACTION_ID, request);
  }

  @Test
  void testManagerNotFoundMappedTo404() {
    doThrow(new NoSuchEntityException("missing"))
        .when(metricsManager)
        .reportBeforeMetrics(anyString(), anyLong(), anyBoolean(), eq(null));
    // null body is rejected before manager; use a real body and force manager 404
    MaintenanceActionMetricsRequest request =
        new MaintenanceActionMetricsRequest(TABLE_ID, POLICY_ID, 100L, "{\"m\":1}");
    doThrow(new NoSuchEntityException("missing"))
        .when(metricsManager)
        .reportBeforeMetrics(METALAKE, ACTION_ID, false, request);

    Response response = ops.reportBeforeMetrics(METALAKE, ACTION_ID, /*finish=*/ false, request);
    Assertions.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @Test
  void testReportBeforeNullBodyReturns400() {
    Response response = ops.reportBeforeMetrics(METALAKE, ACTION_ID, /*finish=*/ false, null);
    Assertions.assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());
  }
}
