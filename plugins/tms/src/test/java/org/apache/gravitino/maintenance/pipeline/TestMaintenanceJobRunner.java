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
package org.apache.gravitino.maintenance.pipeline;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.gravitino.policy.Policy;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

public class TestMaintenanceJobRunner {

  @Test
  void runsSampleGateExecutePerTableInOrder() {
    MaintenanceTypeHandler handler = mock(MaintenanceTypeHandler.class);
    when(handler.policyType()).thenReturn(Policy.BuiltInType.ICEBERG_SNAPSHOT_EXPIRATION);
    when(handler.supportsMetricsSample()).thenReturn(false);
    when(handler.shouldExecute(any())).thenReturn(MaintenanceGateDecision.EXECUTE);

    List<Long> tableOrder = new ArrayList<>();
    doAnswer(
            invocation -> {
              MaintenanceTypeContext ctx = invocation.getArgument(0);
              tableOrder.add(ctx.tableId());
              return null;
            })
        .when(handler)
        .execute(any());

    MaintenanceJobRunner runner =
        new MaintenanceJobRunner(
            new MaintenanceTypeOrchestrator(Collections.singletonList(handler)));
    runner.runForTables(new long[] {10L, 11L, 12L}, new long[] {99L});

    assertEquals(List.of(10L, 11L, 12L), tableOrder);
    verify(handler, times(3)).shouldExecute(any());
    verify(handler, times(3)).execute(any());
  }

  @Test
  void skipOnFirstTableStillRunsSecondTable() {
    MaintenanceTypeHandler handler = mock(MaintenanceTypeHandler.class);
    when(handler.policyType()).thenReturn(Policy.BuiltInType.ICEBERG_SNAPSHOT_EXPIRATION);
    when(handler.supportsMetricsSample()).thenReturn(false);
    when(handler.shouldExecute(any()))
        .thenAnswer(
            invocation -> {
              MaintenanceTypeContext ctx = invocation.getArgument(0);
              return ctx.tableId() == 1L
                  ? MaintenanceGateDecision.SKIP_TYPE
                  : MaintenanceGateDecision.EXECUTE;
            });

    MaintenanceJobRunner runner =
        new MaintenanceJobRunner(
            new MaintenanceTypeOrchestrator(Collections.singletonList(handler)));
    runner.runForTables(new long[] {1L, 2L}, new long[] {7L});

    verify(handler)
        .execute(argThat((MaintenanceTypeContext ctx) -> ctx != null && ctx.tableId() == 2L));
    verify(handler, times(1)).execute(any());
  }

  @Test
  void failureOnFirstTableStillRunsSecondTable() {
    MaintenanceTypeHandler handler = mock(MaintenanceTypeHandler.class);
    when(handler.policyType()).thenReturn(Policy.BuiltInType.ICEBERG_SNAPSHOT_EXPIRATION);
    when(handler.supportsMetricsSample()).thenReturn(false);
    when(handler.shouldExecute(any())).thenReturn(MaintenanceGateDecision.EXECUTE);
    doThrow(new RuntimeException("boom"))
        .when(handler)
        .execute(argThat((MaintenanceTypeContext ctx) -> ctx != null && ctx.tableId() == 1L));

    MaintenanceJobRunner runner =
        new MaintenanceJobRunner(
            new MaintenanceTypeOrchestrator(Collections.singletonList(handler)));
    runner.runForTables(new long[] {1L, 2L}, new long[] {7L});

    InOrder order = inOrder(handler);
    order.verify(handler).execute(argThat(ctx -> ctx.tableId() == 1L));
    order.verify(handler).execute(argThat(ctx -> ctx.tableId() == 2L));
  }

  @Test
  void singleTableDelegatesToOrchestrator() {
    MaintenanceTypeHandler handler = mock(MaintenanceTypeHandler.class);
    when(handler.policyType()).thenReturn(Policy.BuiltInType.ICEBERG_COMPACTION);
    when(handler.supportsMetricsSample()).thenReturn(true);
    when(handler.shouldExecute(any())).thenReturn(MaintenanceGateDecision.EXECUTE);

    MaintenanceJobRunner runner =
        new MaintenanceJobRunner(
            new MaintenanceTypeOrchestrator(Collections.singletonList(handler)));
    runner.runForTables(new long[] {42L}, new long[] {5L});

    verify(handler).sampleBefore(argThat(ctx -> ctx.tableId() == 42L && ctx.policyId() == 5L));
    verify(handler).execute(any());
    verify(handler).sampleAfter(any());
  }

  @Test
  void rejectsEmptyTableIds() {
    MaintenanceJobRunner runner =
        new MaintenanceJobRunner(
            new MaintenanceTypeOrchestrator(
                MaintenanceTypeHandlers.singleton(Policy.BuiltInType.ICEBERG_SNAPSHOT_EXPIRATION)));
    assertThrows(
        IllegalArgumentException.class, () -> runner.runForTables(new long[] {}, new long[] {1L}));
  }

  @Test
  void batchedSnapshotExpiryStubsRunWithoutError() {
    MaintenanceJobRunner runner =
        new MaintenanceJobRunner(
            new MaintenanceTypeOrchestrator(
                MaintenanceTypeHandlers.singleton(Policy.BuiltInType.ICEBERG_SNAPSHOT_EXPIRATION)));
    runner.runForTables(new long[] {100L, 101L, 102L}, new long[] {9L});
  }
}
