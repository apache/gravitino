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

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import org.apache.gravitino.policy.Policy;
import org.junit.jupiter.api.Test;

public class TestMaintenanceTypeOrchestrator {

  @Test
  void runsSampleGateExecuteSampleAfterInOrder() {
    MaintenanceTypeHandler handler = mock(MaintenanceTypeHandler.class);
    when(handler.policyType()).thenReturn(Policy.BuiltInType.ICEBERG_COMPACTION);
    when(handler.supportsMetricsSample()).thenReturn(true);
    when(handler.shouldExecute(any())).thenReturn(MaintenanceGateDecision.EXECUTE);

    MaintenanceTypeOrchestrator orchestrator =
        new MaintenanceTypeOrchestrator(Collections.singletonList(handler));
    orchestrator.run(10L, new long[] {20L});

    verify(handler).sampleBefore(any());
    verify(handler).shouldExecute(any());
    verify(handler).execute(any());
    verify(handler).sampleAfter(any());
  }

  @Test
  void skipTypeSkipsExecuteButContinuesChain() {
    MaintenanceTypeHandler first = mock(MaintenanceTypeHandler.class);
    when(first.policyType()).thenReturn(Policy.BuiltInType.ICEBERG_COMPACTION);
    when(first.supportsMetricsSample()).thenReturn(true);
    when(first.shouldExecute(any())).thenReturn(MaintenanceGateDecision.SKIP_TYPE);

    MaintenanceTypeHandler second = mock(MaintenanceTypeHandler.class);
    when(second.policyType()).thenReturn(Policy.BuiltInType.ICEBERG_SNAPSHOT_EXPIRATION);
    when(second.supportsMetricsSample()).thenReturn(false);
    when(second.shouldExecute(any())).thenReturn(MaintenanceGateDecision.EXECUTE);

    MaintenanceTypeOrchestrator orchestrator =
        new MaintenanceTypeOrchestrator(Arrays.asList(first, second));
    orchestrator.run(1L, new long[] {2L, 3L});

    verify(first).sampleBefore(any());
    verify(first, never()).execute(any());
    verify(first, never()).sampleAfter(any());

    verify(second).sampleBefore(any());
    verify(second).execute(any());
    verify(second).sampleAfter(any());
  }

  @Test
  void snapshotDriftSkipsExecuteForMetricsType() {
    MaintenanceTypeHandler handler = mock(MaintenanceTypeHandler.class);
    when(handler.policyType()).thenReturn(Policy.BuiltInType.ICEBERG_COMPACTION);
    when(handler.supportsMetricsSample()).thenReturn(true);
    doAnswer(
            invocation -> {
              MaintenanceTypeContext ctx = invocation.getArgument(0);
              ctx.setSnapshotDrifted(true);
              return null;
            })
        .when(handler)
        .sampleBefore(any());

    MaintenanceTypeOrchestrator orchestrator =
        new MaintenanceTypeOrchestrator(Collections.singletonList(handler));
    orchestrator.run(10L, new long[] {20L});

    verify(handler).sampleBefore(any());
    verify(handler, never()).shouldExecute(any());
    verify(handler, never()).execute(any());
    verify(handler, never()).sampleAfter(any());
  }

  @Test
  void driftIgnoredWhenTypeDoesNotSample() {
    MaintenanceTypeHandler handler = mock(MaintenanceTypeHandler.class);
    when(handler.policyType()).thenReturn(Policy.BuiltInType.ICEBERG_SNAPSHOT_EXPIRATION);
    when(handler.supportsMetricsSample()).thenReturn(false);
    when(handler.shouldExecute(any())).thenReturn(MaintenanceGateDecision.EXECUTE);
    doAnswer(
            invocation -> {
              MaintenanceTypeContext ctx = invocation.getArgument(0);
              ctx.setSnapshotDrifted(true);
              return null;
            })
        .when(handler)
        .sampleBefore(any());

    MaintenanceTypeOrchestrator orchestrator =
        new MaintenanceTypeOrchestrator(Collections.singletonList(handler));
    orchestrator.run(10L, new long[] {20L});

    verify(handler).shouldExecute(any());
    verify(handler).execute(any());
  }

  @Test
  void commitChainStubsRunWithoutError() {
    MaintenanceTypeOrchestrator orchestrator =
        new MaintenanceTypeOrchestrator(MaintenanceTypeHandlers.commitChain());
    orchestrator.run(100L, new long[] {1L, 2L, 3L});
  }

  @Test
  void rejectsMismatchedPolicyIds() {
    MaintenanceTypeOrchestrator orchestrator =
        new MaintenanceTypeOrchestrator(MaintenanceTypeHandlers.commitChain());
    assertThrows(IllegalArgumentException.class, () -> orchestrator.run(1L, new long[] {1L}));
  }

  @Test
  void rejectsEmptyHandlers() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new MaintenanceTypeOrchestrator(Collections.emptyList()));
  }

  @Test
  void runsAllHandlersInChain() {
    MaintenanceTypeHandler a = mockHandler(Policy.BuiltInType.ICEBERG_COMPACTION, true);
    MaintenanceTypeHandler b = mockHandler(Policy.BuiltInType.ICEBERG_REWRITE_MANIFESTS, true);
    MaintenanceTypeHandler c = mockHandler(Policy.BuiltInType.ICEBERG_SNAPSHOT_EXPIRATION, false);

    MaintenanceTypeOrchestrator orchestrator =
        new MaintenanceTypeOrchestrator(Arrays.asList(a, b, c));
    orchestrator.run(9L, new long[] {1L, 2L, 3L});

    verify(a, times(1)).execute(any());
    verify(b, times(1)).execute(any());
    verify(c, times(1)).execute(any());
  }

  private static MaintenanceTypeHandler mockHandler(
      Policy.BuiltInType type, boolean supportsSample) {
    MaintenanceTypeHandler handler = mock(MaintenanceTypeHandler.class);
    when(handler.policyType()).thenReturn(type);
    when(handler.supportsMetricsSample()).thenReturn(supportsSample);
    when(handler.shouldExecute(any())).thenReturn(MaintenanceGateDecision.EXECUTE);
    return handler;
  }
}
