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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.gravitino.policy.Policy;
import org.junit.jupiter.api.Test;

public class TestAbstractMaintenanceTypeHandler {

  @Test
  void metricsTypeInvokesDoSampleHooks() {
    CountingHandler handler = new CountingHandler(Policy.BuiltInType.ICEBERG_COMPACTION);
    assertTrue(handler.supportsMetricsSample());

    MaintenanceTypeContext ctx =
        new MaintenanceTypeContext(Policy.BuiltInType.ICEBERG_COMPACTION, 1L, 2L);
    handler.sampleBefore(ctx);
    handler.sampleAfter(ctx);

    assertEquals(1, handler.beforeCount);
    assertEquals(1, handler.afterCount);
  }

  @Test
  void nonMetricsTypeSkipsDoSampleHooks() {
    CountingHandler handler = new CountingHandler(Policy.BuiltInType.ICEBERG_SNAPSHOT_EXPIRATION);
    assertFalse(handler.supportsMetricsSample());

    MaintenanceTypeContext ctx =
        new MaintenanceTypeContext(Policy.BuiltInType.ICEBERG_SNAPSHOT_EXPIRATION, 1L, 2L);
    handler.sampleBefore(ctx);
    handler.sampleAfter(ctx);

    assertEquals(0, handler.beforeCount);
    assertEquals(0, handler.afterCount);
  }

  private static final class CountingHandler extends AbstractMaintenanceTypeHandler {
    private int beforeCount;
    private int afterCount;

    private CountingHandler(Policy.BuiltInType policyType) {
      super(policyType);
    }

    @Override
    protected void doSampleBefore(MaintenanceTypeContext context) {
      beforeCount++;
    }

    @Override
    protected void doSampleAfter(MaintenanceTypeContext context) {
      afterCount++;
    }

    @Override
    public MaintenanceGateDecision shouldExecute(MaintenanceTypeContext context) {
      return MaintenanceGateDecision.EXECUTE;
    }

    @Override
    public void execute(MaintenanceTypeContext context) {}
  }
}
