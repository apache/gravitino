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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.apache.gravitino.policy.Policy;
import org.junit.jupiter.api.Test;

public class TestMaintenanceTypeHandlers {

  @Test
  void getReturnsTypedHandlers() {
    assertEquals(
        Policy.BuiltInType.ICEBERG_COMPACTION,
        MaintenanceTypeHandlers.get(Policy.BuiltInType.ICEBERG_COMPACTION).policyType());
    assertTrue(
        MaintenanceTypeHandlers.get(Policy.BuiltInType.ICEBERG_COMPACTION).supportsMetricsSample());
    assertTrue(
        MaintenanceTypeHandlers.get(Policy.BuiltInType.ICEBERG_REWRITE_MANIFESTS)
            .supportsMetricsSample());
    assertFalse(
        MaintenanceTypeHandlers.get(Policy.BuiltInType.ICEBERG_SNAPSHOT_EXPIRATION)
            .supportsMetricsSample());
    assertFalse(
        MaintenanceTypeHandlers.get(Policy.BuiltInType.ICEBERG_ORPHAN_FILE_REMOVAL)
            .supportsMetricsSample());
  }

  @Test
  void getRejectsNonMaintenanceType() {
    assertThrows(
        IllegalArgumentException.class,
        () -> MaintenanceTypeHandlers.get(Policy.BuiltInType.ROW_FILTER));
  }

  @Test
  void commitChainOrder() {
    List<MaintenanceTypeHandler> chain = MaintenanceTypeHandlers.commitChain();
    assertEquals(3, chain.size());
    assertEquals(Policy.BuiltInType.ICEBERG_COMPACTION, chain.get(0).policyType());
    assertEquals(Policy.BuiltInType.ICEBERG_REWRITE_MANIFESTS, chain.get(1).policyType());
    assertEquals(Policy.BuiltInType.ICEBERG_SNAPSHOT_EXPIRATION, chain.get(2).policyType());
  }

  @Test
  void singletonChain() {
    List<MaintenanceTypeHandler> chain =
        MaintenanceTypeHandlers.singleton(Policy.BuiltInType.ICEBERG_ORPHAN_FILE_REMOVAL);
    assertEquals(1, chain.size());
    assertEquals(Policy.BuiltInType.ICEBERG_ORPHAN_FILE_REMOVAL, chain.get(0).policyType());
  }
}
