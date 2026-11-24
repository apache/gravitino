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
package org.apache.gravitino.maintenance.jobs.iceberg;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.Map;
import org.apache.gravitino.policy.IcebergDataCompactionContent;
import org.apache.gravitino.policy.IcebergRewriteManifestsContent;
import org.apache.gravitino.policy.Policy;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link MaintenanceTriggerGate}. */
public class TestMaintenanceTriggerGate {

  @Test
  public void testExpireAndOrphanAlwaysMaintain() {
    assertTrue(
        MaintenanceTriggerGate.shouldMaintain(
            Policy.BuiltInType.ICEBERG_SNAPSHOT_EXPIRATION, new HashMap<>(), "{}"));
    Map<String, Object> skip = new HashMap<>();
    skip.put("trigger-expr", "false");
    assertTrue(
        MaintenanceTriggerGate.shouldMaintain(
            Policy.BuiltInType.ICEBERG_ORPHAN_FILE_REMOVAL, skip, null));
  }

  @Test
  public void testCompactionUsesEnqueueRules() {
    Map<String, Object> rules = new HashMap<>();
    rules.put("trigger-expr", "custom-data-file-mse >= minDataFileMse");
    rules.put("minDataFileMse", 10);
    String mse = IcebergDataCompactionContent.DATA_FILE_MSE_METRIC;
    String dels = IcebergDataCompactionContent.DELETE_FILE_NUMBER_METRIC;
    assertFalse(
        MaintenanceTriggerGate.shouldMaintain(
            Policy.BuiltInType.ICEBERG_COMPACTION,
            rules,
            "{\"" + mse + "\":5,\"" + dels + "\":0}"));
    assertTrue(
        MaintenanceTriggerGate.shouldMaintain(
            Policy.BuiltInType.ICEBERG_COMPACTION,
            rules,
            "{\"" + mse + "\":11,\"" + dels + "\":0}"));
  }

  @Test
  public void testManifestRewriteThresholdAndRules() {
    String metric = IcebergRewriteManifestsContent.MANIFEST_NUMBER_METRIC;
    assertFalse(
        MaintenanceTriggerGate.shouldMaintain(
            Policy.BuiltInType.ICEBERG_REWRITE_MANIFESTS,
            new HashMap<>(),
            "{\"" + metric + "\":99}"));
    assertTrue(
        MaintenanceTriggerGate.shouldMaintain(
            Policy.BuiltInType.ICEBERG_REWRITE_MANIFESTS,
            new HashMap<>(),
            "{\"" + metric + "\":100}"));

    Map<String, Object> rules = new HashMap<>();
    rules.put("trigger-expr", "custom-manifest-number >= manifest_count_threshold");
    rules.put("manifest_count_threshold", 10);
    assertFalse(
        MaintenanceTriggerGate.shouldMaintain(
            Policy.BuiltInType.ICEBERG_REWRITE_MANIFESTS, rules, "{\"" + metric + "\":5}"));
    assertTrue(
        MaintenanceTriggerGate.shouldMaintain(
            Policy.BuiltInType.ICEBERG_REWRITE_MANIFESTS, rules, "{\"" + metric + "\":10}"));
  }

  @Test
  public void testMissingMetricsSkipAndUnsupportedThrows() {
    assertFalse(
        MaintenanceTriggerGate.shouldMaintain(
            Policy.BuiltInType.ICEBERG_COMPACTION, new HashMap<>(), "{}"));
    assertFalse(
        MaintenanceTriggerGate.shouldMaintain(
            Policy.BuiltInType.ICEBERG_REWRITE_MANIFESTS, new HashMap<>(), "{}"));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            MaintenanceTriggerGate.shouldMaintain(
                Policy.BuiltInType.CUSTOM, new HashMap<>(), "{}"));
  }

  @Test
  public void testMalformedTriggerExprSkipsNotThrows() {
    Map<String, Object> rules = new HashMap<>();
    rules.put("trigger-expr", "(((( broken");
    String metrics =
        "{\""
            + IcebergDataCompactionContent.DATA_FILE_MSE_METRIC
            + "\":15,\""
            + IcebergDataCompactionContent.DELETE_FILE_NUMBER_METRIC
            + "\":2}";
    assertFalse(
        MaintenanceTriggerGate.shouldMaintain(
            Policy.BuiltInType.ICEBERG_COMPACTION, rules, metrics));
  }
}
