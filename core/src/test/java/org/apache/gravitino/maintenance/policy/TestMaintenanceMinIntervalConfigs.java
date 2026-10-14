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
package org.apache.gravitino.maintenance.policy;

import org.apache.gravitino.Config;
import org.apache.gravitino.policy.Policy;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestMaintenanceMinIntervalConfigs {

  @Test
  void testRegisteredDefaults() {
    Config config = new Config(false) {};
    Assertions.assertEquals(
        MaintenanceMinIntervalConfigs.DEFAULT_HOURLY_MIN_INTERVAL_MS,
        config.get(MaintenanceMinIntervalConfigs.COMPACTION_MIN_INTERVAL_MS));
    Assertions.assertEquals(
        MaintenanceMinIntervalConfigs.DEFAULT_HOURLY_MIN_INTERVAL_MS,
        config.get(MaintenanceMinIntervalConfigs.MANIFEST_REWRITE_MIN_INTERVAL_MS));
    Assertions.assertEquals(
        MaintenanceMinIntervalConfigs.DEFAULT_HOURLY_MIN_INTERVAL_MS,
        config.get(MaintenanceMinIntervalConfigs.SNAPSHOT_EXPIRY_MIN_INTERVAL_MS));
    Assertions.assertEquals(
        MaintenanceMinIntervalConfigs.DEFAULT_ORPHAN_CLEANUP_MIN_INTERVAL_MS,
        config.get(MaintenanceMinIntervalConfigs.ORPHAN_CLEANUP_MIN_INTERVAL_MS));
  }

  @Test
  void testConfigKeys() {
    Assertions.assertEquals(
        MaintenanceMinIntervalConfigs.COMPACTION_MIN_INTERVAL_MS_KEY,
        MaintenanceMinIntervalConfigs.forBuiltInType(Policy.BuiltInType.ICEBERG_COMPACTION)
            .getKey());
    Assertions.assertEquals(
        MaintenanceMinIntervalConfigs.MANIFEST_REWRITE_MIN_INTERVAL_MS_KEY,
        MaintenanceMinIntervalConfigs.forBuiltInType(Policy.BuiltInType.ICEBERG_REWRITE_MANIFESTS)
            .getKey());
    Assertions.assertEquals(
        MaintenanceMinIntervalConfigs.SNAPSHOT_EXPIRY_MIN_INTERVAL_MS_KEY,
        MaintenanceMinIntervalConfigs.forBuiltInType(Policy.BuiltInType.ICEBERG_SNAPSHOT_EXPIRATION)
            .getKey());
    Assertions.assertEquals(
        MaintenanceMinIntervalConfigs.ORPHAN_CLEANUP_MIN_INTERVAL_MS_KEY,
        MaintenanceMinIntervalConfigs.forBuiltInType(Policy.BuiltInType.ICEBERG_ORPHAN_FILE_REMOVAL)
            .getKey());
  }

  @Test
  void testResolvePrefersPolicyThenConfigThenDefault() {
    Config config = new Config(false) {};
    config.set(MaintenanceMinIntervalConfigs.COMPACTION_MIN_INTERVAL_MS, 900_000L);

    Assertions.assertEquals(
        120_000L,
        MaintenanceMinIntervalConfigs.resolve(
            config, Policy.BuiltInType.ICEBERG_COMPACTION, 120_000L));
    Assertions.assertEquals(
        900_000L,
        MaintenanceMinIntervalConfigs.resolve(config, Policy.BuiltInType.ICEBERG_COMPACTION, null));

    Config defaultsOnly = new Config(false) {};
    Assertions.assertEquals(
        MaintenanceMinIntervalConfigs.DEFAULT_ORPHAN_CLEANUP_MIN_INTERVAL_MS,
        MaintenanceMinIntervalConfigs.resolve(
            defaultsOnly, Policy.BuiltInType.ICEBERG_ORPHAN_FILE_REMOVAL, null));
  }

  @Test
  void testRejectsNegativeMinInterval() {
    Config config = new Config(false) {};
    config.set(MaintenanceMinIntervalConfigs.COMPACTION_MIN_INTERVAL_MS, -1L);
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> config.get(MaintenanceMinIntervalConfigs.COMPACTION_MIN_INTERVAL_MS));
  }

  @Test
  void testResolveRejectsNegativePolicyMinInterval() {
    Config config = new Config(false) {};
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            MaintenanceMinIntervalConfigs.resolve(
                config, Policy.BuiltInType.ICEBERG_COMPACTION, -1L));
  }

  @Test
  void testRejectsNonMaintenanceBuiltInType() {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> MaintenanceMinIntervalConfigs.forBuiltInType(Policy.BuiltInType.ROW_FILTER));
    Assertions.assertFalse(
        MaintenancePolicyDefinitions.isMaintenance(Policy.BuiltInType.ROW_FILTER));
  }

  @Test
  void testRegistryCoversFourMaintenanceTypes() {
    Assertions.assertEquals(4, MaintenancePolicyDefinitions.all().size());
    Assertions.assertTrue(
        MaintenancePolicyDefinitions.isMaintenance(Policy.BuiltInType.ICEBERG_COMPACTION));
    Assertions.assertTrue(
        MaintenancePolicyDefinitions.isMaintenance(Policy.BuiltInType.ICEBERG_REWRITE_MANIFESTS));
    Assertions.assertTrue(
        MaintenancePolicyDefinitions.isMaintenance(Policy.BuiltInType.ICEBERG_SNAPSHOT_EXPIRATION));
    Assertions.assertTrue(
        MaintenancePolicyDefinitions.isMaintenance(Policy.BuiltInType.ICEBERG_ORPHAN_FILE_REMOVAL));

    MaintenancePolicyDefinition compaction =
        MaintenancePolicyDefinitions.require(Policy.BuiltInType.ICEBERG_COMPACTION);
    Assertions.assertEquals(Policy.BuiltInType.ICEBERG_COMPACTION, compaction.builtInType());
    Assertions.assertEquals(
        MaintenanceMinIntervalConfigs.COMPACTION_MIN_INTERVAL_MS, compaction.minIntervalMs());
    Assertions.assertEquals(
        MaintenanceMinIntervalConfigs.COMPACTION_MIN_INTERVAL_MS_KEY,
        compaction.minIntervalMsKey());
  }
}
