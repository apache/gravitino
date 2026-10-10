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

package org.apache.gravitino.testing;

import static org.apache.gravitino.testing.CoreDatabaseConcurrency.DEFAULT_BUILD_PROCESS_MEMORY_RESERVE_BYTES;
import static org.apache.gravitino.testing.CoreDatabaseConcurrency.DEFAULT_TEST_WORKER_OVERHEAD_BYTES;
import static org.apache.gravitino.testing.CoreDatabaseConcurrency.TEST_WORKER_MAX_HEAP_BYTES;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.gravitino.testing.CoreDatabaseConcurrency.Budget;
import org.apache.gravitino.testing.CoreDatabaseConcurrency.ConfigurationSource;
import org.apache.gravitino.testing.CoreDatabaseConcurrency.DetectedCapacity;
import org.apache.gravitino.testing.CoreDatabaseConcurrency.LimitingFactor;
import org.apache.gravitino.testing.CoreDatabaseConcurrency.MemoryConfiguration;
import org.apache.gravitino.testing.CoreDatabaseConcurrency.Resolution;
import org.apache.gravitino.testing.CoreDatabaseConcurrency.Source;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class TestCoreDatabaseConcurrency {

  private static final long MIB = 1024L * 1024L;
  private static final long GIB = 1024L * MIB;

  @Test
  void usesDefaultMemoryConfiguration() {
    MemoryConfiguration configuration =
        CoreDatabaseConcurrency.resolveMemoryConfiguration(null, null, null, null);

    assertEquals(DEFAULT_BUILD_PROCESS_MEMORY_RESERVE_BYTES, configuration.buildReserveBytes());
    assertEquals(ConfigurationSource.DEFAULT, configuration.buildReserveSource());
    assertEquals(DEFAULT_TEST_WORKER_OVERHEAD_BYTES, configuration.forkOverheadBytes());
    assertEquals(ConfigurationSource.DEFAULT, configuration.forkOverheadSource());
  }

  @Test
  void memoryPropertiesTakePrecedenceOverEnvironment() {
    MemoryConfiguration configuration =
        CoreDatabaseConcurrency.resolveMemoryConfiguration("5120", "6144", "1024", "2048");

    assertEquals(5120L * MIB, configuration.buildReserveBytes());
    assertEquals(ConfigurationSource.PROPERTY, configuration.buildReserveSource());
    assertEquals(1024L * MIB, configuration.forkOverheadBytes());
    assertEquals(ConfigurationSource.PROPERTY, configuration.forkOverheadSource());
  }

  @Test
  void memoryEnvironmentIsUsedWhenPropertiesAreAbsent() {
    MemoryConfiguration configuration =
        CoreDatabaseConcurrency.resolveMemoryConfiguration(null, "5120", null, "1024");

    assertEquals(5120L * MIB, configuration.buildReserveBytes());
    assertEquals(ConfigurationSource.ENVIRONMENT, configuration.buildReserveSource());
    assertEquals(1024L * MIB, configuration.forkOverheadBytes());
    assertEquals(ConfigurationSource.ENVIRONMENT, configuration.forkOverheadSource());
  }

  @ParameterizedTest
  @ValueSource(strings = {"", " ", "zero", "0", "-1", "1.5", "9223372036854775807"})
  void rejectsInvalidConfiguredMemory(String value) {
    assertThrows(
        IllegalArgumentException.class,
        () -> CoreDatabaseConcurrency.resolveMemoryConfiguration(value, null, null, null));
    assertThrows(
        IllegalArgumentException.class,
        () -> CoreDatabaseConcurrency.resolveMemoryConfiguration(null, null, value, null));
  }

  @Test
  void rolloutBudgetIncludesHeapAndNativeAllowance() {
    MemoryConfiguration configuration =
        CoreDatabaseConcurrency.resolveMemoryConfiguration(null, null, null, null);
    Budget budget = CoreDatabaseConcurrency.rolloutBudget(configuration);

    assertEquals(
        TEST_WORKER_MAX_HEAP_BYTES + DEFAULT_TEST_WORKER_OVERHEAD_BYTES,
        budget.memoryBytesPerFork());
    assertEquals(DEFAULT_BUILD_PROCESS_MEMORY_RESERVE_BYTES, budget.buildReserveBytes());
    assertEquals(2, budget.rolloutCap());
  }

  @Test
  void rejectsOverflowingRolloutBudget() {
    MemoryConfiguration configuration =
        new MemoryConfiguration(
            GIB, ConfigurationSource.DEFAULT, Long.MAX_VALUE, ConfigurationSource.PROPERTY);

    assertThrows(
        IllegalArgumentException.class, () -> CoreDatabaseConcurrency.rolloutBudget(configuration));
  }

  @Test
  void calculatesAvailableMemoryWithoutUnderflow() {
    assertEquals(
        12L * GIB, CoreDatabaseConcurrency.availableMemoryForTestWorkers(16L * GIB, 4L * GIB));
    assertEquals(0, CoreDatabaseConcurrency.availableMemoryForTestWorkers(2L * GIB, 4L * GIB));
    assertEquals(0, CoreDatabaseConcurrency.availableMemoryForTestWorkers(0, 4L * GIB));
  }

  @Test
  void fullyDetectedCapacityEnablesTwoForkRollout() {
    Resolution resolution = resolve(null, null, new DetectedCapacity(8, 64L * GIB, 8, 1), 0);

    assertEquals(2, resolution.forks());
    assertEquals(Source.DETECTED, resolution.source());
    assertEquals(LimitingFactor.ROLLOUT, resolution.limitingFactor());
    assertEquals(8, resolution.safeMaximum());
    assertEquals(2, resolution.allowedMaximum());
  }

  @Test
  void cpuCanLimitForks() {
    Resolution resolution = resolve(null, null, new DetectedCapacity(1, 64L * GIB, 8, 1), 0);

    assertEquals(1, resolution.forks());
    assertEquals(LimitingFactor.CPU, resolution.limitingFactor());
  }

  @Test
  void memoryCanLimitForks() {
    Resolution resolution = resolve(null, null, new DetectedCapacity(8, 10L * GIB, 8, 1), 0);

    assertEquals(1, resolution.forks());
    assertEquals(LimitingFactor.MEMORY, resolution.limitingFactor());
  }

  @Test
  void gradleWorkersCanLimitForks() {
    Resolution resolution = resolve(null, null, new DetectedCapacity(8, 64L * GIB, 1, 1), 0);

    assertEquals(1, resolution.forks());
    assertEquals(LimitingFactor.GRADLE, resolution.limitingFactor());
  }

  @Test
  void selectedLaneCountDividesDetectedCapacity() {
    Resolution resolution = resolve(null, null, new DetectedCapacity(2, 16L * GIB, 2, 2), 0);

    assertEquals(1, resolution.forks());
    assertEquals(1, resolution.safeMaximum());
    assertEquals(LimitingFactor.CPU, resolution.limitingFactor());
  }

  @Test
  void incompleteSignalsFallBackToOneFork() {
    Resolution resolution = resolve(null, null, new DetectedCapacity(8, 0, 8, 1), 0);

    assertEquals(1, resolution.forks());
    assertEquals(Source.DEFAULT, resolution.source());
    assertEquals(LimitingFactor.DEFAULT, resolution.limitingFactor());
    assertEquals(8, resolution.safeMaximum());
  }

  @Test
  void absentSignalsFallBackToOneFork() {
    Resolution resolution = resolve(null, null, new DetectedCapacity(0, 0, 0, 1), 0);

    assertEquals(1, resolution.forks());
    assertEquals(Source.DEFAULT, resolution.source());
    assertEquals(0, resolution.safeMaximum());
    assertEquals(2, resolution.allowedMaximum());
  }

  @Test
  void propertyOverrideTakesPrecedence() {
    Resolution resolution = resolve("1", "2", new DetectedCapacity(8, 64L * GIB, 8, 1), 0);

    assertEquals(1, resolution.forks());
    assertEquals(Source.PROPERTY, resolution.source());
  }

  @Test
  void environmentOverrideIsUsedWhenPropertyIsAbsent() {
    Resolution resolution = resolve(null, "2", new DetectedCapacity(8, 64L * GIB, 8, 1), 0);

    assertEquals(2, resolution.forks());
    assertEquals(Source.ENVIRONMENT, resolution.source());
  }

  @Test
  void exactOverrideCanUseUnknownCapacityWithinKnownLimits() {
    Resolution resolution = resolve("2", null, new DetectedCapacity(0, 0, 0, 1), 0);

    assertEquals(2, resolution.forks());
    assertEquals(Source.PROPERTY, resolution.source());
    assertEquals(2, resolution.allowedMaximum());
  }

  @ParameterizedTest
  @ValueSource(strings = {"", " ", "zero", "0", "-1", "1.5", "2147483648"})
  void rejectsMalformedExactOverrides(String value) {
    assertThrows(
        IllegalArgumentException.class,
        () -> resolve(value, null, new DetectedCapacity(8, 64L * GIB, 8, 1), 0));
  }

  @Test
  void rejectsOverrideAboveRolloutCap() {
    IllegalArgumentException failure =
        assertThrows(
            IllegalArgumentException.class,
            () -> resolve("3", null, new DetectedCapacity(8, 64L * GIB, 8, 1), 0));

    assertTrue(failure.getMessage().contains("allowed maximum is 2"));
  }

  @Test
  void rejectsOverrideAboveKnownPartialCapacity() {
    assertThrows(
        IllegalArgumentException.class,
        () -> resolve("2", null, new DetectedCapacity(1, 0, 0, 1), 0));
  }

  @Test
  void rejectsOverrideAbovePerLaneCapacity() {
    assertThrows(
        IllegalArgumentException.class,
        () -> resolve("2", null, new DetectedCapacity(2, 16L * GIB, 2, 2), 0));
  }

  @Test
  void fixedNetworkDockerLaneForcesOneFork() {
    int laneMaximum = CoreDatabaseConcurrency.laneMaximum(true, true);
    Resolution resolution =
        resolve(null, null, new DetectedCapacity(8, 64L * GIB, 8, 1), laneMaximum);

    assertEquals(1, resolution.forks());
    assertEquals(LimitingFactor.LANE, resolution.limitingFactor());
    assertThrows(
        IllegalArgumentException.class,
        () -> resolve("2", null, new DetectedCapacity(8, 64L * GIB, 8, 1), laneMaximum));
  }

  @Test
  void h2AndNonFixedNetworkDockerLanesHaveNoSpecialMaximum() {
    assertEquals(0, CoreDatabaseConcurrency.laneMaximum(false, true));
    assertEquals(0, CoreDatabaseConcurrency.laneMaximum(true, false));
  }

  @Test
  void rejectsInvalidCapacityAndBudgetInputs() {
    Budget budget = defaultBudget();
    assertThrows(
        IllegalArgumentException.class,
        () ->
            CoreDatabaseConcurrency.resolve(
                null, null, new DetectedCapacity(-1, 1, 1, 1), budget, 0));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            CoreDatabaseConcurrency.resolve(
                null, null, new DetectedCapacity(1, -1, 1, 1), budget, 0));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            CoreDatabaseConcurrency.resolve(
                null, null, new DetectedCapacity(1, 1, -1, 1), budget, 0));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            CoreDatabaseConcurrency.resolve(
                null, null, new DetectedCapacity(1, 1, 1, 0), budget, 0));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            CoreDatabaseConcurrency.resolve(
                null, null, new DetectedCapacity(1, 1, 1, 1), budget, -1));
  }

  private static Resolution resolve(
      String propertyValue, String environmentValue, DetectedCapacity capacity, int laneMaximum) {
    return CoreDatabaseConcurrency.resolve(
        propertyValue, environmentValue, capacity, defaultBudget(), laneMaximum);
  }

  private static Budget defaultBudget() {
    return CoreDatabaseConcurrency.rolloutBudget(
        CoreDatabaseConcurrency.resolveMemoryConfiguration(null, null, null, null));
  }
}
