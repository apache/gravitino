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

import javax.annotation.Nullable;

/** Calculates a conservative number of forks for the Core database test tasks. */
public final class CoreDatabaseConcurrency {

  private static final long MIB = 1024L * 1024L;
  private static final long TEST_WORKER_MAX_HEAP_MIB = 4096L;
  private static final int ROLLOUT_CAP = 2;
  private static final int DEFAULT_FORKS = 1;

  /** Maximum-heap JVM argument configured for every Gradle test worker. */
  public static final String TEST_WORKER_MAX_HEAP_ARGUMENT =
      "-Xmx" + TEST_WORKER_MAX_HEAP_MIB + "m";

  /** Maximum heap configured for every Gradle test worker. */
  public static final long TEST_WORKER_MAX_HEAP_BYTES = TEST_WORKER_MAX_HEAP_MIB * MIB;

  /** Default native-memory and database-container allowance for each test worker. */
  public static final long DEFAULT_TEST_WORKER_OVERHEAD_BYTES = 2048L * MIB;

  /** Default memory kept for the Gradle daemon and other build processes. */
  public static final long DEFAULT_BUILD_PROCESS_MEMORY_RESERVE_BYTES = 4096L * MIB;

  /** Gradle property for the build-process memory reserve, in MiB. */
  public static final String BUILD_MEMORY_RESERVE_PROPERTY = "coreDatabaseBuildMemoryReserveMiB";

  /** Environment variable for the build-process memory reserve, in MiB. */
  public static final String BUILD_MEMORY_RESERVE_ENVIRONMENT_VARIABLE =
      "CORE_DATABASE_BUILD_MEMORY_RESERVE_MIB";

  /** Gradle property for native and container memory overhead per test worker, in MiB. */
  public static final String FORK_MEMORY_OVERHEAD_PROPERTY = "coreDatabaseForkMemoryOverheadMiB";

  /** Environment variable for native and container memory overhead per test worker, in MiB. */
  public static final String FORK_MEMORY_OVERHEAD_ENVIRONMENT_VARIABLE =
      "CORE_DATABASE_FORK_MEMORY_OVERHEAD_MIB";

  /** Gradle property for an exact database-test fork count. */
  public static final String FORKS_PROPERTY = "coreDatabaseForks";

  /** Environment variable for an exact database-test fork count. */
  public static final String FORKS_ENVIRONMENT_VARIABLE = "CORE_DATABASE_FORKS";

  /** Root-project extra-property key for the mac-docker-connector fixed-network predicate. */
  public static final String MAC_DOCKER_CONNECTOR_FIXED_NETWORK_EXTRA =
      "macDockerConnectorFixedNetwork";

  /** Maximum safe fork count for a Docker lane using mac-docker-connector's fixed network. */
  public static final int MAC_DOCKER_CONNECTOR_MAX_FORKS = 1;

  /** Indicates that a lane has no task-specific maximum. */
  public static final int NO_LANE_MAXIMUM = 0;

  private CoreDatabaseConcurrency() {}

  /** Identifies where an operator-configurable memory value came from. */
  public enum ConfigurationSource {
    /** A Gradle property supplied the value. */
    PROPERTY,
    /** An environment variable supplied the value. */
    ENVIRONMENT,
    /** The built-in conservative default supplied the value. */
    DEFAULT
  }

  /** Identifies how the selected fork count was resolved. */
  public enum Source {
    /** The {@link #FORKS_PROPERTY} value was used. */
    PROPERTY,
    /** The {@link #FORKS_ENVIRONMENT_VARIABLE} value was used. */
    ENVIRONMENT,
    /** All detected machine and build limits were used. */
    DETECTED,
    /** Capacity detection was absent or partial, so the serial fallback was used. */
    DEFAULT
  }

  /** Resource that constrained the calculated fork count. */
  public enum LimitingFactor {
    /** Processor count, divided across the selected database lanes. */
    CPU,
    /** Available memory, divided across the selected database lanes. */
    MEMORY,
    /** Gradle worker count, divided across the selected database lanes. */
    GRADLE,
    /** Operational rollout ceiling. */
    ROLLOUT,
    /** Task-specific lane constraint. */
    LANE,
    /** Conservative fallback used when capacity detection is absent or partial. */
    DEFAULT
  }

  /**
   * Resolves the configurable memory assumptions without reading process-global state.
   *
   * <p>Each Gradle property takes precedence over its corresponding environment variable.
   * Configured values must be positive whole MiB counts.
   *
   * @param buildReservePropertyValue value of {@link #BUILD_MEMORY_RESERVE_PROPERTY}, or null
   * @param buildReserveEnvironmentValue value of {@link
   *     #BUILD_MEMORY_RESERVE_ENVIRONMENT_VARIABLE}, or null
   * @param forkOverheadPropertyValue value of {@link #FORK_MEMORY_OVERHEAD_PROPERTY}, or null
   * @param forkOverheadEnvironmentValue value of {@link
   *     #FORK_MEMORY_OVERHEAD_ENVIRONMENT_VARIABLE}, or null
   * @return the resolved memory assumptions and their sources
   */
  public static MemoryConfiguration resolveMemoryConfiguration(
      @Nullable String buildReservePropertyValue,
      @Nullable String buildReserveEnvironmentValue,
      @Nullable String forkOverheadPropertyValue,
      @Nullable String forkOverheadEnvironmentValue) {
    ResolvedBytes buildReserve =
        resolvePositiveMebibytes(
            buildReservePropertyValue,
            buildReserveEnvironmentValue,
            DEFAULT_BUILD_PROCESS_MEMORY_RESERVE_BYTES,
            BUILD_MEMORY_RESERVE_PROPERTY,
            BUILD_MEMORY_RESERVE_ENVIRONMENT_VARIABLE);
    ResolvedBytes forkOverhead =
        resolvePositiveMebibytes(
            forkOverheadPropertyValue,
            forkOverheadEnvironmentValue,
            DEFAULT_TEST_WORKER_OVERHEAD_BYTES,
            FORK_MEMORY_OVERHEAD_PROPERTY,
            FORK_MEMORY_OVERHEAD_ENVIRONMENT_VARIABLE);

    return new MemoryConfiguration(
        buildReserve.bytes(), buildReserve.source(), forkOverhead.bytes(), forkOverhead.source());
  }

  /**
   * Returns the conservative per-fork resource budget using resolved memory assumptions.
   *
   * @param memoryConfiguration resolved build and worker memory assumptions
   * @return per-fork memory budget and rollout ceiling
   */
  public static Budget rolloutBudget(MemoryConfiguration memoryConfiguration) {
    if (memoryConfiguration == null) {
      throw new IllegalArgumentException("memory configuration must not be null");
    }
    if (memoryConfiguration.buildReserveBytes() <= 0) {
      throw new IllegalArgumentException("build reserve bytes must be positive");
    }
    if (memoryConfiguration.forkOverheadBytes() <= 0) {
      throw new IllegalArgumentException("fork overhead bytes must be positive");
    }

    try {
      return new Budget(
          Math.addExact(TEST_WORKER_MAX_HEAP_BYTES, memoryConfiguration.forkOverheadBytes()),
          memoryConfiguration.buildReserveBytes(),
          ROLLOUT_CAP);
    } catch (ArithmeticException e) {
      throw new IllegalArgumentException(
          "test-worker memory budget exceeds the supported range", e);
    }
  }

  /**
   * Calculates memory available to database-test workers from JVM-visible physical memory.
   *
   * @param totalMemoryBytes total memory visible to the Gradle JVM; zero means unavailable
   * @param buildReserveBytes memory retained for Gradle and other build processes
   * @return memory left after the build-process reserve
   */
  public static long availableMemoryForTestWorkers(long totalMemoryBytes, long buildReserveBytes) {
    if (totalMemoryBytes < 0) {
      throw new IllegalArgumentException("total memory bytes must not be negative");
    }
    if (buildReserveBytes <= 0) {
      throw new IllegalArgumentException("build reserve bytes must be positive");
    }
    if (totalMemoryBytes == 0) {
      return 0;
    }
    return Math.max(0, totalMemoryBytes - buildReserveBytes);
  }

  /**
   * Returns the fixed lane maximum for a Docker task using mac-docker-connector.
   *
   * @param dockerLane whether the database lane requires Docker
   * @param macDockerConnectorFixedNetwork whether the shared fixed network is active
   * @return one when both inputs are true, otherwise {@link #NO_LANE_MAXIMUM}
   */
  public static int laneMaximum(boolean dockerLane, boolean macDockerConnectorFixedNetwork) {
    return dockerLane && macDockerConnectorFixedNetwork
        ? MAC_DOCKER_CONNECTOR_MAX_FORKS
        : NO_LANE_MAXIMUM;
  }

  /**
   * Resolves the fork count without reading process-global state.
   *
   * <p>The Gradle property takes precedence over the environment variable. Exact requests are
   * checked against every known capacity bound, the rollout cap, and the lane maximum. Without an
   * exact request, every capacity signal must be known before automatic parallelism is enabled.
   * Machine capacity is divided across the database lanes selected by the same Gradle invocation.
   *
   * @param propertyValue value of {@link #FORKS_PROPERTY}, or null
   * @param environmentValue value of {@link #FORKS_ENVIRONMENT_VARIABLE}, or null
   * @param capacity detected machine, build, and lane capacity
   * @param budget per-fork memory budget and rollout cap
   * @param laneMaximum task-specific maximum, or {@link #NO_LANE_MAXIMUM}
   * @return the selected fork count and its resolution details
   */
  public static Resolution resolve(
      @Nullable String propertyValue,
      @Nullable String environmentValue,
      DetectedCapacity capacity,
      Budget budget,
      int laneMaximum) {
    validate(capacity, budget, laneMaximum);

    CapacityLimit capacityLimit = calculateCapacityLimit(capacity, budget);
    AppliedLimit allowedLimit = calculateAllowedLimit(capacityLimit, budget, laneMaximum);
    String exactValue = propertyValue != null ? propertyValue : environmentValue;
    Source exactSource = propertyValue != null ? Source.PROPERTY : Source.ENVIRONMENT;

    if (exactValue != null) {
      int requestedForks = parseExactForks(exactValue, exactSource);
      if (requestedForks > allowedLimit.maximum()) {
        throw new IllegalArgumentException(
            String.format(
                "%s requests %d database forks, but the allowed maximum is %d "
                    + "(known per-lane capacity maximum %s, rollout cap %d, lane maximum %s)",
                sourceName(exactSource),
                requestedForks,
                allowedLimit.maximum(),
                displayMaximum(capacityLimit.detected(), capacityLimit.maximum()),
                budget.rolloutCap(),
                displayMaximum(laneMaximum > 0, laneMaximum)));
      }
      return new Resolution(
          requestedForks,
          capacityLimit.detected() ? capacityLimit.maximum() : 0,
          allowedLimit.maximum(),
          exactSource,
          allowedLimit.factor());
    }

    if (!capacityLimit.fullyDetected()) {
      return new Resolution(
          DEFAULT_FORKS,
          capacityLimit.detected() ? capacityLimit.maximum() : 0,
          allowedLimit.maximum(),
          Source.DEFAULT,
          LimitingFactor.DEFAULT);
    }

    return new Resolution(
        allowedLimit.maximum(),
        capacityLimit.maximum(),
        allowedLimit.maximum(),
        Source.DETECTED,
        allowedLimit.factor());
  }

  private static void validate(DetectedCapacity capacity, Budget budget, int laneMaximum) {
    if (capacity == null) {
      throw new IllegalArgumentException("detected capacity must not be null");
    }
    if (budget == null) {
      throw new IllegalArgumentException("resource budget must not be null");
    }
    if (capacity.processors() < 0) {
      throw new IllegalArgumentException("detected processor count must not be negative");
    }
    if (capacity.totalMemoryBytes() < 0) {
      throw new IllegalArgumentException("detected total memory must not be negative");
    }
    if (capacity.gradleMaxWorkers() < 0) {
      throw new IllegalArgumentException("Gradle maximum workers must not be negative");
    }
    if (capacity.activeDatabaseLanes() <= 0) {
      throw new IllegalArgumentException("active database lane count must be positive");
    }
    if (budget.memoryBytesPerFork() <= 0) {
      throw new IllegalArgumentException("memory bytes per fork must be positive");
    }
    if (budget.buildReserveBytes() <= 0) {
      throw new IllegalArgumentException("build reserve bytes must be positive");
    }
    if (budget.rolloutCap() <= 0) {
      throw new IllegalArgumentException("rollout cap must be positive");
    }
    if (laneMaximum < 0) {
      throw new IllegalArgumentException("lane maximum must not be negative");
    }
  }

  private static CapacityLimit calculateCapacityLimit(DetectedCapacity capacity, Budget budget) {
    int maximum = Integer.MAX_VALUE;
    LimitingFactor factor = LimitingFactor.DEFAULT;
    int detectedSignals = 0;

    if (capacity.processors() > 0) {
      maximum = perLane(capacity.processors(), capacity.activeDatabaseLanes());
      factor = LimitingFactor.CPU;
      detectedSignals++;
    }

    if (capacity.totalMemoryBytes() > 0) {
      long availableMemoryBytes =
          availableMemoryForTestWorkers(capacity.totalMemoryBytes(), budget.buildReserveBytes());
      long memoryForks =
          Math.max(DEFAULT_FORKS, availableMemoryBytes / budget.memoryBytesPerFork());
      int boundedMemoryForks = (int) Math.min(memoryForks, Integer.MAX_VALUE);
      int memoryForksPerLane = perLane(boundedMemoryForks, capacity.activeDatabaseLanes());
      if (memoryForksPerLane < maximum) {
        maximum = memoryForksPerLane;
        factor = LimitingFactor.MEMORY;
      }
      detectedSignals++;
    }

    if (capacity.gradleMaxWorkers() > 0) {
      int gradleWorkersPerLane =
          perLane(capacity.gradleMaxWorkers(), capacity.activeDatabaseLanes());
      if (gradleWorkersPerLane < maximum) {
        maximum = gradleWorkersPerLane;
        factor = LimitingFactor.GRADLE;
      }
      detectedSignals++;
    }

    if (detectedSignals == 0) {
      return new CapacityLimit(0, LimitingFactor.DEFAULT, false, false);
    }
    return new CapacityLimit(maximum, factor, true, detectedSignals == 3);
  }

  private static AppliedLimit calculateAllowedLimit(
      CapacityLimit capacityLimit, Budget budget, int laneMaximum) {
    int maximum = budget.rolloutCap();
    LimitingFactor factor = LimitingFactor.ROLLOUT;

    if (capacityLimit.detected() && capacityLimit.maximum() <= maximum) {
      maximum = capacityLimit.maximum();
      factor = capacityLimit.factor();
    }
    if (laneMaximum > 0 && laneMaximum <= maximum) {
      maximum = laneMaximum;
      factor = LimitingFactor.LANE;
    }

    return new AppliedLimit(maximum, factor);
  }

  private static int perLane(int capacity, int activeDatabaseLanes) {
    return Math.max(DEFAULT_FORKS, capacity / activeDatabaseLanes);
  }

  private static ResolvedBytes resolvePositiveMebibytes(
      @Nullable String propertyValue,
      @Nullable String environmentValue,
      long defaultBytes,
      String propertyName,
      String environmentName) {
    if (propertyValue != null) {
      return new ResolvedBytes(
          parsePositiveMebibytes(propertyValue, propertyName), ConfigurationSource.PROPERTY);
    }
    if (environmentValue != null) {
      return new ResolvedBytes(
          parsePositiveMebibytes(environmentValue, environmentName),
          ConfigurationSource.ENVIRONMENT);
    }
    return new ResolvedBytes(defaultBytes, ConfigurationSource.DEFAULT);
  }

  private static long parsePositiveMebibytes(String value, String name) {
    String trimmed = value.trim();
    if (trimmed.isEmpty()) {
      throw positiveMebibytesError(name, null);
    }

    try {
      long mebibytes = Long.parseLong(trimmed);
      if (mebibytes <= 0) {
        throw positiveMebibytesError(name, null);
      }
      return Math.multiplyExact(mebibytes, MIB);
    } catch (NumberFormatException | ArithmeticException e) {
      throw positiveMebibytesError(name, e);
    }
  }

  private static IllegalArgumentException positiveMebibytesError(
      String name, @Nullable RuntimeException cause) {
    String message = name + " must be a positive whole number of MiB";
    return cause == null
        ? new IllegalArgumentException(message)
        : new IllegalArgumentException(message, cause);
  }

  private static int parseExactForks(String value, Source source) {
    String trimmed = value.trim();
    if (trimmed.isEmpty()) {
      throw new IllegalArgumentException(sourceName(source) + " must be a positive integer");
    }

    try {
      int parsed = Integer.parseInt(trimmed);
      if (parsed <= 0) {
        throw new IllegalArgumentException(sourceName(source) + " must be a positive integer");
      }
      return parsed;
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(sourceName(source) + " must be a positive integer", e);
    }
  }

  private static String sourceName(Source source) {
    return source == Source.PROPERTY ? FORKS_PROPERTY : FORKS_ENVIRONMENT_VARIABLE;
  }

  private static String displayMaximum(boolean known, int maximum) {
    return known ? Integer.toString(maximum) : "unknown";
  }

  /**
   * Resolved build-process and per-worker memory assumptions.
   *
   * @param buildReserveBytes memory retained for Gradle and other build processes
   * @param buildReserveSource input that supplied {@code buildReserveBytes}
   * @param forkOverheadBytes native and database-container allowance per fork
   * @param forkOverheadSource input that supplied {@code forkOverheadBytes}
   */
  public record MemoryConfiguration(
      long buildReserveBytes,
      ConfigurationSource buildReserveSource,
      long forkOverheadBytes,
      ConfigurationSource forkOverheadSource) {}

  /**
   * Machine, Gradle, and lane capacity detected by the caller.
   *
   * <p>A zero processor, memory, or Gradle-worker value means that signal was unavailable.
   *
   * @param processors available processors
   * @param totalMemoryBytes total memory visible to the Gradle JVM before the build reserve
   * @param gradleMaxWorkers maximum Gradle workers available to the build
   * @param activeDatabaseLanes database lanes selected by the same Gradle invocation
   */
  public record DetectedCapacity(
      int processors, long totalMemoryBytes, int gradleMaxWorkers, int activeDatabaseLanes) {}

  /**
   * Resource budget for a database-test fork.
   *
   * @param memoryBytesPerFork memory reserved for each fork
   * @param buildReserveBytes memory retained for Gradle and other build processes
   * @param rolloutCap operational ceiling for parallel forks
   */
  public record Budget(long memoryBytesPerFork, long buildReserveBytes, int rolloutCap) {}

  /**
   * Result of resolving database-test concurrency.
   *
   * @param forks selected fork count
   * @param safeMaximum smallest known per-lane capacity bound, or zero when none is known
   * @param allowedMaximum smallest detected, rollout, and task-specific lane bound
   * @param source input that selected {@code forks}
   * @param limitingFactor bound that determined {@code allowedMaximum}
   */
  public record Resolution(
      int forks,
      int safeMaximum,
      int allowedMaximum,
      Source source,
      LimitingFactor limitingFactor) {}

  private record ResolvedBytes(long bytes, ConfigurationSource source) {}

  private record CapacityLimit(
      int maximum, LimitingFactor factor, boolean detected, boolean fullyDetected) {}

  private record AppliedLimit(int maximum, LimitingFactor factor) {}
}
