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
package org.apache.gravitino.maintenance;

import org.apache.gravitino.Config;
import org.apache.gravitino.config.ConfigBuilder;
import org.apache.gravitino.config.ConfigConstants;
import org.apache.gravitino.config.ConfigEntry;

/** Configuration for table maintenance scheduler pools (§7.2). */
public final class TableMaintenanceConfig {

  public static final ConfigEntry<Integer> SCHEDULER_EXPAND_THREADS =
      new ConfigBuilder("gravitino.maintenance.scheduler.expandThreads")
          .doc("Thread count for the policy-expand db-scheduler pool")
          .version(ConfigConstants.VERSION_2_0_0)
          .intConf()
          .checkValue(value -> value > 0, ConfigConstants.POSITIVE_NUMBER_ERROR_MSG)
          .createWithDefault(4);

  public static final ConfigEntry<Integer> SCHEDULER_CRON_THREADS =
      new ConfigBuilder("gravitino.maintenance.scheduler.cronThreads")
          .doc("Thread count for the cron-job db-scheduler pool")
          .version(ConfigConstants.VERSION_2_0_0)
          .intConf()
          .checkValue(value -> value > 0, ConfigConstants.POSITIVE_NUMBER_ERROR_MSG)
          .createWithDefault(8);

  public static final ConfigEntry<Integer> SCHEDULER_COMMIT_THREADS =
      new ConfigBuilder("gravitino.maintenance.scheduler.commitThreads")
          .doc("Thread count for the commit-job db-scheduler pool")
          .version(ConfigConstants.VERSION_2_0_0)
          .intConf()
          .checkValue(value -> value > 0, ConfigConstants.POSITIVE_NUMBER_ERROR_MSG)
          .createWithDefault(4);

  public static final ConfigEntry<Long> SCHEDULER_POLLING_INTERVAL_MS =
      new ConfigBuilder("gravitino.maintenance.scheduler.pollingIntervalMs")
          .doc("db-scheduler poll interval in milliseconds")
          .version(ConfigConstants.VERSION_2_0_0)
          .longConf()
          .checkValue(value -> value > 0, ConfigConstants.POSITIVE_NUMBER_ERROR_MSG)
          .createWithDefault(10_000L);

  public static final ConfigEntry<Long> SCHEDULER_HEARTBEAT_INTERVAL_MS =
      new ConfigBuilder("gravitino.maintenance.scheduler.heartbeatIntervalMs")
          .doc("Heartbeat interval while a scheduler callback runs")
          .version(ConfigConstants.VERSION_2_0_0)
          .longConf()
          .checkValue(value -> value > 0, ConfigConstants.POSITIVE_NUMBER_ERROR_MSG)
          .createWithDefault(60_000L);

  public static final ConfigEntry<Integer> SCHEDULER_MISSED_HEARTBEATS_LIMIT =
      new ConfigBuilder("gravitino.maintenance.scheduler.missedHeartbeatsLimit")
          .doc("Missed heartbeats before a picked task is considered dead")
          .version(ConfigConstants.VERSION_2_0_0)
          .intConf()
          .checkValue(value -> value > 0, ConfigConstants.POSITIVE_NUMBER_ERROR_MSG)
          .createWithDefault(6);

  public static final ConfigEntry<Integer> EXPAND_ENQUEUE_BATCH_SIZE =
      new ConfigBuilder("gravitino.maintenance.expand.enqueueBatchSize")
          .doc("Maximum cron-jobs enqueued per policy-expand pick")
          .version(ConfigConstants.VERSION_2_0_0)
          .intConf()
          .checkValue(value -> value > 0, ConfigConstants.POSITIVE_NUMBER_ERROR_MSG)
          .createWithDefault(100);

  /** Default unbound claim TTL ({@code job_id} still null), matching ShedLock-style short locks. */
  public static final long DEFAULT_CLAIM_TIMEOUT_MS = 120_000L;

  public static final ConfigEntry<Long> OCCUPANCY_CLAIM_TIMEOUT_MS =
      new ConfigBuilder("gravitino.maintenance.occupancy.claimTimeoutMs")
          .doc(
              "Max age of an unbound table_maintenance_action claim (job_id null) before another"
                  + " submitter may reclaim it")
          .version(ConfigConstants.VERSION_2_0_0)
          .longConf()
          .checkValue(value -> value > 0, ConfigConstants.POSITIVE_NUMBER_ERROR_MSG)
          .createWithDefault(DEFAULT_CLAIM_TIMEOUT_MS);

  private final int expandThreads;
  private final int cronThreads;
  private final int commitThreads;
  private final long pollingIntervalMs;
  private final long heartbeatIntervalMs;
  private final int missedHeartbeatsLimit;
  private final int expandEnqueueBatchSize;
  private final long claimTimeoutMs;

  private TableMaintenanceConfig(
      int expandThreads,
      int cronThreads,
      int commitThreads,
      long pollingIntervalMs,
      long heartbeatIntervalMs,
      int missedHeartbeatsLimit,
      int expandEnqueueBatchSize,
      long claimTimeoutMs) {
    this.expandThreads = expandThreads;
    this.cronThreads = cronThreads;
    this.commitThreads = commitThreads;
    this.pollingIntervalMs = pollingIntervalMs;
    this.heartbeatIntervalMs = heartbeatIntervalMs;
    this.missedHeartbeatsLimit = missedHeartbeatsLimit;
    this.expandEnqueueBatchSize = expandEnqueueBatchSize;
    this.claimTimeoutMs = claimTimeoutMs;
  }

  /**
   * Reads scheduler settings from server configuration.
   *
   * @param config gravitino server config
   * @return resolved maintenance config
   */
  public static TableMaintenanceConfig from(Config config) {
    return new TableMaintenanceConfig(
        config.get(SCHEDULER_EXPAND_THREADS),
        config.get(SCHEDULER_CRON_THREADS),
        config.get(SCHEDULER_COMMIT_THREADS),
        config.get(SCHEDULER_POLLING_INTERVAL_MS),
        config.get(SCHEDULER_HEARTBEAT_INTERVAL_MS),
        config.get(SCHEDULER_MISSED_HEARTBEATS_LIMIT),
        config.get(EXPAND_ENQUEUE_BATCH_SIZE),
        config.get(OCCUPANCY_CLAIM_TIMEOUT_MS));
  }

  /**
   * @return expand pool thread count
   */
  public int expandThreads() {
    return expandThreads;
  }

  /**
   * @return cron pool thread count
   */
  public int cronThreads() {
    return cronThreads;
  }

  /**
   * @return commit pool thread count
   */
  public int commitThreads() {
    return commitThreads;
  }

  /**
   * @return poll interval in milliseconds
   */
  public long pollingIntervalMs() {
    return pollingIntervalMs;
  }

  /**
   * @return heartbeat interval in milliseconds
   */
  public long heartbeatIntervalMs() {
    return heartbeatIntervalMs;
  }

  /**
   * @return missed heartbeat limit
   */
  public int missedHeartbeatsLimit() {
    return missedHeartbeatsLimit;
  }

  /**
   * @return expand enqueue batch size
   */
  public int expandEnqueueBatchSize() {
    return expandEnqueueBatchSize;
  }

  /**
   * @return unbound occupancy claim timeout in milliseconds
   */
  public long claimTimeoutMs() {
    return claimTimeoutMs;
  }

  /**
   * Test-only factory with explicit values.
   *
   * @param expandThreads expand pool threads
   * @param cronThreads cron pool threads
   * @param commitThreads commit pool threads
   * @param pollingIntervalMs poll interval
   * @param heartbeatIntervalMs heartbeat interval
   * @param missedHeartbeatsLimit missed heartbeat limit
   * @param expandEnqueueBatchSize expand batch size
   * @return config instance
   */
  public static TableMaintenanceConfig forTest(
      int expandThreads,
      int cronThreads,
      int commitThreads,
      long pollingIntervalMs,
      long heartbeatIntervalMs,
      int missedHeartbeatsLimit,
      int expandEnqueueBatchSize) {
    return forTest(
        expandThreads,
        cronThreads,
        commitThreads,
        pollingIntervalMs,
        heartbeatIntervalMs,
        missedHeartbeatsLimit,
        expandEnqueueBatchSize,
        DEFAULT_CLAIM_TIMEOUT_MS);
  }

  /**
   * Test-only factory with explicit values including claim timeout.
   *
   * @param expandThreads expand pool threads
   * @param cronThreads cron pool threads
   * @param commitThreads commit pool threads
   * @param pollingIntervalMs poll interval
   * @param heartbeatIntervalMs heartbeat interval
   * @param missedHeartbeatsLimit missed heartbeat limit
   * @param expandEnqueueBatchSize expand batch size
   * @param claimTimeoutMs unbound claim timeout
   * @return config instance
   */
  public static TableMaintenanceConfig forTest(
      int expandThreads,
      int cronThreads,
      int commitThreads,
      long pollingIntervalMs,
      long heartbeatIntervalMs,
      int missedHeartbeatsLimit,
      int expandEnqueueBatchSize,
      long claimTimeoutMs) {
    return new TableMaintenanceConfig(
        expandThreads,
        cronThreads,
        commitThreads,
        pollingIntervalMs,
        heartbeatIntervalMs,
        missedHeartbeatsLimit,
        expandEnqueueBatchSize,
        claimTimeoutMs);
  }
}
