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
package org.apache.gravitino.maintenance.scheduler;

import java.util.Objects;
import java.util.Optional;
import javax.annotation.Nullable;

/**
 * Builds and parses db-scheduler {@code task_instance} ids for TMS expand / cron / commit pools
 * (§5.2).
 *
 * <p>Two separators (see {@link #KV_SEP} and {@link #PAIR_SEP}):
 *
 * <ul>
 *   <li>{@link #KV_SEP} {@code ':'} — separates a <em>key</em> from its <em>value</em> inside one
 *       pair ({@code table:10})
 *   <li>{@link #PAIR_SEP} {@code ';'} — separates adjacent pairs ({@code table:10;policy:20})
 * </ul>
 *
 * <p>Formats:
 *
 * <ul>
 *   <li>policy-expand: {@code policy:{policy_id}}
 *   <li>cron-job (table): {@code table:{table_id};policy:{policy_id}}
 *   <li>cron-job (batch): {@code batch:{batch_id};policy:{policy_id}}
 *   <li>commit-job: {@code table:{table_id}}
 * </ul>
 */
public final class MaintenanceTaskIds {

  /**
   * Key/value separator inside one pair. Example: {@code table} + {@link #KV_SEP} + {@code 10} →
   * {@code table:10}.
   */
  public static final char KV_SEP = ':';

  /**
   * Pair separator between adjacent {@code key}{@link #KV_SEP}{@code value} pairs. Example: {@code
   * table:10} + {@link #PAIR_SEP} + {@code policy:20} → {@code table:10;policy:20}.
   */
  public static final char PAIR_SEP = ';';

  private static final String POLICY_KEY = "policy";
  private static final String TABLE_KEY = "table";
  private static final String BATCH_KEY = "batch";

  private static final String POLICY_PAIR_PREFIX = POLICY_KEY + KV_SEP;
  private static final String TABLE_PAIR_PREFIX = TABLE_KEY + KV_SEP;
  private static final String BATCH_PAIR_PREFIX = BATCH_KEY + KV_SEP;
  /** Leading {@link #PAIR_SEP} plus the {@code policy}{@link #KV_SEP} prefix of the next pair. */
  private static final String POLICY_PAIR_SUFFIX = PAIR_SEP + POLICY_PAIR_PREFIX;

  private MaintenanceTaskIds() {}

  /**
   * @param policyId policy entity id
   * @return policy-expand instance id {@code policy:{policy_id}}
   */
  public static String forExpandPolicy(long policyId) {
    return pair(POLICY_KEY, policyId);
  }

  /**
   * @param tableId table entity id
   * @param policyId policy entity id
   * @return cron-job instance id {@code table:{table_id};policy:{policy_id}}
   */
  public static String forCronTable(long tableId, long policyId) {
    return pair(TABLE_KEY, tableId) + POLICY_PAIR_SUFFIX + policyId;
  }

  /**
   * @param batchId per-policy batch sequence id ({@code 0}, {@code 1}, …)
   * @param policyId policy entity id
   * @return cron-job instance id {@code batch:{batch_id};policy:{policy_id}}
   */
  public static String forCronBatch(long batchId, long policyId) {
    return pair(BATCH_KEY, batchId) + POLICY_PAIR_SUFFIX + policyId;
  }

  /**
   * @param tableId table entity id
   * @return commit-job instance id {@code table:{table_id}}
   */
  public static String forCommitTable(long tableId) {
    return pair(TABLE_KEY, tableId);
  }

  /**
   * Parses a policy-expand instance id.
   *
   * @param instanceId db-scheduler task instance id
   * @return policy id when the id is a single {@code policy:{policyId}} pair
   */
  public static Optional<Long> parseExpandPolicy(String instanceId) {
    return parseSinglePair(instanceId, POLICY_PAIR_PREFIX);
  }

  /**
   * Parses a single-table cron-job instance id.
   *
   * @param instanceId db-scheduler task instance id
   * @return table and policy ids when the id matches {@code table:{tableId};policy:{policyId}}
   */
  public static Optional<TablePolicyIds> parseCronTable(String instanceId) {
    return parseLeftThenPolicy(instanceId, TABLE_PAIR_PREFIX)
        .map(parts -> new TablePolicyIds(parts[0], parts[1]));
  }

  /**
   * Parses a batched snapshot-expiry cron-job instance id.
   *
   * @param instanceId db-scheduler task instance id
   * @return batch and policy ids when the id matches {@code batch:{batchId};policy:{policyId}}
   */
  public static Optional<BatchPolicyIds> parseCronBatch(String instanceId) {
    return parseLeftThenPolicy(instanceId, BATCH_PAIR_PREFIX)
        .map(parts -> new BatchPolicyIds(parts[0], parts[1]));
  }

  /**
   * Parses a commit-job instance id.
   *
   * @param instanceId db-scheduler task instance id
   * @return table id when the id is a single {@code table:{tableId}} pair (no {@link #PAIR_SEP})
   */
  public static Optional<Long> parseCommitTable(String instanceId) {
    return parseSinglePair(instanceId, TABLE_PAIR_PREFIX);
  }

  private static String pair(String key, long value) {
    return key + KV_SEP + value;
  }

  private static Optional<Long> parseSinglePair(String instanceId, String pairPrefix) {
    if (instanceId == null || !instanceId.startsWith(pairPrefix)) {
      return Optional.empty();
    }
    String value = instanceId.substring(pairPrefix.length());
    // A single pair must not contain KV_SEP or PAIR_SEP inside the value.
    if (value.isEmpty() || value.indexOf(KV_SEP) >= 0 || value.indexOf(PAIR_SEP) >= 0) {
      return Optional.empty();
    }
    try {
      return Optional.of(Long.parseLong(value));
    } catch (NumberFormatException e) {
      return Optional.empty();
    }
  }

  private static Optional<long[]> parseLeftThenPolicy(String instanceId, String leftPairPrefix) {
    if (instanceId == null || !instanceId.startsWith(leftPairPrefix)) {
      return Optional.empty();
    }
    String rest = instanceId.substring(leftPairPrefix.length());
    int sep = rest.indexOf(POLICY_PAIR_SUFFIX);
    if (sep <= 0 || sep + POLICY_PAIR_SUFFIX.length() >= rest.length()) {
      return Optional.empty();
    }
    String leftValue = rest.substring(0, sep);
    String policyValue = rest.substring(sep + POLICY_PAIR_SUFFIX.length());
    if (leftValue.indexOf(KV_SEP) >= 0
        || leftValue.indexOf(PAIR_SEP) >= 0
        || policyValue.indexOf(KV_SEP) >= 0
        || policyValue.indexOf(PAIR_SEP) >= 0) {
      return Optional.empty();
    }
    try {
      return Optional.of(new long[] {Long.parseLong(leftValue), Long.parseLong(policyValue)});
    } catch (NumberFormatException e) {
      return Optional.empty();
    }
  }

  /** Parsed {@code table:{tableId};policy:{policyId}} ids. */
  public static final class TablePolicyIds {
    private final long tableId;
    private final long policyId;

    /**
     * @param tableId table entity id
     * @param policyId policy entity id
     */
    public TablePolicyIds(long tableId, long policyId) {
      this.tableId = tableId;
      this.policyId = policyId;
    }

    /**
     * @return table entity id
     */
    public long tableId() {
      return tableId;
    }

    /**
     * @return policy entity id
     */
    public long policyId() {
      return policyId;
    }

    @Override
    public boolean equals(@Nullable Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof TablePolicyIds)) {
        return false;
      }
      TablePolicyIds that = (TablePolicyIds) o;
      return tableId == that.tableId && policyId == that.policyId;
    }

    @Override
    public int hashCode() {
      return Objects.hash(tableId, policyId);
    }

    @Override
    public String toString() {
      return forCronTable(tableId, policyId);
    }
  }

  /** Parsed {@code batch:{batchId};policy:{policyId}} ids. */
  public static final class BatchPolicyIds {
    private final long batchId;
    private final long policyId;

    /**
     * @param batchId per-policy batch sequence id
     * @param policyId policy entity id
     */
    public BatchPolicyIds(long batchId, long policyId) {
      this.batchId = batchId;
      this.policyId = policyId;
    }

    /**
     * @return batch sequence id
     */
    public long batchId() {
      return batchId;
    }

    /**
     * @return policy entity id
     */
    public long policyId() {
      return policyId;
    }

    @Override
    public boolean equals(@Nullable Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof BatchPolicyIds)) {
        return false;
      }
      BatchPolicyIds that = (BatchPolicyIds) o;
      return batchId == that.batchId && policyId == that.policyId;
    }

    @Override
    public int hashCode() {
      return Objects.hash(batchId, policyId);
    }

    @Override
    public String toString() {
      return forCronBatch(batchId, policyId);
    }
  }
}
