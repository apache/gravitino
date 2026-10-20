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
package org.apache.gravitino.maintenance.scheduler.cron;

import java.util.Objects;
import java.util.Optional;
import javax.annotation.Nullable;

/**
 * Builds and parses db-scheduler instance ids for one-shot {@code cron-job} rows (§5.2.2).
 *
 * <p>Most types use {@code table:{table_id}:{policy_id}}. Batched snapshot-expiry uses {@code
 * batch:{batch_id}:{policy_id}} where {@code batch_id} is a per-policy sequence ({@code 0}, {@code
 * 1}, …) so concurrent batch names stay distinct under one {@code policy_id}.
 */
public final class CronJobIds {

  private static final String TABLE_PREFIX = "table:";
  private static final String BATCH_PREFIX = "batch:";

  private CronJobIds() {}

  /**
   * @param tableId table entity id
   * @param policyId policy entity id
   * @return cron-job instance id
   */
  public static String forTable(long tableId, long policyId) {
    return TABLE_PREFIX + tableId + ":" + policyId;
  }

  /**
   * @param batchId per-policy batch sequence id ({@code 0}, {@code 1}, …)
   * @param policyId policy entity id
   * @return cron-job instance id {@code batch:{batch_id}:{policy_id}}
   */
  public static String forBatch(long batchId, long policyId) {
    return BATCH_PREFIX + batchId + ":" + policyId;
  }

  /**
   * Parses a single-table cron-job instance id.
   *
   * @param instanceId db-scheduler task instance id
   * @return table and policy ids when the id matches {@code table:{tableId}:{policyId}}
   */
  public static Optional<TablePolicyIds> parseTable(String instanceId) {
    if (instanceId == null || !instanceId.startsWith(TABLE_PREFIX)) {
      return Optional.empty();
    }
    String rest = instanceId.substring(TABLE_PREFIX.length());
    int sep = rest.indexOf(':');
    if (sep <= 0 || sep == rest.length() - 1) {
      return Optional.empty();
    }
    try {
      long tableId = Long.parseLong(rest.substring(0, sep));
      long policyId = Long.parseLong(rest.substring(sep + 1));
      return Optional.of(new TablePolicyIds(tableId, policyId));
    } catch (NumberFormatException e) {
      return Optional.empty();
    }
  }

  /**
   * Parses a batched snapshot-expiry cron-job instance id.
   *
   * @param instanceId db-scheduler task instance id
   * @return batch and policy ids when the id matches {@code batch:{batchId}:{policyId}}
   */
  public static Optional<BatchPolicyIds> parseBatch(String instanceId) {
    if (instanceId == null || !instanceId.startsWith(BATCH_PREFIX)) {
      return Optional.empty();
    }
    String rest = instanceId.substring(BATCH_PREFIX.length());
    int sep = rest.indexOf(':');
    if (sep <= 0 || sep == rest.length() - 1) {
      return Optional.empty();
    }
    try {
      long batchId = Long.parseLong(rest.substring(0, sep));
      long policyId = Long.parseLong(rest.substring(sep + 1));
      return Optional.of(new BatchPolicyIds(batchId, policyId));
    } catch (NumberFormatException e) {
      return Optional.empty();
    }
  }

  /** Parsed {@code table:{tableId}:{policyId}} ids. */
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
      return forTable(tableId, policyId);
    }
  }

  /** Parsed {@code batch:{batchId}:{policyId}} ids. */
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
      return forBatch(batchId, policyId);
    }
  }
}
