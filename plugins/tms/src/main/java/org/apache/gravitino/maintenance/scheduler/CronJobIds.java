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

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Builds db-scheduler instance ids for one-shot {@code cron-job} rows (§5.2.2).
 *
 * <p>Most types use {@code table:{table_id}:{policy_id}}. Batched snapshot-expiry uses {@code
 * batch:{batch_key}:{policy_id}} where {@code batch_key} is a stable hash of the table id set.
 */
public final class CronJobIds {

  private CronJobIds() {}

  /**
   * @param tableId table entity id
   * @param policyId policy entity id
   * @return cron-job instance id
   */
  public static String forTable(long tableId, long policyId) {
    return "table:" + tableId + ":" + policyId;
  }

  /**
   * @param policyId policy entity id
   * @param tableIds tables covered by the batch (order ignored)
   * @return cron-job instance id for a snapshot-expiry batch
   */
  public static String forBatch(long policyId, List<Long> tableIds) {
    return "batch:" + batchKey(tableIds) + ":" + policyId;
  }

  /**
   * Stable short hash of the sorted table-id set.
   *
   * @param tableIds table entity ids
   * @return hex batch key
   */
  public static String batchKey(List<Long> tableIds) {
    List<Long> sorted = new ArrayList<>(tableIds == null ? List.of() : tableIds);
    Collections.sort(sorted);
    String material = sorted.stream().map(String::valueOf).collect(Collectors.joining(","));
    try {
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      byte[] hash = digest.digest(material.getBytes(StandardCharsets.UTF_8));
      StringBuilder hex = new StringBuilder(16);
      for (int i = 0; i < 8; i++) {
        hex.append(String.format("%02x", hash[i]));
      }
      return hex.toString();
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("SHA-256 not available", e);
    }
  }
}
