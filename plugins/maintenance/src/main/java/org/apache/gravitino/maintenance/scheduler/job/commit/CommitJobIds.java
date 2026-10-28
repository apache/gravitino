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
package org.apache.gravitino.maintenance.scheduler.job.commit;

import java.util.Optional;

/**
 * Builds and parses db-scheduler instance ids for one-shot {@code commit-job} rows (§5.2.3).
 *
 * <p>Format: {@code table:{table_id}} (no policy segment — one coalesce key per table).
 */
public final class CommitJobIds {

  private static final String TABLE_PREFIX = "table:";

  private CommitJobIds() {}

  /**
   * @param tableId table entity id
   * @return commit-job instance id {@code table:{table_id}}
   */
  public static String forTable(long tableId) {
    return TABLE_PREFIX + tableId;
  }

  /**
   * Parses a commit-job instance id.
   *
   * @param instanceId db-scheduler task instance id
   * @return table id when the id matches {@code table:{tableId}} with no further segments
   */
  public static Optional<Long> parseTable(String instanceId) {
    if (instanceId == null || !instanceId.startsWith(TABLE_PREFIX)) {
      return Optional.empty();
    }
    String rest = instanceId.substring(TABLE_PREFIX.length());
    // Reject cron-job style table:{tableId};policy:{policyId} (and any multi-segment id).
    if (rest.isEmpty() || rest.indexOf(':') >= 0 || rest.indexOf(';') >= 0) {
      return Optional.empty();
    }
    try {
      return Optional.of(Long.parseLong(rest));
    } catch (NumberFormatException e) {
      return Optional.empty();
    }
  }
}
