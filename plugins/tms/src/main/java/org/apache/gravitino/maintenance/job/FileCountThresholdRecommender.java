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
package org.apache.gravitino.maintenance.job;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Preconditions;
import java.io.IOException;
import org.apache.gravitino.json.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Recommends {@link MaintenanceRecommendDecision#MAINTAIN} when metrics JSON {@code fileCount} is
 * greater than or equal to a configured threshold; otherwise {@link
 * MaintenanceRecommendDecision#SKIP}.
 *
 * <p>Placeholder until type-specific optimizer recommenders are wired. Missing or unreadable {@code
 * fileCount} yields {@code SKIP}.
 */
public class FileCountThresholdRecommender implements MaintenanceRecommender {

  private static final Logger LOG = LoggerFactory.getLogger(FileCountThresholdRecommender.class);

  private static final String FILE_COUNT_FIELD = "fileCount";

  private final long minFileCount;

  /**
   * @param minFileCount inclusive lower bound on {@code fileCount} to recommend maintain
   */
  public FileCountThresholdRecommender(long minFileCount) {
    Preconditions.checkArgument(minFileCount >= 0, "minFileCount must be >= 0");
    this.minFileCount = minFileCount;
  }

  @Override
  public MaintenanceRecommendDecision recommend(
      long tableId, long policyId, long sampledSnapshotId, String metricsJson) {
    Preconditions.checkNotNull(metricsJson, "metricsJson");
    try {
      JsonNode root = JsonUtils.objectMapper().readTree(metricsJson);
      if (root == null
          || !root.has(FILE_COUNT_FIELD)
          || !root.get(FILE_COUNT_FIELD).canConvertToLong()) {
        LOG.debug(
            "Skip maintain for tableId={} policyId={} snapshotId={}: missing fileCount in metrics",
            tableId,
            policyId,
            sampledSnapshotId);
        return MaintenanceRecommendDecision.SKIP;
      }
      long fileCount = root.get(FILE_COUNT_FIELD).asLong();
      if (fileCount >= minFileCount) {
        return MaintenanceRecommendDecision.MAINTAIN;
      }
      LOG.debug(
          "Skip maintain for tableId={} policyId={} snapshotId={}: fileCount={} < min={}",
          tableId,
          policyId,
          sampledSnapshotId,
          fileCount,
          minFileCount);
      return MaintenanceRecommendDecision.SKIP;
    } catch (IOException e) {
      LOG.warn(
          "Skip maintain for tableId={} policyId={} snapshotId={}: failed to parse metrics JSON",
          tableId,
          policyId,
          sampledSnapshotId,
          e);
      return MaintenanceRecommendDecision.SKIP;
    }
  }
}
