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
package org.apache.gravitino.storage.relational.service;

import javax.annotation.Nullable;
import org.apache.gravitino.storage.relational.mapper.TableSnapshotMetricsMapper;
import org.apache.gravitino.storage.relational.po.TableSnapshotMetricsPO;
import org.apache.gravitino.storage.relational.utils.SessionUtils;

/** DAO service for {@code table_snapshot_metrics}. */
public class TableSnapshotMetricsMetaService {

  /**
   * Stores metrics JSON for a table/policy/snapshot triple.
   *
   * @param tableId table id
   * @param policyId policy id
   * @param snapshotId Iceberg snapshot id
   * @param metricsJson serialized metrics payload
   */
  public void putMetrics(long tableId, long policyId, long snapshotId, String metricsJson) {
    TableSnapshotMetricsPO existing = getMetrics(tableId, policyId, snapshotId);
    if (existing == null) {
      TableSnapshotMetricsPO po =
          TableSnapshotMetricsPO.builder()
              .withTableId(tableId)
              .withPolicyId(policyId)
              .withSnapshotId(snapshotId)
              .withMetricsValue(metricsJson)
              .build();
      SessionUtils.doWithCommit(
          TableSnapshotMetricsMapper.class, mapper -> mapper.insertMetrics(po));
    } else {
      TableSnapshotMetricsPO po =
          TableSnapshotMetricsPO.builder()
              .withTableId(tableId)
              .withPolicyId(policyId)
              .withSnapshotId(snapshotId)
              .withMetricsValue(metricsJson)
              .build();
      SessionUtils.doWithCommit(
          TableSnapshotMetricsMapper.class, mapper -> mapper.updateMetrics(po));
    }
  }

  /**
   * @param tableId table id
   * @param policyId policy id
   * @param snapshotId snapshot id
   * @return stored metrics or null
   */
  @Nullable
  public TableSnapshotMetricsPO getMetrics(long tableId, long policyId, long snapshotId) {
    return SessionUtils.getWithoutCommit(
        TableSnapshotMetricsMapper.class,
        mapper -> mapper.selectMetrics(tableId, policyId, snapshotId));
  }
}
