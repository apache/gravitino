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
package org.apache.gravitino.maintenance.storage.service;

import static org.apache.gravitino.metrics.source.MetricsSource.GRAVITINO_RELATIONAL_STORE_METRIC_NAME;

import javax.annotation.Nullable;
import org.apache.gravitino.maintenance.storage.mapper.TableSnapshotMetricsMapper;
import org.apache.gravitino.maintenance.storage.po.TableSnapshotMetricsPO;
import org.apache.gravitino.metrics.Monitored;
import org.apache.gravitino.storage.relational.utils.SessionUtils;

/** DAO service for {@code table_snapshot_metrics}. */
public class TableSnapshotMetricsMetaService {

  private static final TableSnapshotMetricsMetaService INSTANCE =
      new TableSnapshotMetricsMetaService();

  private TableSnapshotMetricsMetaService() {}

  /**
   * @return singleton service instance
   */
  public static TableSnapshotMetricsMetaService getInstance() {
    return INSTANCE;
  }

  /**
   * Inserts a metrics row and returns its generated id. The same Iceberg {@code snapshotId} may be
   * stored more than once (S0 and S1 can share a snapshot).
   *
   * @param tableId table id
   * @param policyId policy id
   * @param snapshotId Iceberg snapshot id sampled with these metrics
   * @param metricsJson serialized metrics payload
   * @return generated {@code table_snapshot_metrics.metrics_id}
   */
  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "insertTableSnapshotMetrics")
  public long insertMetrics(long tableId, long policyId, long snapshotId, String metricsJson) {
    TableSnapshotMetricsPO metricsMeta =
        TableSnapshotMetricsPO.builder()
            .withTableId(tableId)
            .withPolicyId(policyId)
            .withSnapshotId(snapshotId)
            .withMetricsValue(metricsJson)
            .withCreatedAt(System.currentTimeMillis())
            .build();
    SessionUtils.doWithCommit(
        TableSnapshotMetricsMapper.class, mapper -> mapper.insertMetrics(metricsMeta));
    return metricsMeta.metricsId();
  }

  /**
   * @param metricsId metrics row id
   * @return stored metrics or null
   */
  @Nullable
  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "getTableSnapshotMetricsById")
  public TableSnapshotMetricsPO getMetrics(long metricsId) {
    return SessionUtils.getWithoutCommit(
        TableSnapshotMetricsMapper.class, mapper -> mapper.selectById(metricsId));
  }
}
