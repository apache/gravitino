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

import java.sql.SQLIntegrityConstraintViolationException;
import java.util.Locale;
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
   * Idempotently stores metrics JSON for a table/policy/snapshot triple. If a row already exists,
   * this is a no-op (does not overwrite) so sample / listener retries are safe.
   *
   * <p>A concurrent insert that loses the unique-key race is treated as success (first writer
   * wins).
   *
   * @param tableId table id
   * @param policyId policy id
   * @param snapshotId Iceberg snapshot id
   * @param metricsJson serialized metrics payload
   */
  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "insertTableSnapshotMetrics")
  public void insertMetrics(long tableId, long policyId, long snapshotId, String metricsJson) {
    if (getMetrics(tableId, policyId, snapshotId) != null) {
      return;
    }
    TableSnapshotMetricsPO metricsMeta =
        TableSnapshotMetricsPO.builder()
            .withTableId(tableId)
            .withPolicyId(policyId)
            .withSnapshotId(snapshotId)
            .withMetricsValue(metricsJson)
            .withCreatedAt(System.currentTimeMillis())
            .build();
    try {
      SessionUtils.doWithCommit(
          TableSnapshotMetricsMapper.class, mapper -> mapper.insertMetrics(metricsMeta));
    } catch (RuntimeException e) {
      if (isUniqueViolation(e)) {
        return;
      }
      throw e;
    }
  }

  /**
   * @param tableId table id
   * @param policyId policy id
   * @param snapshotId snapshot id
   * @return stored metrics or null
   */
  @Nullable
  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "getTableSnapshotMetrics")
  public TableSnapshotMetricsPO getMetrics(long tableId, long policyId, long snapshotId) {
    return SessionUtils.getWithoutCommit(
        TableSnapshotMetricsMapper.class,
        mapper -> mapper.selectMetrics(tableId, policyId, snapshotId));
  }

  private static boolean isUniqueViolation(Throwable throwable) {
    for (Throwable t = throwable; t != null; t = t.getCause()) {
      if (t instanceof SQLIntegrityConstraintViolationException) {
        return true;
      }
      String message = t.getMessage();
      if (message != null) {
        String lower = message.toLowerCase(Locale.ROOT);
        if (lower.contains("unique") || lower.contains("duplicate")) {
          return true;
        }
      }
    }
    return false;
  }
}
