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
package org.apache.gravitino.maintenance.storage.mapper;

import org.apache.gravitino.maintenance.storage.mapper.provider.base.TableSnapshotMetricsBaseSQLProvider;
import org.apache.gravitino.maintenance.storage.po.TableSnapshotMetricsPO;
import org.apache.ibatis.annotations.Param;

/** SQL provider factory for {@link TableSnapshotMetricsMapper}. */
public class TableSnapshotMetricsSQLProviderFactory {

  private static final TableSnapshotMetricsBaseSQLProvider PROVIDER =
      new TableSnapshotMetricsBaseSQLProvider();

  private TableSnapshotMetricsSQLProviderFactory() {}

  /**
   * @param po metrics row
   * @return INSERT statement
   */
  public static String insertMetrics(@Param("po") TableSnapshotMetricsPO po) {
    return PROVIDER.insertMetrics(po);
  }

  /**
   * @param po metrics row
   * @return UPDATE statement
   */
  public static String updateMetrics(@Param("po") TableSnapshotMetricsPO po) {
    return PROVIDER.updateMetrics(po);
  }

  /**
   * @param tableId table id
   * @param policyId policy id
   * @param snapshotId snapshot id
   * @return SELECT statement
   */
  public static String selectMetrics(
      @Param("tableId") long tableId,
      @Param("policyId") long policyId,
      @Param("snapshotId") long snapshotId) {
    return PROVIDER.selectMetrics(tableId, policyId, snapshotId);
  }
}
