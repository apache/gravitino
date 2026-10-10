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
package org.apache.gravitino.storage.relational.mapper;

import javax.annotation.Nullable;
import org.apache.gravitino.storage.relational.po.TableSnapshotMetricsPO;
import org.apache.ibatis.annotations.InsertProvider;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.SelectProvider;
import org.apache.ibatis.annotations.UpdateProvider;

/** MyBatis mapper for {@code table_snapshot_metrics}. */
public interface TableSnapshotMetricsMapper {

  String TABLE_NAME = "table_snapshot_metrics";

  @InsertProvider(type = TableSnapshotMetricsSQLProviderFactory.class, method = "insertMetrics")
  void insertMetrics(@Param("po") TableSnapshotMetricsPO po);

  @UpdateProvider(type = TableSnapshotMetricsSQLProviderFactory.class, method = "updateMetrics")
  int updateMetrics(@Param("po") TableSnapshotMetricsPO po);

  @Nullable
  @SelectProvider(type = TableSnapshotMetricsSQLProviderFactory.class, method = "selectMetrics")
  TableSnapshotMetricsPO selectMetrics(
      @Param("tableId") long tableId,
      @Param("policyId") long policyId,
      @Param("snapshotId") long snapshotId);
}
