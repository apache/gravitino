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

import java.util.List;
import org.apache.gravitino.storage.relational.po.StatisticPO;
import org.apache.ibatis.annotations.DeleteProvider;
import org.apache.ibatis.annotations.InsertProvider;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.SelectProvider;
import org.apache.ibatis.annotations.UpdateProvider;

public interface StatisticMetaMapper {

  String STATISTIC_META_TABLE_NAME = "statistic_meta";

  @SelectProvider(type = StatisticSQLProviderFactory.class, method = "listStatisticPOsByEntityId")
  List<StatisticPO> listStatisticPOsByEntityId(
      @Param("metalakeId") Long metalakeId, @Param("entityId") long entityId);

  /** Inserts a statistic only when no live row has the same name and target. */
  @InsertProvider(type = StatisticSQLProviderFactory.class, method = "insertStatisticPO")
  Integer insertStatisticPO(@Param("statisticPO") StatisticPO statisticPO);

  /** Replaces a statistic value only if its observed version is still current. */
  @UpdateProvider(type = StatisticSQLProviderFactory.class, method = "updateStatisticPOWithVersion")
  Integer updateStatisticPOWithVersion(
      @Param("statisticPO") StatisticPO statisticPO, @Param("previous") StatisticPO previous);

  /** Soft-deletes a statistic only if its observed version is still current. */
  @UpdateProvider(type = StatisticSQLProviderFactory.class, method = "deleteStatisticPOWithVersion")
  Integer deleteStatisticPOWithVersion(@Param("previous") StatisticPO previous);

  @UpdateProvider(
      type = StatisticSQLProviderFactory.class,
      method = "softDeleteStatisticsByEntityId")
  Integer softDeleteStatisticsByEntityId(@Param("entityId") Long entityId);

  @UpdateProvider(
      type = StatisticSQLProviderFactory.class,
      method = "softDeleteStatisticsByMetalakeId")
  Integer softDeleteStatisticsByMetalakeId(@Param("metalakeId") Long metalakeId);

  @UpdateProvider(
      type = StatisticSQLProviderFactory.class,
      method = "softDeleteStatisticsByCatalogId")
  Integer softDeleteStatisticsByCatalogId(@Param("catalogId") Long catalogId);

  @UpdateProvider(
      type = StatisticSQLProviderFactory.class,
      method = "softDeleteStatisticsBySchemaIds")
  Integer softDeleteStatisticsBySchemaIds(@Param("schemaIds") List<Long> schemaIds);

  @DeleteProvider(
      type = StatisticSQLProviderFactory.class,
      method = "deleteStatisticsByLegacyTimeline")
  Integer deleteStatisticsByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit);
}
