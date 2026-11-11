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

  /** Lists only the named live statistics for a metadata object. */
  @SelectProvider(type = StatisticSQLProviderFactory.class, method = "listStatisticPOsByNames")
  List<StatisticPO> listStatisticPOsByNames(
      @Param("metalakeId") Long metalakeId,
      @Param("entityId") long entityId,
      @Param("names") List<String> names);

  /**
   * Inserts statistics in one statement. The whole statement fails with a unique-key violation if
   * any of them already has a live row with the same name and target.
   *
   * @param statisticPOs the statistics to insert, with their initial versions and deleted_at
   * @return the number of inserted rows
   */
  @InsertProvider(type = StatisticSQLProviderFactory.class, method = "batchInsertStatisticPOs")
  Integer batchInsertStatisticPOs(@Param("statisticPOs") List<StatisticPO> statisticPOs);

  /**
   * Replaces statistic values in one statement, each only if its observed version is still current.
   *
   * @param statisticPOs one PO per observed row: its ID, target, name and current version identify
   *     the row, and its value and audit info are the replacement
   * @return the number of replaced rows; fewer than requested means some rows changed meanwhile
   */
  @UpdateProvider(
      type = StatisticSQLProviderFactory.class,
      method = "batchUpdateStatisticPOsWithVersion")
  Integer batchUpdateStatisticPOsWithVersion(@Param("statisticPOs") List<StatisticPO> statisticPOs);

  /**
   * Soft-deletes statistics in one statement, each only if its observed version is still current.
   *
   * @param statisticPOs the observed rows, identified by ID, target, name and current version
   * @return the number of deleted rows; fewer than requested means some rows changed meanwhile
   */
  @UpdateProvider(
      type = StatisticSQLProviderFactory.class,
      method = "batchDeleteStatisticPOsWithVersion")
  Integer batchDeleteStatisticPOsWithVersion(@Param("statisticPOs") List<StatisticPO> statisticPOs);

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
