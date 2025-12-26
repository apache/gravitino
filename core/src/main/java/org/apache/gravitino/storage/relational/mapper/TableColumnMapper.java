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
import org.apache.gravitino.storage.relational.po.ColumnPO;
import org.apache.ibatis.annotations.DeleteProvider;
import org.apache.ibatis.annotations.InsertProvider;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.SelectProvider;
import org.apache.ibatis.annotations.UpdateProvider;

public interface TableColumnMapper {

  String COLUMN_TABLE_NAME = "table_column_version_info";

  @SelectProvider(
      type = TableColumnSQLProviderFactory.class,
      method = "listColumnPOsByTableIdAndVersion")
  List<ColumnPO> listColumnPOsByTableIdAndVersion(
      @Param("tableId") Long tableId, @Param("tableVersion") Long tableVersion);

  @InsertProvider(type = TableColumnSQLProviderFactory.class, method = "insertColumnPOs")
  void insertColumnPOs(@Param("columnPOs") List<ColumnPO> columnPOs);

  @UpdateProvider(type = TableColumnSQLProviderFactory.class, method = "updateSchemaIdByTableId")
  void updateSchemaIdByTableId(
      @Param("tableId") Long tableId, @Param("newSchemaId") Long newSchemaId);

  @UpdateProvider(type = TableColumnSQLProviderFactory.class, method = "softDeleteColumnsByTableId")
  Integer softDeleteColumnsByTableId(@Param("tableId") Long tableId);

  @UpdateProvider(
      type = TableColumnSQLProviderFactory.class,
      method = "softDeleteColumnsByMetalakeId")
  Integer softDeleteColumnsByMetalakeId(@Param("metalakeId") Long metalakeId);

  @UpdateProvider(
      type = TableColumnSQLProviderFactory.class,
      method = "softDeleteColumnsByCatalogId")
  Integer softDeleteColumnsByCatalogId(@Param("catalogId") Long catalogId);

  @UpdateProvider(
      type = TableColumnSQLProviderFactory.class,
      method = "softDeleteColumnsBySchemaId")
  Integer softDeleteColumnsBySchemaId(@Param("schemaId") Long schemaId);

  @DeleteProvider(
      type = TableColumnSQLProviderFactory.class,
      method = "deleteColumnPOsByLegacyTimeline")
  Integer deleteColumnPOsByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit);

  @SelectProvider(
      type = TableColumnSQLProviderFactory.class,
      method = "selectColumnIdByTableIdAndName")
  Long selectColumnIdByTableIdAndName(
      @Param("tableId") Long tableId, @Param("columnName") String name);

  @SelectProvider(type = TableColumnSQLProviderFactory.class, method = "selectColumnPOById")
  ColumnPO selectColumnPOById(@Param("columnId") Long columnId);

  @SelectProvider(type = TableColumnSQLProviderFactory.class, method = "listColumnPOsByColumnIds")
  List<ColumnPO> listColumnPOsByColumnIds(@Param("columnIds") List<Long> columnIds);
}
