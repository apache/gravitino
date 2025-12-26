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
import org.apache.gravitino.storage.relational.po.TablePO;
import org.apache.ibatis.annotations.DeleteProvider;
import org.apache.ibatis.annotations.InsertProvider;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.SelectProvider;
import org.apache.ibatis.annotations.UpdateProvider;

/**
 * A MyBatis Mapper for table meta operation SQLs.
 *
 * <p>This interface class is a specification defined by MyBatis. It requires this interface class
 * to identify the corresponding SQLs for execution. We can write SQLs in an additional XML file, or
 * write SQLs with annotations in this interface Mapper. See: <a
 * href="https://mybatis.org/mybatis-3/getting-started.html"></a>
 */
public interface TableMetaMapper {
  String TABLE_NAME = "table_meta";

  @SelectProvider(type = TableMetaSQLProviderFactory.class, method = "listTablePOsBySchemaId")
  List<TablePO> listTablePOsBySchemaId(@Param("schemaId") Long schemaId);

  @SelectProvider(type = TableMetaSQLProviderFactory.class, method = "listTablePOsByTableIds")
  List<TablePO> listTablePOsByTableIds(@Param("tableIds") List<Long> tableIds);

  @SelectProvider(
      type = TableMetaSQLProviderFactory.class,
      method = "selectTableIdBySchemaIdAndName")
  Long selectTableIdBySchemaIdAndName(
      @Param("schemaId") Long schemaId, @Param("tableName") String name);

  @SelectProvider(
      type = TableMetaSQLProviderFactory.class,
      method = "selectTableMetaBySchemaIdAndName")
  TablePO selectTableMetaBySchemaIdAndName(
      @Param("schemaId") Long schemaId, @Param("tableName") String name);

  @InsertProvider(type = TableMetaSQLProviderFactory.class, method = "insertTableMeta")
  void insertTableMeta(@Param("tableMeta") TablePO tablePO);

  @InsertProvider(
      type = TableMetaSQLProviderFactory.class,
      method = "insertTableMetaOnDuplicateKeyUpdate")
  void insertTableMetaOnDuplicateKeyUpdate(@Param("tableMeta") TablePO tablePO);

  @UpdateProvider(type = TableMetaSQLProviderFactory.class, method = "updateTableMeta")
  Integer updateTableMeta(
      @Param("newTableMeta") TablePO newTablePO,
      @Param("oldTableMeta") TablePO oldTablePO,
      @Param("newSchemaId") Long newSchemaId);

  @UpdateProvider(
      type = TableMetaSQLProviderFactory.class,
      method = "softDeleteTableMetasByTableId")
  Integer softDeleteTableMetasByTableId(@Param("tableId") Long tableId);

  @UpdateProvider(
      type = TableMetaSQLProviderFactory.class,
      method = "softDeleteTableMetasByMetalakeId")
  Integer softDeleteTableMetasByMetalakeId(@Param("metalakeId") Long metalakeId);

  @UpdateProvider(
      type = TableMetaSQLProviderFactory.class,
      method = "softDeleteTableMetasByCatalogId")
  Integer softDeleteTableMetasByCatalogId(@Param("catalogId") Long catalogId);

  @UpdateProvider(
      type = TableMetaSQLProviderFactory.class,
      method = "softDeleteTableMetasBySchemaId")
  Integer softDeleteTableMetasBySchemaId(@Param("schemaId") Long schemaId);

  @DeleteProvider(
      type = TableMetaSQLProviderFactory.class,
      method = "deleteTableMetasByLegacyTimeline")
  Integer deleteTableMetasByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit);
}
