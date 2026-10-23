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
import javax.annotation.Nullable;
import org.apache.gravitino.storage.relational.po.ColumnPO;
import org.apache.gravitino.storage.relational.po.TablePO;
import org.apache.ibatis.annotations.DeleteProvider;
import org.apache.ibatis.annotations.InsertProvider;
import org.apache.ibatis.annotations.Many;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Result;
import org.apache.ibatis.annotations.ResultMap;
import org.apache.ibatis.annotations.Results;
import org.apache.ibatis.annotations.Select;
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

  /**
   * Defines the nested result mapping used by the table aggregate reads.
   *
   * @return unused; this mapping declaration is never executed
   */
  @Results(
      id = "tableColumnSnapshot",
      value = {
        @Result(property = "columnId", column = "columnId", id = true),
        @Result(property = "columnName", column = "columnName"),
        @Result(property = "columnPosition", column = "columnPosition"),
        @Result(property = "metalakeId", column = "metalakeId"),
        @Result(property = "catalogId", column = "catalogId"),
        @Result(property = "schemaId", column = "schemaId"),
        @Result(property = "tableId", column = "tableId"),
        @Result(property = "tableVersion", column = "tableVersion", id = true),
        @Result(property = "columnType", column = "columnType"),
        @Result(property = "columnComment", column = "columnComment"),
        @Result(property = "nullable", column = "nullable"),
        @Result(property = "autoIncrement", column = "autoIncrement"),
        @Result(property = "defaultValue", column = "defaultValue"),
        @Result(property = "columnOpType", column = "columnOpType"),
        @Result(property = "deletedAt", column = "deletedAt"),
        @Result(property = "auditInfo", column = "auditInfo")
      })
  @Select("SELECT 1")
  ColumnPO tableColumnSnapshot();

  /**
   * Reads table metadata, its current version, and columns from one database statement snapshot.
   *
   * @param schemaId the parent schema ID
   * @param tableName the table name
   * @return the aggregate snapshot, or null when the table does not exist
   */
  @Results(
      id = "tableWithColumns",
      value = {
        @Result(property = "tableId", column = "tableId", id = true),
        @Result(property = "tableName", column = "tableName"),
        @Result(property = "metalakeId", column = "metalakeId"),
        @Result(property = "catalogId", column = "catalogId"),
        @Result(property = "schemaId", column = "schemaId"),
        @Result(property = "auditInfo", column = "auditInfo"),
        @Result(property = "currentVersion", column = "currentVersion"),
        @Result(property = "lastVersion", column = "lastVersion"),
        @Result(property = "deletedAt", column = "deletedAt"),
        @Result(property = "format", column = "format"),
        @Result(property = "properties", column = "properties"),
        @Result(property = "partitions", column = "partitions"),
        @Result(property = "sortOrders", column = "sortOrders"),
        @Result(property = "distribution", column = "distribution"),
        @Result(property = "indexes", column = "indexes"),
        @Result(property = "comment", column = "comment"),
        @Result(
            property = "columnPOs",
            javaType = List.class,
            many = @Many(resultMap = "tableColumnSnapshot", columnPrefix = "col_"))
      })
  @SelectProvider(
      type = TableMetaSQLProviderFactory.class,
      method = "selectTableWithColumnsBySchemaIdAndName")
  @Nullable
  TablePO selectTableWithColumnsBySchemaIdAndName(
      @Param("schemaId") Long schemaId, @Param("tableName") String tableName);

  /**
   * Reads a table aggregate by its full name, retaining missing-parent placeholders.
   *
   * @param metalakeName the metalake name
   * @param catalogName the catalog name
   * @param schemaName the physical schema name
   * @param tableName the table name
   * @return the aggregate or parent placeholder, or null when the metalake or catalog is absent
   */
  @ResultMap("tableWithColumns")
  @SelectProvider(
      type = TableMetaSQLProviderFactory.class,
      method = "selectTableWithColumnsByFullQualifiedName")
  @Nullable
  TablePO selectTableWithColumnsByFullQualifiedName(
      @Param("metalakeName") String metalakeName,
      @Param("catalogName") String catalogName,
      @Param("schemaName") String schemaName,
      @Param("tableName") String tableName);

  @SelectProvider(type = TableMetaSQLProviderFactory.class, method = "listTablePOsBySchemaId")
  List<TablePO> listTablePOsBySchemaId(@Param("schemaId") Long schemaId);

  @SelectProvider(
      type = TableMetaSQLProviderFactory.class,
      method = "listTablePOsByFullQualifiedName")
  List<TablePO> listTablePOsByFullQualifiedName(
      @Param("metalakeName") String metalakeName,
      @Param("catalogName") String catalogName,
      @Param("schemaName") String schemaName);

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

  @SelectProvider(
      type = TableMetaSQLProviderFactory.class,
      method = "selectTableByFullQualifiedName")
  TablePO selectTableByFullQualifiedName(
      @Param("metalakeName") String metalakeName,
      @Param("catalogName") String catalogName,
      @Param("schemaName") String schemaName,
      @Param("tableName") String tableName);

  /**
   * Selects and exclusively locks an active table metadata row.
   *
   * @param tableId the table ID
   * @return the active table metadata, or {@code null} when it no longer exists
   */
  @SelectProvider(type = TableMetaSQLProviderFactory.class, method = "selectTableMetaByIdForUpdate")
  TablePO selectTableMetaByIdForUpdate(@Param("tableId") Long tableId);

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

  /**
   * Soft-deletes a table only if its version has not changed since the caller read it.
   *
   * @param tableId the table ID
   * @param currentVersion the version observed by the caller
   * @return the number of deleted rows; zero means the table changed or disappeared
   */
  @UpdateProvider(
      type = TableMetaSQLProviderFactory.class,
      method = "softDeleteTableMetasByTableId")
  Integer softDeleteTableMetasByTableId(
      @Param("tableId") Long tableId, @Param("currentVersion") Long currentVersion);

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
      method = "softDeleteTableMetasBySchemaIds")
  Integer softDeleteTableMetasBySchemaIds(@Param("schemaIds") List<Long> schemaIds);

  @DeleteProvider(
      type = TableMetaSQLProviderFactory.class,
      method = "deleteTableMetasByLegacyTimeline")
  Integer deleteTableMetasByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit);

  @SelectProvider(type = TableMetaSQLProviderFactory.class, method = "batchSelectTableByIdentifier")
  List<TablePO> batchSelectTableByIdentifier(
      @Param("schemaId") Long schemaId, @Param("tableNames") List<String> tableNames);
}
