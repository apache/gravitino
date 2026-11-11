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
package org.apache.gravitino.storage.relational.mapper.provider.base;

import static org.apache.gravitino.storage.relational.mapper.TableMetaMapper.TABLE_NAME;

import com.google.common.base.Preconditions;
import java.util.List;
import org.apache.gravitino.storage.relational.mapper.CatalogMetaMapper;
import org.apache.gravitino.storage.relational.mapper.MetalakeMetaMapper;
import org.apache.gravitino.storage.relational.mapper.SchemaMetaMapper;
import org.apache.gravitino.storage.relational.mapper.TableColumnMapper;
import org.apache.gravitino.storage.relational.mapper.TableVersionMapper;
import org.apache.gravitino.storage.relational.mapper.provider.DatabaseTimeSQL;
import org.apache.gravitino.storage.relational.po.ColumnPO;
import org.apache.gravitino.storage.relational.po.TablePO;
import org.apache.ibatis.annotations.Param;

public class TableMetaBaseSQLProvider {

  public String listTablePOsBySchemaId(@Param("schemaId") Long schemaId) {
    return "SELECT tm.table_id as tableId, tm.table_name as tableName,"
        + " tm.metalake_id as metalakeId, tm.catalog_id as catalogId,"
        + " tm.schema_id as schemaId, tm.audit_info as auditInfo,"
        + " tm.current_version as currentVersion, tm.last_version as lastVersion,"
        + " tm.deleted_at as deletedAt,"
        + " tv.format as format, "
        + " tv.properties as properties,"
        + " tv.partitioning as partitions, tv.sort_orders as sortOrders,"
        + " tv.distribution as distribution, tv.indexes as indexes,"
        + " tv.comment as comment"
        + " FROM "
        + TABLE_NAME
        + " tm LEFT JOIN "
        + TableVersionMapper.TABLE_NAME
        + " tv ON tm.table_id = tv.table_id AND tm.current_version = tv.version"
        + " AND tv.deleted_at = 0"
        + " WHERE tm.schema_id = #{schemaId} AND tm.deleted_at = 0";
  }

  public String listTablePOsByTableIds(List<Long> tableIds) {
    return "<script>"
        + "SELECT tm.table_id as tableId, tm.table_name as tableName,"
        + " tm.metalake_id as metalakeId, tm.catalog_id as catalogId,"
        + " tm.schema_id as schemaId, tm.audit_info as auditInfo,"
        + " tm.current_version as currentVersion, tm.last_version as lastVersion,"
        + " tm.deleted_at as deletedAt,"
        + " tv.format as format, "
        + " tv.properties as properties,"
        + " tv.partitioning as partitions, tv.sort_orders as sortOrders,"
        + " tv.distribution as distribution, tv.indexes as indexes,"
        + " tv.comment as comment"
        + " FROM "
        + TABLE_NAME
        + " tm LEFT JOIN "
        + TableVersionMapper.TABLE_NAME
        + " tv ON tm.table_id = tv.table_id AND tm.current_version = tv.version"
        + " AND tv.deleted_at = 0"
        + " WHERE tm.deleted_at = 0"
        + " AND tm.table_id IN ("
        + "<foreach collection='tableIds' item='tableId' separator=','>"
        + "#{tableId}"
        + "</foreach>"
        + ")"
        + "</script>";
  }

  public String listTablePOsByFullQualifiedName(
      @Param("metalakeName") String metalakeName,
      @Param("catalogName") String catalogName,
      @Param("schemaName") String schemaName) {
    return """
        SELECT
            sm.schema_id AS schemaId,
            cm.catalog_id AS catalogId,
            tm.table_id AS tableId,
            tm.table_name AS tableName,
            tm.metalake_id AS metalakeId,
            tm.audit_info AS auditInfo,
            tm.current_version AS currentVersion,
            tm.last_version AS lastVersion,
            tm.deleted_at AS deletedAt,
            tvi.format AS format,
            tvi.properties AS properties,
            tvi.partitioning AS partitions,
            tvi.sort_orders AS sortOrders,
            tvi.distribution AS distribution,
            tvi.indexes AS indexes,
            tvi.comment AS comment
        FROM
            %s mm
        INNER JOIN
            %s cm ON mm.metalake_id = cm.metalake_id
            AND cm.catalog_name = #{catalogName}
            AND cm.deleted_at = 0
        LEFT JOIN
            %s sm ON cm.catalog_id = sm.catalog_id
            AND sm.schema_name = #{schemaName}
            AND sm.deleted_at = 0
        LEFT JOIN
            %s tm ON sm.schema_id = tm.schema_id
            AND tm.deleted_at = 0
        LEFT JOIN
            %s tvi ON tm.table_id = tvi.table_id
            AND tm.current_version = tvi.version
            AND tvi.deleted_at = 0
        WHERE
            mm.metalake_name = #{metalakeName}
            AND mm.deleted_at = 0;
            """
        .formatted(
            MetalakeMetaMapper.TABLE_NAME,
            CatalogMetaMapper.TABLE_NAME,
            SchemaMetaMapper.TABLE_NAME,
            TABLE_NAME,
            TableVersionMapper.TABLE_NAME);
  }

  public String selectTableIdBySchemaIdAndName(
      @Param("schemaId") Long schemaId, @Param("tableName") String name) {
    return "SELECT table_id as tableId FROM "
        + TABLE_NAME
        + " WHERE schema_id = #{schemaId} AND table_name = #{tableName}"
        + " AND deleted_at = 0";
  }

  public String selectTableMetaBySchemaIdAndName(
      @Param("schemaId") Long schemaId, @Param("tableName") String name) {
    return "SELECT tm.table_id as tableId, tm.table_name as tableName,"
        + " tm.metalake_id as metalakeId, tm.catalog_id as catalogId,"
        + " tm.schema_id as schemaId, tm.audit_info as auditInfo,"
        + " tm.current_version as currentVersion, tm.last_version as lastVersion,"
        + " tm.deleted_at as deletedAt,"
        + " tv.format as format, "
        + " tv.properties as properties,"
        + " tv.partitioning as partitions, tv.sort_orders as sortOrders,"
        + " tv.distribution as distribution, tv.indexes as indexes,"
        + " tv.comment as comment"
        + " FROM "
        + TABLE_NAME
        + " tm LEFT JOIN "
        + TableVersionMapper.TABLE_NAME
        + " tv ON tm.table_id = tv.table_id AND tm.current_version = tv.version"
        + " AND tv.deleted_at = 0"
        + " WHERE tm.schema_id = #{schemaId} AND tm.table_name = #{tableName}"
        + " AND tm.deleted_at = 0";
  }

  public String selectTableMetaById(@Param("tableId") Long tableId) {
    return "SELECT tm.table_id as tableId, tm.table_name as tableName,"
        + " tm.metalake_id as metalakeId, tm.catalog_id as catalogId,"
        + " tm.schema_id as schemaId, tm.audit_info as auditInfo,"
        + " tm.current_version as currentVersion, tm.last_version as lastVersion,"
        + " tm.deleted_at as deletedAt,"
        + " tv.format as format, "
        + " tv.properties as properties,"
        + " tv.partitioning as partitions, tv.sort_orders as sortOrders,"
        + " tv.distribution as distribution, tv.indexes as indexes,"
        + " tv.comment as comment"
        + " FROM "
        + TABLE_NAME
        + " tm LEFT JOIN "
        + TableVersionMapper.TABLE_NAME
        + " tv ON tm.table_id = tv.table_id AND tm.current_version = tv.version"
        + " AND tv.deleted_at = 0"
        + " WHERE tm.table_id = #{tableId} AND tm.deleted_at = 0";
  }

  /**
   * Returns the active table metadata row and holds it exclusively for the current transaction.
   *
   * <p>Unlike the metalake, catalog and schema providers, this cannot be written as {@code
   * selectTableMetaById(id) + " FOR UPDATE"}: that select LEFT JOINs {@code table_version_info},
   * and locking the nullable side of an outer join is rejected by PostgreSQL and locks the wrong
   * rows on MySQL. The projection is therefore spelled out for {@code table_meta} alone, and the
   * returned row carries only the identity and version columns its callers read.
   *
   * @param tableId the table ID
   * @return the locking select SQL
   */
  public String selectTableMetaByIdForUpdate(@Param("tableId") Long tableId) {
    return "SELECT table_id as tableId, table_name as tableName,"
        + " metalake_id as metalakeId, catalog_id as catalogId,"
        + " schema_id as schemaId, audit_info as auditInfo,"
        + " current_version as currentVersion, last_version as lastVersion,"
        + " deleted_at as deletedAt"
        + " FROM "
        + TABLE_NAME
        + " WHERE table_id = #{tableId} AND deleted_at = 0 FOR UPDATE";
  }

  public String insertTableMeta(@Param("tableMeta") TablePO tablePO) {
    return "INSERT INTO "
        + TABLE_NAME
        + " (table_id, table_name, metalake_id,"
        + " catalog_id, schema_id, audit_info,"
        + " current_version, last_version, deleted_at)"
        + " VALUES ("
        + " #{tableMeta.tableId},"
        + " #{tableMeta.tableName},"
        + " #{tableMeta.metalakeId},"
        + " #{tableMeta.catalogId},"
        + " #{tableMeta.schemaId},"
        + " #{tableMeta.auditInfo},"
        + " #{tableMeta.currentVersion},"
        + " #{tableMeta.lastVersion},"
        + " #{tableMeta.deletedAt}"
        + " )";
  }

  public String insertTableMetaOnDuplicateKeyUpdate(@Param("tableMeta") TablePO tablePO) {
    return "INSERT INTO "
        + TABLE_NAME
        + " (table_id, table_name, metalake_id,"
        + " catalog_id, schema_id, audit_info,"
        + " current_version, last_version, deleted_at)"
        + " VALUES ("
        + " #{tableMeta.tableId},"
        + " #{tableMeta.tableName},"
        + " #{tableMeta.metalakeId},"
        + " #{tableMeta.catalogId},"
        + " #{tableMeta.schemaId},"
        + " #{tableMeta.auditInfo},"
        + " #{tableMeta.currentVersion},"
        + " #{tableMeta.lastVersion},"
        + " #{tableMeta.deletedAt}"
        + " )"
        + " ON DUPLICATE KEY UPDATE"
        + " table_name = #{tableMeta.tableName},"
        + " metalake_id = #{tableMeta.metalakeId},"
        + " catalog_id = #{tableMeta.catalogId},"
        + " schema_id = #{tableMeta.schemaId},"
        + " audit_info = #{tableMeta.auditInfo},"
        // Keep the OCC token monotonic on overwrite. Resetting it to the incoming initial version
        // would let a writer that read the same old version pass its CAS after this statement.
        // last_version is assigned first, so both columns advance from the stored current version.
        + " last_version = current_version + 1,"
        + " current_version = current_version + 1,"
        + " deleted_at = #{tableMeta.deletedAt}";
  }

  /**
   * Returns SQL that updates a table only while its OCC version is unchanged.
   *
   * <p>The WHERE clause intentionally compares only the stable ID, the version seen by the caller,
   * and the active flag. Comparing serialized payload or audit fields would make harmless encoding
   * differences look like conflicts; the version is the single source of truth for concurrency.
   *
   * @param newTablePO the table values to write
   * @param oldTablePO the table values and OCC version read by the caller
   * @param newSchemaId the target schema ID
   * @return the version-checked update SQL
   */
  public String updateTableMeta(
      @Param("newTableMeta") TablePO newTablePO,
      @Param("oldTableMeta") TablePO oldTablePO,
      @Param("newSchemaId") Long newSchemaId) {
    return "UPDATE "
        + TABLE_NAME
        + " SET table_name = #{newTableMeta.tableName},"
        + " metalake_id = #{newTableMeta.metalakeId},"
        + " catalog_id = #{newTableMeta.catalogId},"
        + " schema_id = #{newSchemaId},"
        + " audit_info = #{newTableMeta.auditInfo},"
        + " current_version = #{newTableMeta.currentVersion},"
        + " last_version = #{newTableMeta.lastVersion},"
        + " deleted_at = #{newTableMeta.deletedAt}"
        + " WHERE table_id = #{oldTableMeta.tableId}"
        + " AND current_version = #{oldTableMeta.currentVersion}"
        + " AND deleted_at = 0";
  }

  /**
   * Returns SQL that deletes only the table version observed by the caller.
   *
   * <p>For example, a drop that read version 3 must not delete version 4 after a concurrent alter.
   *
   * @param tableId the table ID
   * @param currentVersion the version observed by the caller
   * @return the version-checked delete SQL
   */
  public String softDeleteTableMetasByTableId(
      @Param("tableId") Long tableId, @Param("currentVersion") Long currentVersion) {
    return "UPDATE "
        + TABLE_NAME
        + " SET deleted_at = "
        + DatabaseTimeSQL.MYSQL
        + " WHERE table_id = #{tableId}"
        + " AND current_version = #{currentVersion} AND deleted_at = 0";
  }

  public String softDeleteTableMetasByMetalakeId(@Param("metalakeId") Long metalakeId) {
    return "UPDATE "
        + TABLE_NAME
        + " SET deleted_at = "
        + DatabaseTimeSQL.MYSQL
        + " WHERE metalake_id = #{metalakeId} AND deleted_at = 0";
  }

  public String softDeleteTableMetasByCatalogId(@Param("catalogId") Long catalogId) {
    return "UPDATE "
        + TABLE_NAME
        + " SET deleted_at = "
        + DatabaseTimeSQL.MYSQL
        + " WHERE catalog_id = #{catalogId} AND deleted_at = 0";
  }

  public String softDeleteTableMetasBySchemaIds(@Param("schemaIds") List<Long> schemaIds) {
    return "<script>"
        + "UPDATE "
        + TABLE_NAME
        + " SET deleted_at = "
        + DatabaseTimeSQL.MYSQL
        + " WHERE schema_id IN ("
        + "<foreach collection='schemaIds' item='schemaId' separator=','>"
        + "#{schemaId}"
        + "</foreach>"
        + ") AND deleted_at = 0"
        + "</script>";
  }

  public String deleteTableMetasByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit) {
    return "DELETE FROM "
        + TABLE_NAME
        + " WHERE deleted_at > 0 AND deleted_at < #{legacyTimeline} LIMIT #{limit}";
  }

  public String selectTableByFullQualifiedName(
      @Param("metalakeName") String metalakeName,
      @Param("catalogName") String catalogName,
      @Param("schemaName") String schemaName,
      @Param("tableName") String tableName) {
    return """
        SELECT
            sm.schema_id AS schemaId,
            cm.catalog_id AS catalogId,
            tm.table_id AS tableId,
            tm.table_name AS tableName,
            tm.metalake_id AS metalakeId,
            tm.audit_info AS auditInfo,
            tm.current_version AS currentVersion,
            tm.last_version AS lastVersion,
            tm.deleted_at AS deletedAt,
            tvi.format AS format,
            tvi.properties AS properties,
            tvi.partitioning AS partitions,
            tvi.sort_orders AS sortOrders,
            tvi.distribution AS distribution,
            tvi.indexes AS indexes,
            tvi.comment AS comment
        FROM
            %s mm
        INNER JOIN
            %s cm ON mm.metalake_id = cm.metalake_id
            AND cm.catalog_name = #{catalogName}
            AND cm.deleted_at = 0
        LEFT JOIN
            %s sm ON cm.catalog_id = sm.catalog_id
            AND sm.schema_name = #{schemaName}
            AND sm.deleted_at = 0
        LEFT JOIN
            %s tm ON sm.schema_id = tm.schema_id
            AND tm.table_name = #{tableName}
            AND tm.deleted_at = 0
        LEFT JOIN
            %s tvi ON tm.table_id = tvi.table_id
            AND tm.current_version = tvi.version
            AND tvi.deleted_at = 0
        WHERE
            mm.metalake_name = #{metalakeName}
            AND mm.deleted_at = 0;
            """
        .formatted(
            MetalakeMetaMapper.TABLE_NAME,
            CatalogMetaMapper.TABLE_NAME,
            SchemaMetaMapper.TABLE_NAME,
            TABLE_NAME,
            TableVersionMapper.TABLE_NAME);
  }

  public String batchSelectTableByIdentifier(
      @Param("schemaId") Long schemaId, @Param("tableNames") List<String> tableNames) {
    return """
            <script>
            SELECT
            tm.table_id AS tableId,
            tm.table_name AS tableName,
            tm.metalake_id AS metalakeId,
            tm.catalog_id AS catalogId,
            tm.schema_id AS schemaId,
            tm.audit_info AS auditInfo,
            tm.current_version AS currentVersion,
            tm.last_version AS lastVersion,
            tm.deleted_at AS deletedAt,
            tv.format AS format,
            tv.properties AS properties,
            tv.partitioning AS partitions,
            tv.sort_orders AS sortOrders,
            tv.distribution AS distribution,
            tv.indexes AS indexes,
            tv.comment AS comment
            FROM %s tm LEFT JOIN %s tv
            ON tm.table_id = tv.table_id AND tm.current_version = tv.version
            AND tv.deleted_at = 0
            WHERE tm.schema_id = #{schemaId}
            AND tm.table_name IN
            <foreach collection="tableNames" item="tableName" open="(" separator="," close=")">
            #{tableName}
            </foreach>
             AND tm.deleted_at = 0
             </script>
            """
        .formatted(TABLE_NAME, TableVersionMapper.TABLE_NAME);
  }
  /**
   * Builds a single-statement table and column snapshot by schema ID and name.
   *
   * @param schemaId the schema ID
   * @param tableName the table name
   * @return the aggregate select SQL
   */
  public String selectTableWithColumnsBySchemaIdAndName(
      @Param("schemaId") Long schemaId, @Param("tableName") String tableName) {
    return withColumns(selectTableMetaBySchemaIdAndName(schemaId, tableName));
  }

  /**
   * Builds a single-statement table and column snapshot by full name.
   *
   * @param metalakeName the metalake name
   * @param catalogName the catalog name
   * @param schemaName the physical schema name
   * @param tableName the table name
   * @return the aggregate select SQL
   */
  public String selectTableWithColumnsByFullQualifiedName(
      @Param("metalakeName") String metalakeName,
      @Param("catalogName") String catalogName,
      @Param("schemaName") String schemaName,
      @Param("tableName") String tableName) {
    return withColumns(
        selectTableByFullQualifiedName(metalakeName, catalogName, schemaName, tableName));
  }

  private String withColumns(String tableSql) {
    // One statement gives the root, version and columns a common snapshot. Overwrite retires old
    // columns, so a second SELECT cannot reconstruct them. Aggregate history once and join by the
    // existing (table_id, table_version, column_id, deleted_at) unique key. Correlated scans for
    // each column would be quadratic for long histories. Root subqueries have independent bound
    // parameters on all supported backends. Exclude DELETE markers only after resolving history,
    // otherwise a dropped column would reappear through its earlier CREATE/UPDATE row.
    // Derived tables accept static SELECTs only. MyBatis handles <script> only at the start of
    // the complete statement, so a dynamic provider cannot be embedded here.
    Preconditions.checkArgument(
        !tableSql.stripLeading().startsWith("<script>"),
        "Aggregate table reads require static root SQL, not a MyBatis <script>");
    String rootSql = tableSql.strip().replaceFirst(";\\s*$", "");
    return "SELECT t.*, "
        + "c.column_id AS col_columnId, "
        + "c.column_name AS col_columnName, "
        + "c.column_position AS col_columnPosition, "
        + "c.metalake_id AS col_metalakeId, "
        + "c.catalog_id AS col_catalogId, "
        + "c.schema_id AS col_schemaId, "
        + "c.table_id AS col_tableId, "
        + "c.table_version AS col_tableVersion, "
        + "c.column_type AS col_columnType, "
        + "c.column_comment AS col_columnComment, "
        + "c.column_nullable AS col_nullable, "
        + "c.column_auto_increment AS col_autoIncrement, "
        + "c.column_default_value AS col_defaultValue, "
        + "c.column_op_type AS col_columnOpType, "
        + "c.deleted_at AS col_deletedAt, "
        + "c.audit_info AS col_auditInfo "
        + "FROM ("
        + rootSql
        + ") t LEFT JOIN ("
        + " SELECT history.table_id, history.column_id, MAX(history.table_version) AS max_version"
        + " FROM "
        + TableColumnMapper.COLUMN_TABLE_NAME
        + " history JOIN ("
        + rootSql
        + ") observed"
        + " ON history.table_id = observed.tableId AND history.table_version <= observed.currentVersion"
        + " WHERE history.deleted_at = 0 GROUP BY history.table_id, history.column_id"
        + ") latest ON latest.table_id = t.tableId LEFT JOIN "
        + TableColumnMapper.COLUMN_TABLE_NAME
        + " c"
        + " ON c.table_id = latest.table_id AND c.column_id = latest.column_id"
        + " AND c.table_version = latest.max_version AND c.deleted_at = 0"
        + " AND c.column_op_type != "
        + ColumnPO.ColumnOpType.DELETE.value();
  }
}
