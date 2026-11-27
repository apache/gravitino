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

import java.util.Locale;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.storage.relational.po.ColumnPO;
import org.apache.ibatis.annotations.Param;

/** Resolves a bounded set of typed Article bindings to their current component names and IDs. */
public final class ArticleAssetSQLProvider {
  private ArticleAssetSQLProvider() {}

  /** Builds the live hierarchy join for one supported asset type. */
  public static String liveAssets(@Param("type") MetadataObject.Type type) {
    String catalog = " FROM catalog_meta c";
    String schema = " JOIN schema_meta s ON s.catalog_id=c.catalog_id AND s.deleted_at=0";
    String table = " JOIN table_meta t ON t.schema_id=s.schema_id AND t.deleted_at=0";
    String joins = catalog;
    String parents =
        "NULL AS catalogId, NULL AS catalogName, NULL AS schemaId, NULL AS schemaName, NULL AS tableId, NULL AS tableName";
    String condition = "";
    String target;
    switch (type) {
      case CATALOG -> target = "c.catalog";
      case SCHEMA -> {
        joins += schema;
        target = "s.schema";
        parents =
            "c.catalog_id AS catalogId, c.catalog_name AS catalogName, NULL AS schemaId, NULL AS schemaName, NULL AS tableId, NULL AS tableName";
      }
      case TABLE, VIEW, FILESET, TOPIC, MODEL, SEMANTIC_MODEL -> {
        String leaf = type.name().toLowerCase(Locale.ROOT);
        joins += schema + " JOIN " + leaf + "_meta a ON a.schema_id=s.schema_id AND a.deleted_at=0";
        target = "a." + leaf;
        parents =
            "c.catalog_id AS catalogId, c.catalog_name AS catalogName, s.schema_id AS schemaId, s.schema_name AS schemaName, NULL AS tableId, NULL AS tableName";
      }
      case COLUMN -> {
        joins +=
            schema
                + table
                + " JOIN table_column_version_info a ON a.table_id=t.table_id AND a.deleted_at=0";
        target = "a.column";
        parents =
            "c.catalog_id AS catalogId, c.catalog_name AS catalogName, s.schema_id AS schemaId, s.schema_name AS schemaName, t.table_id AS tableId, t.table_name AS tableName";
        // A rename or delete appends a version. Only the latest row for this ID can be live.
        condition =
            " AND a.column_op_type != "
                + ColumnPO.ColumnOpType.DELETE.value()
                + " AND NOT EXISTS (SELECT 1 FROM table_column_version_info newer WHERE newer.table_id=a.table_id"
                + " AND newer.column_id=a.column_id AND newer.deleted_at=0 AND newer.table_version &gt; a.table_version)";
      }
      default -> throw new IllegalArgumentException("Unsupported Article asset type: " + type);
    }
    return "<script>SELECT "
        + target
        + "_id AS id, "
        + target
        + "_name AS name, "
        + parents
        + joins
        + " WHERE c.metalake_id=#{lake} AND c.deleted_at=0 AND "
        + target
        + "_id IN "
        + "<foreach item='id' collection='ids' open='(' separator=',' close=')'>#{id}</foreach>"
        + condition
        + "</script>";
  }
}
