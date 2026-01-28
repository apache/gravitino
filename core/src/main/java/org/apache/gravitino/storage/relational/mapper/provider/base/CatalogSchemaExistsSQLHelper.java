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

import java.util.ArrayList;
import java.util.List;
import org.apache.gravitino.storage.relational.mapper.CatalogMetaMapper;
import org.apache.gravitino.storage.relational.mapper.FilesetMetaMapper;
import org.apache.gravitino.storage.relational.mapper.ModelMetaMapper;
import org.apache.gravitino.storage.relational.mapper.SchemaMetaMapper;
import org.apache.gravitino.storage.relational.mapper.TableMetaMapper;
import org.apache.gravitino.storage.relational.mapper.TopicMetaMapper;
import org.apache.gravitino.storage.relational.mapper.ViewMetaMapper;

/** Helper class to generate EXISTS SQL for catalog/schema related soft delete operations. */
public class CatalogSchemaExistsSQLHelper {

  private CatalogSchemaExistsSQLHelper() {}

  /**
   * Generates EXISTS SQL clause for catalog or schema related entities.
   *
   * @param tableAlias the alias of the main table (e.g., "ot", "sect")
   * @param objectIdColumn the column name for metadata object id (e.g., "metadata_object_id")
   * @param objectTypeColumn the column name for metadata object type (e.g., "metadata_object_type",
   *     "type")
   * @param columnName the column to filter on (e.g., "catalog_id", "schema_id")
   * @param paramName the parameter name for MyBatis (e.g., "catalogId", "schemaId")
   * @param includeCatalog whether to include catalog entity in the SQL
   * @return the EXISTS SQL clause
   */
  public static String generateExistsSQL(
      String tableAlias,
      String objectIdColumn,
      String objectTypeColumn,
      String columnName,
      String paramName,
      boolean includeCatalog) {
    List<EntityConfig> entities = new ArrayList<>();
    if (includeCatalog) {
      entities.add(new EntityConfig(CatalogMetaMapper.TABLE_NAME, "ct", "catalog_id", "CATALOG"));
    }
    entities.add(new EntityConfig(SchemaMetaMapper.TABLE_NAME, "st", "schema_id", "SCHEMA"));
    entities.add(new EntityConfig(TopicMetaMapper.TABLE_NAME, "tt", "topic_id", "TOPIC"));
    entities.add(new EntityConfig(TableMetaMapper.TABLE_NAME, "tat", "table_id", "TABLE"));
    entities.add(
        new EntityConfig(FilesetMetaMapper.META_TABLE_NAME, "ft", "fileset_id", "FILESET"));
    entities.add(new EntityConfig(ModelMetaMapper.TABLE_NAME, "mt", "model_id", "MODEL"));
    entities.add(new EntityConfig(ViewMetaMapper.TABLE_NAME, "vt", "view_id", "VIEW"));

    StringBuilder sql = new StringBuilder();
    for (int i = 0; i < entities.size(); i++) {
      if (i > 0) {
        sql.append(" UNION");
      }
      EntityConfig entity = entities.get(i);
      sql.append(
          existsClause(
              entity, tableAlias, objectIdColumn, objectTypeColumn, columnName, paramName));
    }
    return sql.toString();
  }

  private static String existsClause(
      EntityConfig entity,
      String tableAlias,
      String objectIdColumn,
      String objectTypeColumn,
      String columnName,
      String paramName) {
    return String.format(
        " SELECT %s.%s FROM %s %s"
            + " WHERE %s.%s = #{%s}"
            + " AND %s.%s = %s.%s"
            + " AND %s.%s = '%s'",
        entity.alias,
        columnName,
        entity.tableName,
        entity.alias,
        entity.alias,
        columnName,
        paramName,
        entity.alias,
        entity.objectIdColumn,
        tableAlias,
        objectIdColumn,
        tableAlias,
        objectTypeColumn,
        entity.objectType);
  }

  private static class EntityConfig {
    final String tableName;
    final String alias;
    final String objectIdColumn;
    final String objectType;

    EntityConfig(String tableName, String alias, String objectIdColumn, String objectType) {
      this.tableName = tableName;
      this.alias = alias;
      this.objectIdColumn = objectIdColumn;
      this.objectType = objectType;
    }
  }
}
