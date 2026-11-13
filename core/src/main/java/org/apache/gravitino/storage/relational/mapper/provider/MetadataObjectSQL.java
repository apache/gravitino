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
package org.apache.gravitino.storage.relational.mapper.provider;

/**
 * SQL predicates that keep historical entity rows from deleting another registration's metadata.
 */
public final class MetadataObjectSQL {
  private MetadataObjectSQL() {}

  /**
   * Excludes identities that currently belong to another catalog.
   *
   * @param tableName the entity table name
   * @param idColumn the stable identity column
   * @param alias the historical entity row alias
   * @return a predicate bound to the catalogId MyBatis parameter
   */
  public static String excludeOtherActiveCatalogs(String tableName, String idColumn, String alias) {
    return " AND NOT EXISTS (SELECT 1 FROM "
        + tableName
        + " live WHERE live."
        + idColumn
        + " = "
        + alias
        + "."
        + idColumn
        + " AND live.deleted_at = 0 AND live.catalog_id != #{catalogId})";
  }

  /**
   * Excludes identities that currently belong to another metalake.
   *
   * @param tableName the entity table name
   * @param idColumn the stable identity column
   * @param alias the historical entity row alias
   * @return a predicate bound to the metalakeId MyBatis parameter
   */
  public static String excludeOtherActiveMetalakes(
      String tableName, String idColumn, String alias) {
    return " AND NOT EXISTS (SELECT 1 FROM "
        + tableName
        + " live WHERE live."
        + idColumn
        + " = "
        + alias
        + "."
        + idColumn
        + " AND live.deleted_at = 0 AND live.metalake_id != #{metalakeId})";
  }

  /**
   * Excludes identities that currently belong to a schema outside the deletion scope.
   *
   * @param tableName the entity table name
   * @param idColumn the stable identity column
   * @param alias the historical entity row alias
   * @return a predicate bound to the schemaIds MyBatis collection
   */
  public static String excludeOtherActiveSchemas(String tableName, String idColumn, String alias) {
    return " AND NOT EXISTS (SELECT 1 FROM "
        + tableName
        + " live WHERE live."
        + idColumn
        + " = "
        + alias
        + "."
        + idColumn
        + " AND live.deleted_at = 0 AND live.schema_id NOT IN "
        + "<foreach collection='schemaIds' item='schemaId' open='(' close=')' separator=','>"
        + "#{schemaId}"
        + "</foreach>)";
  }
}
