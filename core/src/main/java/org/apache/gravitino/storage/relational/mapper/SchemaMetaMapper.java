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
import org.apache.gravitino.storage.relational.helper.SchemaIds;
import org.apache.gravitino.storage.relational.po.SchemaPO;
import org.apache.ibatis.annotations.DeleteProvider;
import org.apache.ibatis.annotations.InsertProvider;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.SelectProvider;
import org.apache.ibatis.annotations.UpdateProvider;

/**
 * A MyBatis Mapper for schema meta operation SQLs.
 *
 * <p>This interface class is a specification defined by MyBatis. It requires this interface class
 * to identify the corresponding SQLs for execution. We can write SQLs in an additional XML file, or
 * write SQLs with annotations in this interface Mapper. See: <a
 * href="https://mybatis.org/mybatis-3/getting-started.html"></a>
 */
public interface SchemaMetaMapper {
  String TABLE_NAME = "schema_meta";

  @SelectProvider(type = SchemaMetaSQLProviderFactory.class, method = "listSchemaPOsByCatalogId")
  List<SchemaPO> listSchemaPOsByCatalogId(@Param("catalogId") Long catalogId);

  /** Lists all active schemas in a metalake. */
  @SelectProvider(type = SchemaMetaSQLProviderFactory.class, method = "listSchemaPOsByMetalakeId")
  List<SchemaPO> listSchemaPOsByMetalakeId(@Param("metalakeId") Long metalakeId);

  @SelectProvider(
      type = SchemaMetaSQLProviderFactory.class,
      method = "listSchemaPOsByFullQualifiedName")
  List<SchemaPO> listSchemaPOsByFullQualifiedName(
      @Param("metalakeName") String metalakeName, @Param("catalogName") String catalogName);

  @SelectProvider(type = SchemaMetaSQLProviderFactory.class, method = "listSchemaPOsBySchemaIds")
  List<SchemaPO> listSchemaPOsBySchemaIds(@Param("schemaIds") List<Long> schemaIds);

  @SelectProvider(
      type = SchemaMetaSQLProviderFactory.class,
      method = "listSchemaPOsByCatalogIdAndNamePrefix")
  List<SchemaPO> listSchemaPOsByCatalogIdAndNamePrefix(
      @Param("catalogId") Long catalogId,
      @Param("schemaName") String schemaName,
      @Param("descendantPrefix") String descendantPrefix);

  @SelectProvider(
      type = SchemaMetaSQLProviderFactory.class,
      method = "selectSchemaIdByCatalogIdAndName")
  Long selectSchemaIdByCatalogIdAndName(
      @Param("catalogId") Long catalogId, @Param("schemaName") String name);

  @SelectProvider(
      type = SchemaMetaSQLProviderFactory.class,
      method = "selectSchemaMetaByCatalogIdAndName")
  SchemaPO selectSchemaMetaByCatalogIdAndName(
      @Param("catalogId") Long catalogId, @Param("schemaName") String name);

  @SelectProvider(
      type = SchemaMetaSQLProviderFactory.class,
      method = "selectSchemaByFullQualifiedName")
  SchemaPO selectSchemaByFullQualifiedName(
      @Param("metalakeName") String metalakeName,
      @Param("catalogName") String catalogName,
      @Param("schemaName") String schemaName);

  @SelectProvider(type = SchemaMetaSQLProviderFactory.class, method = "selectSchemaMetaById")
  SchemaPO selectSchemaMetaById(@Param("schemaId") Long schemaId);

  /**
   * Returns one when an active table, view, fileset, function, model, or topic exists in the
   * schema, and {@code null} otherwise.
   *
   * <p>Only a literal is selected because callers need an existence answer, not complete child
   * metadata. The final limit also lets the database stop as soon as it finds the first child.
   */
  @SelectProvider(type = SchemaMetaSQLProviderFactory.class, method = "selectActiveChildBySchemaId")
  Integer selectActiveChildBySchemaId(@Param("schemaId") Long schemaId);

  /** Selects and locks an active schema by ID for the current transaction. */
  @SelectProvider(
      type = SchemaMetaSQLProviderFactory.class,
      method = "selectSchemaMetaByIdForUpdate")
  SchemaPO selectSchemaMetaByIdForUpdate(@Param("schemaId") Long schemaId);

  /** Selects and share-locks an active schema by ID for the current transaction. */
  @SelectProvider(
      type = SchemaMetaSQLProviderFactory.class,
      method = "selectSchemaMetaByIdForShare")
  SchemaPO selectSchemaMetaByIdForShare(@Param("schemaId") Long schemaId);

  @InsertProvider(type = SchemaMetaSQLProviderFactory.class, method = "insertSchemaMeta")
  void insertSchemaMeta(@Param("schemaMeta") SchemaPO schemaPO);

  @InsertProvider(
      type = SchemaMetaSQLProviderFactory.class,
      method = "insertSchemaMetaOnDuplicateKeyUpdate")
  void insertSchemaMetaOnDuplicateKeyUpdate(@Param("schemaMeta") SchemaPO schemaPO);

  @InsertProvider(type = SchemaMetaSQLProviderFactory.class, method = "batchInsertSchemaMeta")
  void batchInsertSchemaMeta(@Param("schemaMetas") List<SchemaPO> schemaMetas);

  @InsertProvider(
      type = SchemaMetaSQLProviderFactory.class,
      method = "batchInsertSchemaMetaOnDuplicateKeyUpdate")
  void batchInsertSchemaMetaOnDuplicateKeyUpdate(@Param("schemaMetas") List<SchemaPO> schemaMetas);

  @UpdateProvider(type = SchemaMetaSQLProviderFactory.class, method = "updateSchemaMeta")
  Integer updateSchemaMeta(
      @Param("newSchemaMeta") SchemaPO newSchemaPO, @Param("oldSchemaMeta") SchemaPO oldSchemaPO);

  @UpdateProvider(
      type = SchemaMetaSQLProviderFactory.class,
      method = "softDeleteSchemaMetasBySchemaIds")
  Integer softDeleteSchemaMetasBySchemaIds(@Param("schemaIds") List<Long> schemaIds);

  /**
   * Soft-deletes a schema, but only while it still carries the given version.
   *
   * @param schemaId the ID of the schema to delete
   * @param currentVersion the version the caller read before deciding to delete
   * @return 1 when the schema was deleted, 0 when it changed or is already gone
   */
  @UpdateProvider(
      type = SchemaMetaSQLProviderFactory.class,
      method = "softDeleteSchemaMetaBySchemaIdAndVersion")
  Integer softDeleteSchemaMetaBySchemaIdAndVersion(
      @Param("schemaId") Long schemaId, @Param("currentVersion") Long currentVersion);

  /**
   * Soft-deletes schemas whose identifiers and OCC versions still match.
   *
   * @return the number of deleted rows
   */
  @UpdateProvider(
      type = SchemaMetaSQLProviderFactory.class,
      method = "softDeleteSchemaMetasWithVersion")
  Integer softDeleteSchemaMetasWithVersion(@Param("schemaMetas") List<SchemaPO> schemaPOs);

  @DeleteProvider(
      type = SchemaMetaSQLProviderFactory.class,
      method = "deleteSchemaMetasByLegacyTimeline")
  Integer deleteSchemaMetasByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit);

  @SelectProvider(
      type = SchemaMetaSQLProviderFactory.class,
      method = "selectSchemaIdByMetalakeNameAndCatalogNameAndSchemaName")
  SchemaIds selectSchemaIdByMetalakeNameAndCatalogNameAndSchemaName(
      @Param("metalakeName") String metalakeName,
      @Param("catalogName") String catalogName,
      @Param("schemaName") String schemaName);

  @SelectProvider(
      type = SchemaMetaSQLProviderFactory.class,
      method = "batchSelectSchemaByIdentifier")
  List<SchemaPO> batchSelectSchemaByIdentifier(
      @Param("metalakeName") String metalakeName,
      @Param("catalogName") String catalogName,
      @Param("schemaNames") List<String> schemaNames);
}
