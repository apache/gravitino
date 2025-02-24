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

  @SelectProvider(type = SchemaMetaSQLProviderFactory.class, method = "listSchemaPOsBySchemaIds")
  List<SchemaPO> listSchemaPOsBySchemaIds(@Param("schemaIds") List<Long> schemaIds);

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

  @SelectProvider(type = SchemaMetaSQLProviderFactory.class, method = "selectSchemaMetaById")
  SchemaPO selectSchemaMetaById(@Param("schemaId") Long schemaId);

  @InsertProvider(type = SchemaMetaSQLProviderFactory.class, method = "insertSchemaMeta")
  void insertSchemaMeta(@Param("schemaMeta") SchemaPO schemaPO);

  @InsertProvider(
      type = SchemaMetaSQLProviderFactory.class,
      method = "insertSchemaMetaOnDuplicateKeyUpdate")
  void insertSchemaMetaOnDuplicateKeyUpdate(@Param("schemaMeta") SchemaPO schemaPO);

  @UpdateProvider(type = SchemaMetaSQLProviderFactory.class, method = "updateSchemaMeta")
  Integer updateSchemaMeta(
      @Param("newSchemaMeta") SchemaPO newSchemaPO, @Param("oldSchemaMeta") SchemaPO oldSchemaPO);

  @UpdateProvider(
      type = SchemaMetaSQLProviderFactory.class,
      method = "softDeleteSchemaMetasBySchemaId")
  Integer softDeleteSchemaMetasBySchemaId(@Param("schemaId") Long schemaId);

  @UpdateProvider(
      type = SchemaMetaSQLProviderFactory.class,
      method = "softDeleteSchemaMetasByMetalakeId")
  Integer softDeleteSchemaMetasByMetalakeId(@Param("metalakeId") Long metalakeId);

  @UpdateProvider(
      type = SchemaMetaSQLProviderFactory.class,
      method = "softDeleteSchemaMetasByCatalogId")
  Integer softDeleteSchemaMetasByCatalogId(@Param("catalogId") Long catalogId);

  @DeleteProvider(
      type = SchemaMetaSQLProviderFactory.class,
      method = "deleteSchemaMetasByLegacyTimeline")
  Integer deleteSchemaMetasByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit);
}
