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
import org.apache.gravitino.storage.relational.po.ViewPO;
import org.apache.gravitino.storage.relational.po.ViewVersionInfoPO;
import org.apache.ibatis.annotations.DeleteProvider;
import org.apache.ibatis.annotations.InsertProvider;
import org.apache.ibatis.annotations.One;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Result;
import org.apache.ibatis.annotations.ResultMap;
import org.apache.ibatis.annotations.Results;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.SelectProvider;
import org.apache.ibatis.annotations.UpdateProvider;

/** A MyBatis Mapper for view meta operation SQLs. */
public interface ViewMetaMapper {
  String TABLE_NAME = "view_meta";
  String VERSION_TABLE_NAME = "view_version_info";

  @Results(
      id = "mapToViewVersionInfoPO",
      value = {
        @Result(property = "id", column = "id", id = true),
        @Result(property = "metalakeId", column = "version_metalake_id"),
        @Result(property = "catalogId", column = "version_catalog_id"),
        @Result(property = "schemaId", column = "version_schema_id"),
        @Result(property = "viewId", column = "version_view_id"),
        @Result(property = "version", column = "version"),
        @Result(property = "viewComment", column = "view_comment"),
        @Result(property = "columns", column = "columns"),
        @Result(property = "properties", column = "properties"),
        @Result(property = "defaultCatalog", column = "default_catalog"),
        @Result(property = "defaultSchema", column = "default_schema"),
        @Result(property = "representations", column = "representations"),
        @Result(property = "auditInfo", column = "version_audit_info"),
        @Result(property = "deletedAt", column = "version_deleted_at")
      })
  @Select("SELECT 1") // Dummy SQL to avoid MyBatis error, never be executed
  ViewVersionInfoPO mapToViewVersionInfoPO();

  @Results(
      id = "viewPOResultMap",
      value = {
        @Result(property = "viewId", column = "view_id", id = true),
        @Result(property = "viewName", column = "view_name"),
        @Result(property = "metalakeId", column = "metalake_id"),
        @Result(property = "catalogId", column = "catalog_id"),
        @Result(property = "schemaId", column = "schema_id"),
        @Result(property = "currentVersion", column = "current_version"),
        @Result(property = "lastVersion", column = "last_version"),
        @Result(property = "auditInfo", column = "audit_info"),
        @Result(property = "deletedAt", column = "deleted_at"),
        @Result(
            property = "viewVersionInfoPO",
            javaType = ViewVersionInfoPO.class,
            column =
                "{id,version_metalake_id,version_catalog_id,version_schema_id,version_view_id,"
                    + "version,view_comment,columns,properties,default_catalog,default_schema,"
                    + "representations,version_audit_info,version_deleted_at}",
            one = @One(resultMap = "mapToViewVersionInfoPO"))
      })
  @SelectProvider(type = ViewMetaSQLProviderFactory.class, method = "listViewPOsBySchemaId")
  List<ViewPO> listViewPOsBySchemaId(@Param("schemaId") Long schemaId);

  @ResultMap("viewPOResultMap")
  @SelectProvider(
      type = ViewMetaSQLProviderFactory.class,
      method = "listViewPOsByFullQualifiedName")
  List<ViewPO> listViewPOsByFullQualifiedName(
      @Param("metalakeName") String metalakeName,
      @Param("catalogName") String catalogName,
      @Param("schemaName") String schemaName);

  @SelectProvider(type = ViewMetaSQLProviderFactory.class, method = "selectViewIdBySchemaIdAndName")
  Long selectViewIdBySchemaIdAndName(
      @Param("schemaId") Long schemaId, @Param("viewName") String name);

  @ResultMap("viewPOResultMap")
  @SelectProvider(type = ViewMetaSQLProviderFactory.class, method = "listViewPOsByViewIds")
  List<ViewPO> listViewPOsByViewIds(@Param("viewIds") List<Long> viewIds);

  @ResultMap("viewPOResultMap")
  @SelectProvider(
      type = ViewMetaSQLProviderFactory.class,
      method = "selectViewMetaBySchemaIdAndName")
  ViewPO selectViewMetaBySchemaIdAndName(
      @Param("schemaId") Long schemaId, @Param("viewName") String name);

  @ResultMap("viewPOResultMap")
  @SelectProvider(type = ViewMetaSQLProviderFactory.class, method = "selectViewByFullQualifiedName")
  ViewPO selectViewByFullQualifiedName(
      @Param("metalakeName") String metalakeName,
      @Param("catalogName") String catalogName,
      @Param("schemaName") String schemaName,
      @Param("viewName") String viewName);

  @InsertProvider(type = ViewMetaSQLProviderFactory.class, method = "insertViewMeta")
  void insertViewMeta(@Param("viewMeta") ViewPO viewPO);

  @InsertProvider(
      type = ViewMetaSQLProviderFactory.class,
      method = "insertViewMetaOnDuplicateKeyUpdate")
  void insertViewMetaOnDuplicateKeyUpdate(@Param("viewMeta") ViewPO viewPO);

  @UpdateProvider(type = ViewMetaSQLProviderFactory.class, method = "updateViewMeta")
  Integer updateViewMeta(
      @Param("newViewMeta") ViewPO newViewPO, @Param("oldViewMeta") ViewPO oldViewPO);

  @UpdateProvider(type = ViewMetaSQLProviderFactory.class, method = "softDeleteViewMetasByViewId")
  Integer softDeleteViewMetasByViewId(@Param("viewId") Long viewId);

  @UpdateProvider(
      type = ViewMetaSQLProviderFactory.class,
      method = "softDeleteViewMetasByMetalakeId")
  Integer softDeleteViewMetasByMetalakeId(@Param("metalakeId") Long metalakeId);

  @UpdateProvider(
      type = ViewMetaSQLProviderFactory.class,
      method = "softDeleteViewMetasByCatalogId")
  Integer softDeleteViewMetasByCatalogId(@Param("catalogId") Long catalogId);

  @UpdateProvider(type = ViewMetaSQLProviderFactory.class, method = "softDeleteViewMetasBySchemaId")
  Integer softDeleteViewMetasBySchemaId(@Param("schemaId") Long schemaId);

  @DeleteProvider(
      type = ViewMetaSQLProviderFactory.class,
      method = "deleteViewMetasByLegacyTimeline")
  Integer deleteViewMetasByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit);
}
