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
import org.apache.ibatis.annotations.DeleteProvider;
import org.apache.ibatis.annotations.InsertProvider;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.SelectProvider;
import org.apache.ibatis.annotations.UpdateProvider;

/**
 * A MyBatis Mapper for view meta operation SQLs.
 *
 * <p>This interface class is a specification defined by MyBatis. It requires this interface class
 * to identify the corresponding SQLs for execution. We can write SQLs in an additional XML file, or
 * write SQLs with annotations in this interface Mapper. See: <a
 * href="https://mybatis.org/mybatis-3/getting-started.html"></a>
 */
public interface ViewMetaMapper {
  String TABLE_NAME = "view_meta";

  @SelectProvider(type = ViewMetaSQLProviderFactory.class, method = "listViewPOsBySchemaId")
  List<ViewPO> listViewPOsBySchemaId(@Param("schemaId") Long schemaId);

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

  @SelectProvider(type = ViewMetaSQLProviderFactory.class, method = "listViewPOsByViewIds")
  List<ViewPO> listViewPOsByViewIds(@Param("viewIds") List<Long> viewIds);

  @SelectProvider(
      type = ViewMetaSQLProviderFactory.class,
      method = "selectViewMetaBySchemaIdAndName")
  ViewPO selectViewMetaBySchemaIdAndName(
      @Param("schemaId") Long schemaId, @Param("viewName") String name);

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
