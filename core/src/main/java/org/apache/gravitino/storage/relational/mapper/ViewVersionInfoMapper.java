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

import org.apache.gravitino.storage.relational.po.ViewVersionInfoPO;
import org.apache.ibatis.annotations.DeleteProvider;
import org.apache.ibatis.annotations.InsertProvider;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.SelectProvider;
import org.apache.ibatis.annotations.UpdateProvider;

/** A MyBatis Mapper for view version info operation SQLs. */
public interface ViewVersionInfoMapper {

  String TABLE_NAME = "view_version_info";

  @InsertProvider(type = ViewVersionInfoSQLProviderFactory.class, method = "insertViewVersionInfo")
  void insertViewVersionInfo(@Param("viewVersionInfo") ViewVersionInfoPO viewVersionInfoPO);

  @InsertProvider(
      type = ViewVersionInfoSQLProviderFactory.class,
      method = "insertViewVersionInfoOnDuplicateKeyUpdate")
  void insertViewVersionInfoOnDuplicateKeyUpdate(
      @Param("viewVersionInfo") ViewVersionInfoPO viewVersionInfoPO);

  @SelectProvider(
      type = ViewVersionInfoSQLProviderFactory.class,
      method = "selectViewVersionInfoByViewIdAndVersion")
  ViewVersionInfoPO selectViewVersionInfoByViewIdAndVersion(
      @Param("viewId") Long viewId, @Param("version") Integer version);

  @UpdateProvider(
      type = ViewVersionInfoSQLProviderFactory.class,
      method = "softDeleteViewVersionsByViewId")
  Integer softDeleteViewVersionsByViewId(@Param("viewId") Long viewId);

  @UpdateProvider(
      type = ViewVersionInfoSQLProviderFactory.class,
      method = "softDeleteViewVersionsBySchemaId")
  Integer softDeleteViewVersionsBySchemaId(@Param("schemaId") Long schemaId);

  @UpdateProvider(
      type = ViewVersionInfoSQLProviderFactory.class,
      method = "softDeleteViewVersionsByCatalogId")
  Integer softDeleteViewVersionsByCatalogId(@Param("catalogId") Long catalogId);

  @UpdateProvider(
      type = ViewVersionInfoSQLProviderFactory.class,
      method = "softDeleteViewVersionsByMetalakeId")
  Integer softDeleteViewVersionsByMetalakeId(@Param("metalakeId") Long metalakeId);

  @DeleteProvider(
      type = ViewVersionInfoSQLProviderFactory.class,
      method = "deleteViewVersionsByLegacyTimeline")
  Integer deleteViewVersionsByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit);
}
