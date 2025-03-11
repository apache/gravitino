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
import org.apache.gravitino.storage.relational.po.CatalogPO;
import org.apache.ibatis.annotations.DeleteProvider;
import org.apache.ibatis.annotations.InsertProvider;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.SelectProvider;
import org.apache.ibatis.annotations.UpdateProvider;

/**
 * A MyBatis Mapper for catalog meta operation SQLs.
 *
 * <p>This interface class is a specification defined by MyBatis. It requires this interface class
 * to identify the corresponding SQLs for execution. We can write SQLs in an additional XML file, or
 * write SQLs with annotations in this interface Mapper. See: <a
 * href="https://mybatis.org/mybatis-3/getting-started.html"></a>
 */
public interface CatalogMetaMapper {
  String TABLE_NAME = "catalog_meta";

  @SelectProvider(type = CatalogMetaSQLProviderFactory.class, method = "listCatalogPOsByMetalakeId")
  List<CatalogPO> listCatalogPOsByMetalakeId(@Param("metalakeId") Long metalakeId);

  @SelectProvider(type = CatalogMetaSQLProviderFactory.class, method = "listCatalogPOsByCatalogIds")
  List<CatalogPO> listCatalogPOsByCatalogIds(@Param("catalogIds") List<Long> catalogIds);

  @SelectProvider(
      type = CatalogMetaSQLProviderFactory.class,
      method = "selectCatalogIdByMetalakeIdAndName")
  Long selectCatalogIdByMetalakeIdAndName(
      @Param("metalakeId") Long metalakeId, @Param("catalogName") String name);

  @SelectProvider(
      type = CatalogMetaSQLProviderFactory.class,
      method = "selectCatalogMetaByMetalakeIdAndName")
  CatalogPO selectCatalogMetaByMetalakeIdAndName(
      @Param("metalakeId") Long metalakeId, @Param("catalogName") String name);

  @SelectProvider(type = CatalogMetaSQLProviderFactory.class, method = "selectCatalogMetaById")
  CatalogPO selectCatalogMetaById(@Param("catalogId") Long catalogId);

  @InsertProvider(type = CatalogMetaSQLProviderFactory.class, method = "insertCatalogMeta")
  void insertCatalogMeta(@Param("catalogMeta") CatalogPO catalogPO);

  @InsertProvider(
      type = CatalogMetaSQLProviderFactory.class,
      method = "insertCatalogMetaOnDuplicateKeyUpdate")
  void insertCatalogMetaOnDuplicateKeyUpdate(@Param("catalogMeta") CatalogPO catalogPO);

  @UpdateProvider(type = CatalogMetaSQLProviderFactory.class, method = "updateCatalogMeta")
  Integer updateCatalogMeta(
      @Param("newCatalogMeta") CatalogPO newCatalogPO,
      @Param("oldCatalogMeta") CatalogPO oldCatalogPO);

  @UpdateProvider(
      type = CatalogMetaSQLProviderFactory.class,
      method = "softDeleteCatalogMetasByCatalogId")
  Integer softDeleteCatalogMetasByCatalogId(@Param("catalogId") Long catalogId);

  @UpdateProvider(
      type = CatalogMetaSQLProviderFactory.class,
      method = "softDeleteCatalogMetasByMetalakeId")
  Integer softDeleteCatalogMetasByMetalakeId(@Param("metalakeId") Long metalakeId);

  @DeleteProvider(
      type = CatalogMetaSQLProviderFactory.class,
      method = "deleteCatalogMetasByLegacyTimeline")
  Integer deleteCatalogMetasByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit);
}
