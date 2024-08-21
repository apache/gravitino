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

  @SelectProvider.List({
    @SelectProvider(
        type = CatalogMetaMySQLProvider.class,
        method = "listCatalogPOsByMetalakeId",
        databaseId = "mysql"),
    @SelectProvider(
        type = CatalogMetaH2Provider.class,
        method = "listCatalogPOsByMetalakeId",
        databaseId = "h2"),
    @SelectProvider(
        type = CatalogMetaPGProvider.class,
        method = "listCatalogPOsByMetalakeId",
        databaseId = "postgresql"),
  })
  List<CatalogPO> listCatalogPOsByMetalakeId(@Param("metalakeId") Long metalakeId);

  @SelectProvider.List({
    @SelectProvider(
        type = CatalogMetaMySQLProvider.class,
        method = "selectCatalogIdByMetalakeIdAndName",
        databaseId = "mysql"),
    @SelectProvider(
        type = CatalogMetaH2Provider.class,
        method = "selectCatalogIdByMetalakeIdAndName",
        databaseId = "h2"),
    @SelectProvider(
        type = CatalogMetaPGProvider.class,
        method = "selectCatalogIdByMetalakeIdAndName",
        databaseId = "postgresql"),
  })
  Long selectCatalogIdByMetalakeIdAndName(
      @Param("metalakeId") Long metalakeId, @Param("catalogName") String name);

  @SelectProvider.List({
    @SelectProvider(
        type = CatalogMetaMySQLProvider.class,
        method = "selectCatalogMetaByMetalakeIdAndName",
        databaseId = "mysql"),
    @SelectProvider(
        type = CatalogMetaH2Provider.class,
        method = "selectCatalogMetaByMetalakeIdAndName",
        databaseId = "h2"),
    @SelectProvider(
        type = CatalogMetaPGProvider.class,
        method = "selectCatalogMetaByMetalakeIdAndName",
        databaseId = "postgresql"),
  })
  CatalogPO selectCatalogMetaByMetalakeIdAndName(
      @Param("metalakeId") Long metalakeId, @Param("catalogName") String name);

  @SelectProvider.List({
    @SelectProvider(
        type = CatalogMetaMySQLProvider.class,
        method = "selectCatalogMetaById",
        databaseId = "mysql"),
    @SelectProvider(
        type = CatalogMetaH2Provider.class,
        method = "selectCatalogMetaById",
        databaseId = "h2"),
    @SelectProvider(
        type = CatalogMetaPGProvider.class,
        method = "selectCatalogMetaById",
        databaseId = "postgresql"),
  })
  CatalogPO selectCatalogMetaById(@Param("catalogId") Long catalogId);

  @InsertProvider.List({
    @InsertProvider(
        type = CatalogMetaMySQLProvider.class,
        method = "insertCatalogMeta",
        databaseId = "mysql"),
    @InsertProvider(
        type = CatalogMetaH2Provider.class,
        method = "insertCatalogMeta",
        databaseId = "h2"),
    @InsertProvider(
        type = CatalogMetaPGProvider.class,
        method = "insertCatalogMeta",
        databaseId = "postgresql"),
  })
  void insertCatalogMeta(@Param("catalogMeta") CatalogPO catalogPO);

  @InsertProvider.List({
    @InsertProvider(
        type = CatalogMetaMySQLProvider.class,
        method = "insertCatalogMetaOnDuplicateKeyUpdate",
        databaseId = "mysql"),
    @InsertProvider(
        type = CatalogMetaH2Provider.class,
        method = "insertCatalogMetaOnDuplicateKeyUpdate",
        databaseId = "h2"),
    @InsertProvider(
        type = CatalogMetaPGProvider.class,
        method = "insertCatalogMetaOnDuplicateKeyUpdate",
        databaseId = "postgresql"),
  })
  void insertCatalogMetaOnDuplicateKeyUpdate(@Param("catalogMeta") CatalogPO catalogPO);

  @UpdateProvider.List({
    @UpdateProvider(
        type = CatalogMetaMySQLProvider.class,
        method = "updateCatalogMeta",
        databaseId = "mysql"),
    @UpdateProvider(
        type = CatalogMetaH2Provider.class,
        method = "updateCatalogMeta",
        databaseId = "h2"),
    @UpdateProvider(
        type = CatalogMetaPGProvider.class,
        method = "updateCatalogMeta",
        databaseId = "postgresql"),
  })
  Integer updateCatalogMeta(
      @Param("newCatalogMeta") CatalogPO newCatalogPO,
      @Param("oldCatalogMeta") CatalogPO oldCatalogPO);

  @UpdateProvider.List({
    @UpdateProvider(
        type = CatalogMetaMySQLProvider.class,
        method = "softDeleteCatalogMetasByCatalogId",
        databaseId = "mysql"),
    @UpdateProvider(
        type = CatalogMetaH2Provider.class,
        method = "softDeleteCatalogMetasByCatalogId",
        databaseId = "h2"),
    @UpdateProvider(
        type = CatalogMetaPGProvider.class,
        method = "softDeleteCatalogMetasByCatalogId",
        databaseId = "postgresql"),
  })
  Integer softDeleteCatalogMetasByCatalogId(@Param("catalogId") Long catalogId);

  @UpdateProvider.List({
    @UpdateProvider(
        type = CatalogMetaMySQLProvider.class,
        method = "softDeleteCatalogMetasByMetalakeId",
        databaseId = "mysql"),
    @UpdateProvider(
        type = CatalogMetaH2Provider.class,
        method = "softDeleteCatalogMetasByMetalakeId",
        databaseId = "h2"),
    @UpdateProvider(
        type = CatalogMetaPGProvider.class,
        method = "softDeleteCatalogMetasByMetalakeId",
        databaseId = "postgresql"),
  })
  Integer softDeleteCatalogMetasByMetalakeId(@Param("metalakeId") Long metalakeId);

  @DeleteProvider.List({
    @DeleteProvider(
        type = CatalogMetaMySQLProvider.class,
        method = "deleteCatalogMetasByLegacyTimeline",
        databaseId = "mysql"),
    @DeleteProvider(
        type = CatalogMetaH2Provider.class,
        method = "deleteCatalogMetasByLegacyTimeline",
        databaseId = "h2"),
    @DeleteProvider(
        type = CatalogMetaPGProvider.class,
        method = "deleteCatalogMetasByLegacyTimeline",
        databaseId = "postgresql"),
  })
  Integer deleteCatalogMetasByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit);
}
