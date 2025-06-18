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

import org.apache.gravitino.storage.relational.po.GroupPO;
import org.apache.gravitino.storage.relational.po.OwnerRelPO;
import org.apache.gravitino.storage.relational.po.UserPO;
import org.apache.ibatis.annotations.InsertProvider;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.SelectProvider;
import org.apache.ibatis.annotations.UpdateProvider;

/**
 * A MyBatis Mapper for owner meta operation SQLs.
 *
 * <p>This interface class is a specification defined by MyBatis. It requires this interface class
 * to identify the corresponding SQLs for execution. We can write SQLs in an additional XML file, or
 * write SQLs with annotations in this interface Mapper. See: <a
 * href="https://mybatis.org/mybatis-3/getting-started.html"></a>
 */
public interface OwnerMetaMapper {

  String OWNER_TABLE_NAME = "owner_meta";

  @SelectProvider(
      type = OwnerMetaSQLProviderFactory.class,
      method = "selectUserOwnerMetaByMetadataObjectIdAndType")
  UserPO selectUserOwnerMetaByMetadataObjectIdAndType(
      @Param("metadataObjectId") Long metadataObjectId,
      @Param("metadataObjectType") String metadataObjectType);

  @SelectProvider(
      type = OwnerMetaSQLProviderFactory.class,
      method = "selectGroupOwnerMetaByMetadataObjectIdAndType")
  GroupPO selectGroupOwnerMetaByMetadataObjectIdAndType(
      @Param("metadataObjectId") Long metadataObjectId,
      @Param("metadataObjectType") String metadataObjectType);

  @InsertProvider(type = OwnerMetaSQLProviderFactory.class, method = "insertOwnerRel")
  void insertOwnerRel(@Param("ownerRelPO") OwnerRelPO ownerRelPO);

  @UpdateProvider(
      type = OwnerMetaSQLProviderFactory.class,
      method = "softDeleteOwnerRelByMetadataObjectIdAndType")
  void softDeleteOwnerRelByMetadataObjectIdAndType(
      @Param("metadataObjectId") Long metadataObjectId,
      @Param("metadataObjectType") String metadataObjectType);

  @UpdateProvider(
      type = OwnerMetaSQLProviderFactory.class,
      method = "softDeleteOwnerRelByOwnerIdAndType")
  void softDeleteOwnerRelByOwnerIdAndType(
      @Param("ownerId") Long ownerId, @Param("ownerType") String ownerType);

  @UpdateProvider(
      type = OwnerMetaSQLProviderFactory.class,
      method = "softDeleteOwnerRelByMetalakeId")
  void softDeleteOwnerRelByMetalakeId(@Param("metalakeId") Long metalakeId);

  @UpdateProvider(
      type = OwnerMetaSQLProviderFactory.class,
      method = "softDeleteOwnerRelByCatalogId")
  void softDeleteOwnerRelByCatalogId(@Param("catalogId") Long catalogId);

  @UpdateProvider(type = OwnerMetaSQLProviderFactory.class, method = "softDeleteOwnerRelBySchemaId")
  void softDeleteOwnerRelBySchemaId(@Param("schemaId") Long schemaId);

  @UpdateProvider(
      type = OwnerMetaSQLProviderFactory.class,
      method = "deleteOwnerMetasByLegacyTimeline")
  Integer deleteOwnerMetasByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit);
}
