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
import org.apache.gravitino.storage.relational.po.TagMetadataObjectRelPO;
import org.apache.gravitino.storage.relational.po.TagPO;
import org.apache.ibatis.annotations.DeleteProvider;
import org.apache.ibatis.annotations.InsertProvider;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.SelectProvider;
import org.apache.ibatis.annotations.UpdateProvider;

public interface TagMetadataObjectRelMapper {
  String TAG_METADATA_OBJECT_RELATION_TABLE_NAME = "tag_relation_meta";

  @SelectProvider(
      type = TagMetadataObjectRelSQLProviderFactory.class,
      method = "listTagPOsByMetadataObjectIdAndType")
  List<TagPO> listTagPOsByMetadataObjectIdAndType(
      @Param("metadataObjectId") Long metadataObjectId,
      @Param("metadataObjectType") String metadataObjectType);

  @SelectProvider(
      type = TagMetadataObjectRelSQLProviderFactory.class,
      method = "getTagPOsByMetadataObjectAndTagName")
  TagPO getTagPOsByMetadataObjectAndTagName(
      @Param("metadataObjectId") Long metadataObjectId,
      @Param("metadataObjectType") String metadataObjectType,
      @Param("tagName") String tagName);

  @SelectProvider(
      type = TagMetadataObjectRelSQLProviderFactory.class,
      method = "listTagMetadataObjectRelsByMetalakeAndTagName")
  List<TagMetadataObjectRelPO> listTagMetadataObjectRelsByMetalakeAndTagName(
      @Param("metalakeName") String metalakeName, @Param("tagName") String tagName);

  @InsertProvider(
      type = TagMetadataObjectRelSQLProviderFactory.class,
      method = "batchInsertTagMetadataObjectRels")
  void batchInsertTagMetadataObjectRels(@Param("tagRels") List<TagMetadataObjectRelPO> tagRelPOs);

  @UpdateProvider(
      type = TagMetadataObjectRelSQLProviderFactory.class,
      method = "batchDeleteTagMetadataObjectRelsByTagIdsAndMetadataObject")
  void batchDeleteTagMetadataObjectRelsByTagIdsAndMetadataObject(
      @Param("metadataObjectId") Long metadataObjectId,
      @Param("metadataObjectType") String metadataObjectType,
      @Param("tagIds") List<Long> tagIds);

  @UpdateProvider(
      type = TagMetadataObjectRelSQLProviderFactory.class,
      method = "softDeleteTagMetadataObjectRelsByMetalakeAndTagName")
  Integer softDeleteTagMetadataObjectRelsByMetalakeAndTagName(
      @Param("metalakeName") String metalakeName, @Param("tagName") String tagName);

  @UpdateProvider(
      type = TagMetadataObjectRelSQLProviderFactory.class,
      method = "softDeleteTagMetadataObjectRelsByMetalakeId")
  void softDeleteTagMetadataObjectRelsByMetalakeId(@Param("metalakeId") Long metalakeId);

  @DeleteProvider(
      type = TagMetadataObjectRelSQLProviderFactory.class,
      method = "deleteTagEntityRelsByLegacyTimeline")
  Integer deleteTagEntityRelsByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit);
}
