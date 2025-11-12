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
import org.apache.gravitino.storage.relational.po.TagPO;
import org.apache.ibatis.annotations.DeleteProvider;
import org.apache.ibatis.annotations.InsertProvider;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.SelectProvider;
import org.apache.ibatis.annotations.UpdateProvider;

public interface TagMetaMapper {

  String TAG_TABLE_NAME = "tag_meta";

  @SelectProvider(type = TagMetaSQLProviderFactory.class, method = "listTagPOsByMetalake")
  List<TagPO> listTagPOsByMetalake(@Param("metalakeName") String metalakeName);

  @SelectProvider(
      type = TagMetaSQLProviderFactory.class,
      method = "listTagPOsByMetalakeAndTagNames")
  List<TagPO> listTagPOsByMetalakeAndTagNames(
      @Param("metalakeName") String metalakeName, @Param("tagNames") List<String> tagNames);

  @SelectProvider(type = TagMetaSQLProviderFactory.class, method = "selectTagIdByMetalakeAndName")
  Long selectTagIdByMetalakeAndName(
      @Param("metalakeName") String metalakeName, @Param("tagName") String tagName);

  @SelectProvider(type = TagMetaSQLProviderFactory.class, method = "selectTagMetaByMetalakeAndName")
  TagPO selectTagMetaByMetalakeAndName(
      @Param("metalakeName") String metalakeName, @Param("tagName") String tagName);

  @SelectProvider(
      type = TagMetaSQLProviderFactory.class,
      method = "selectTagMetaByMetalakeIdAndName")
  TagPO selectTagMetaByMetalakeIdAndName(
      @Param("metalakeId") Long metalakeId, @Param("name") String tagName);

  @InsertProvider(type = TagMetaSQLProviderFactory.class, method = "insertTagMeta")
  void insertTagMeta(@Param("tagMeta") TagPO tagPO);

  @InsertProvider(
      type = TagMetaSQLProviderFactory.class,
      method = "insertTagMetaOnDuplicateKeyUpdate")
  void insertTagMetaOnDuplicateKeyUpdate(@Param("tagMeta") TagPO tagPO);

  @UpdateProvider(type = TagMetaSQLProviderFactory.class, method = "updateTagMeta")
  Integer updateTagMeta(@Param("newTagMeta") TagPO newTagPO, @Param("oldTagMeta") TagPO oldTagPO);

  @UpdateProvider(
      type = TagMetaSQLProviderFactory.class,
      method = "softDeleteTagMetaByMetalakeAndTagName")
  Integer softDeleteTagMetaByMetalakeAndTagName(
      @Param("metalakeName") String metalakeName, @Param("tagName") String tagName);

  @UpdateProvider(type = TagMetaSQLProviderFactory.class, method = "softDeleteTagMetasByMetalakeId")
  void softDeleteTagMetasByMetalakeId(@Param("metalakeId") Long metalakeId);

  @DeleteProvider(type = TagMetaSQLProviderFactory.class, method = "deleteTagMetasByLegacyTimeline")
  Integer deleteTagMetasByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit);

  @SelectProvider(type = TagMetaSQLProviderFactory.class, method = "selectTagByTagId")
  TagPO selectTagByTagId(@Param("tagId") Long tagId);
}
