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
package com.apache.gravitino.storage.relational.mapper;

import org.apache.ibatis.annotations.Delete;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Update;

public interface TagMetadataObjectRelMapper {
  String TAG_METADATA_OBJECT_RELATION_TABLE_NAME = "tag_relation_meta";

  @Update(
      "UPDATE "
          + TAG_METADATA_OBJECT_RELATION_TABLE_NAME
          + " tmo SET tmo.deleted_at = (UNIX_TIMESTAMP() * 1000.0)"
          + " + EXTRACT(MICROSECOND FROM CURRENT_TIMESTAMP(3)) / 1000"
          + " WHERE tmo.tag_id IN (SELECT tm.tag_id FROM "
          + TagMetaMapper.TAG_TABLE_NAME
          + " tm WHERE tm.metalake_id IN (SELECT mm.metalake_id FROM "
          + MetalakeMetaMapper.TABLE_NAME
          + " mm WHERE mm.metalake_name = #{metalakeName} AND mm.deleted_at = 0)"
          + " AND tm.deleted_at = 0) AND tmo.deleted_at = 0")
  Integer softDeleteTagMetadataObjectRelsByMetalakeAndTagName(
      @Param("metalakeName") String metalakeName, @Param("tagName") String tagName);

  @Update(
      "UPDATE "
          + TAG_METADATA_OBJECT_RELATION_TABLE_NAME
          + " tmo SET tmo.deleted_at = (UNIX_TIMESTAMP() * 1000.0)"
          + " + EXTRACT(MICROSECOND FROM CURRENT_TIMESTAMP(3)) / 1000"
          + " WHERE EXISTS (SELECT * FROM "
          + TagMetaMapper.TAG_TABLE_NAME
          + " tm WHERE tm.metalake_id = #{metalakeId} AND tm.tag_id = tmo.tag_id"
          + " AND tm.deleted_at = 0) AND tmo.deleted_at = 0")
  void softDeleteTagMetadataObjectRelsByMetalakeId(@Param("metalakeId") Long metalakeId);

  @Delete(
      "DELETE FROM "
          + TAG_METADATA_OBJECT_RELATION_TABLE_NAME
          + " WHERE deleted_at > 0 AND deleted_at < #{legacyTimeline} LIMIT #{limit}")
  Integer deleteTagEntityRelsByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit);
}
