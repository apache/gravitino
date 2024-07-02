/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.storage.relational.mapper;

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
          + " WHERE EXISTS ( SELECT * FROM "
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
