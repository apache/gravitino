/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.storage.relation.mysql.mapper;

import com.datastrato.gravitino.storage.relation.mysql.po.MetalakePO;
import java.util.List;
import org.apache.ibatis.annotations.Delete;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;

public interface MetalakeMetaMapper {
  String TABLE_NAME = "metalake_meta";

  @Select(
      "SELECT id, metalake_name as metalakeName, metalake_comment as metalakeComment,"
          + " properties, audit_info as auditInfo, schema_version as schemaVersion"
          + " FROM "
          + TABLE_NAME)
  List<MetalakePO> listMetalakePOs();

  @Select(
      "SELECT id, metalake_name as metalakeName,"
          + " metalake_comment as metalakeComment, properties,"
          + " audit_info as auditInfo, schema_version as schemaVersion"
          + " FROM "
          + TABLE_NAME
          + " WHERE metalake_name = #{metalakeName}")
  MetalakePO selectMetalakeMetaByName(@Param("metalakeName") String name);

  @Select("SELECT id FROM " + TABLE_NAME + " WHERE metalake_name = #{metalakeName}")
  Long selectMetalakeIdMetaByName(@Param("metalakeName") String name);

  @Insert(
      "INSERT INTO "
          + TABLE_NAME
          + "(id, metalake_name, metalake_comment, properties, audit_info, schema_version)"
          + " VALUES("
          + " #{metalakeMeta.id},"
          + " #{metalakeMeta.metalakeName},"
          + " #{metalakeMeta.metalakeComment},"
          + " #{metalakeMeta.properties},"
          + " #{metalakeMeta.auditInfo},"
          + " #{metalakeMeta.schemaVersion}"
          + " )")
  void insertMetalakeMeta(@Param("metalakeMeta") MetalakePO metalakePO);

  @Insert(
      "INSERT INTO "
          + TABLE_NAME
          + "(id, metalake_name, metalake_comment, properties, audit_info, schema_version)"
          + " VALUES("
          + " #{metalakeMeta.id},"
          + " #{metalakeMeta.metalakeName},"
          + " #{metalakeMeta.metalakeComment},"
          + " #{metalakeMeta.properties},"
          + " #{metalakeMeta.auditInfo},"
          + " #{metalakeMeta.schemaVersion}"
          + " )"
          + " ON DUPLICATE KEY UPDATE"
          + " metalake_name = #{metalakeMeta.metalakeName},"
          + " metalake_comment = #{metalakeMeta.metalakeComment},"
          + " properties = #{metalakeMeta.properties},"
          + " audit_info = #{metalakeMeta.auditInfo},"
          + " schema_version = #{metalakeMeta.schemaVersion}")
  void insertMetalakeMetaWithUpdate(@Param("metalakeMeta") MetalakePO metalakePO);

  @Update(
      "UPDATE "
          + TABLE_NAME
          + " SET metalake_name = #{metalakeMeta.metalakeName},"
          + " metalake_comment = #{metalakeMeta.metalakeComment},"
          + " properties = #{metalakeMeta.properties},"
          + " audit_info = #{metalakeMeta.auditInfo},"
          + " schema_version = #{metalakeMeta.schemaVersion}"
          + " WHERE id = #{metalakeMeta.id}")
  void updateMetalakeMeta(@Param("metalakeMeta") MetalakePO metalakePO);

  @Delete("DELETE FROM " + TABLE_NAME + " WHERE id = #{id}")
  Integer deleteMetalakeMetaById(@Param("id") Long id);
}
