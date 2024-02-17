/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.storage.relational.mapper;

import com.datastrato.gravitino.storage.relational.po.MetalakePO;
import java.util.List;
import org.apache.ibatis.annotations.Delete;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;

/**
 * A MyBatis Mapper for metalake meta operation SQLs.
 *
 * <p>This interface class is a specification defined by MyBatis. It requires this interface class
 * to identify the corresponding SQLs for execution. We can write SQLs in an additional XML file, or
 * write SQLs with annotations in this interface Mapper. See: <a
 * href="https://mybatis.org/mybatis-3/getting-started.html"></a>
 */
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
          + " SET metalake_name = #{newMetalakeMeta.metalakeName},"
          + " metalake_comment = #{newMetalakeMeta.metalakeComment},"
          + " properties = #{newMetalakeMeta.properties},"
          + " audit_info = #{newMetalakeMeta.auditInfo},"
          + " schema_version = #{newMetalakeMeta.schemaVersion}"
          + " WHERE id = #{oldMetalakeMeta.id}"
          + " and metalake_name = #{oldMetalakeMeta.metalakeName}"
          + " and metalake_comment = #{oldMetalakeMeta.metalakeComment}"
          + " and properties = #{oldMetalakeMeta.properties}"
          + " and audit_info = #{oldMetalakeMeta.auditInfo}"
          + " and schema_version = #{oldMetalakeMeta.schemaVersion}")
  Integer updateMetalakeMeta(
      @Param("newMetalakeMeta") MetalakePO newMetalakePO,
      @Param("oldMetalakeMeta") MetalakePO oldMetalakePO);

  @Delete("DELETE FROM " + TABLE_NAME + " WHERE id = #{id}")
  Integer deleteMetalakeMetaById(@Param("id") Long id);
}
