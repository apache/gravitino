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
import org.apache.gravitino.storage.relational.po.MetalakePO;
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
      "SELECT metalake_id as metalakeId, metalake_name as metalakeName,"
          + " metalake_comment as metalakeComment, properties,"
          + " audit_info as auditInfo, schema_version as schemaVersion,"
          + " current_version as currentVersion, last_version as lastVersion,"
          + " deleted_at as deletedAt"
          + " FROM "
          + TABLE_NAME
          + " WHERE deleted_at = 0")
  List<MetalakePO> listMetalakePOs();

  @Select(
      "SELECT metalake_id as metalakeId, metalake_name as metalakeName,"
          + " metalake_comment as metalakeComment, properties,"
          + " audit_info as auditInfo, schema_version as schemaVersion,"
          + " current_version as currentVersion, last_version as lastVersion,"
          + " deleted_at as deletedAt"
          + " FROM "
          + TABLE_NAME
          + " WHERE metalake_name = #{metalakeName} and deleted_at = 0")
  MetalakePO selectMetalakeMetaByName(@Param("metalakeName") String name);

  @Select(
      "SELECT metalake_id as metalakeId, metalake_name as metalakeName,"
          + " metalake_comment as metalakeComment, properties,"
          + " audit_info as auditInfo, schema_version as schemaVersion,"
          + " current_version as currentVersion, last_version as lastVersion,"
          + " deleted_at as deletedAt"
          + " FROM "
          + TABLE_NAME
          + " WHERE metalake_id = #{metalaId} and deleted_at = 0")
  MetalakePO selectMetalakeMetaById(@Param("metalakeId") Long metalakeId);

  @Select(
      "SELECT metalake_id FROM "
          + TABLE_NAME
          + " WHERE metalake_name = #{metalakeName} and deleted_at = 0")
  Long selectMetalakeIdMetaByName(@Param("metalakeName") String name);

  @Insert(
      "INSERT INTO "
          + TABLE_NAME
          + "(metalake_id, metalake_name, metalake_comment, properties, audit_info,"
          + " schema_version, current_version, last_version, deleted_at)"
          + " VALUES("
          + " #{metalakeMeta.metalakeId},"
          + " #{metalakeMeta.metalakeName},"
          + " #{metalakeMeta.metalakeComment},"
          + " #{metalakeMeta.properties},"
          + " #{metalakeMeta.auditInfo},"
          + " #{metalakeMeta.schemaVersion},"
          + " #{metalakeMeta.currentVersion},"
          + " #{metalakeMeta.lastVersion},"
          + " #{metalakeMeta.deletedAt}"
          + " )")
  void insertMetalakeMeta(@Param("metalakeMeta") MetalakePO metalakePO);

  @Insert(
      "INSERT INTO "
          + TABLE_NAME
          + "(metalake_id, metalake_name, metalake_comment, properties, audit_info,"
          + " schema_version, current_version, last_version, deleted_at)"
          + " VALUES("
          + " #{metalakeMeta.metalakeId},"
          + " #{metalakeMeta.metalakeName},"
          + " #{metalakeMeta.metalakeComment},"
          + " #{metalakeMeta.properties},"
          + " #{metalakeMeta.auditInfo},"
          + " #{metalakeMeta.schemaVersion},"
          + " #{metalakeMeta.currentVersion},"
          + " #{metalakeMeta.lastVersion},"
          + " #{metalakeMeta.deletedAt}"
          + " )"
          + " ON DUPLICATE KEY UPDATE"
          + " metalake_name = #{metalakeMeta.metalakeName},"
          + " metalake_comment = #{metalakeMeta.metalakeComment},"
          + " properties = #{metalakeMeta.properties},"
          + " audit_info = #{metalakeMeta.auditInfo},"
          + " schema_version = #{metalakeMeta.schemaVersion},"
          + " current_version = #{metalakeMeta.currentVersion},"
          + " last_version = #{metalakeMeta.lastVersion},"
          + " deleted_at = #{metalakeMeta.deletedAt}")
  void insertMetalakeMetaOnDuplicateKeyUpdate(@Param("metalakeMeta") MetalakePO metalakePO);

  @Update(
      "UPDATE "
          + TABLE_NAME
          + " SET metalake_name = #{newMetalakeMeta.metalakeName},"
          + " metalake_comment = #{newMetalakeMeta.metalakeComment},"
          + " properties = #{newMetalakeMeta.properties},"
          + " audit_info = #{newMetalakeMeta.auditInfo},"
          + " schema_version = #{newMetalakeMeta.schemaVersion},"
          + " current_version = #{newMetalakeMeta.currentVersion},"
          + " last_version = #{newMetalakeMeta.lastVersion}"
          + " WHERE metalake_id = #{oldMetalakeMeta.metalakeId}"
          + " AND metalake_name = #{oldMetalakeMeta.metalakeName}"
          + " AND metalake_comment = #{oldMetalakeMeta.metalakeComment}"
          + " AND properties = #{oldMetalakeMeta.properties}"
          + " AND audit_info = #{oldMetalakeMeta.auditInfo}"
          + " AND schema_version = #{oldMetalakeMeta.schemaVersion}"
          + " AND current_version = #{oldMetalakeMeta.currentVersion}"
          + " AND last_version = #{oldMetalakeMeta.lastVersion}"
          + " AND deleted_at = 0")
  Integer updateMetalakeMeta(
      @Param("newMetalakeMeta") MetalakePO newMetalakePO,
      @Param("oldMetalakeMeta") MetalakePO oldMetalakePO);

  @Update(
      "UPDATE "
          + TABLE_NAME
          + " SET deleted_at = (UNIX_TIMESTAMP() * 1000.0)"
          + " + EXTRACT(MICROSECOND FROM CURRENT_TIMESTAMP(3)) / 1000"
          + " WHERE metalake_id = #{metalakeId} AND deleted_at = 0")
  Integer softDeleteMetalakeMetaByMetalakeId(@Param("metalakeId") Long metalakeId);

  @Delete(
      "DELETE FROM "
          + TABLE_NAME
          + " WHERE deleted_at > 0 AND deleted_at < #{legacyTimeline} LIMIT #{limit}")
  Integer deleteMetalakeMetasByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit);
}
