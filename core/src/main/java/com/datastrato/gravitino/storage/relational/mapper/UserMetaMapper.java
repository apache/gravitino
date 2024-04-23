/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.storage.relational.mapper;

import com.datastrato.gravitino.storage.relational.po.UserPO;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;

/**
 * A MyBatis Mapper for table meta operation SQLs.
 *
 * <p>This interface class is a specification defined by MyBatis. It requires this interface class
 * to identify the corresponding SQLs for execution. We can write SQLs in an additional XML file, or
 * write SQLs with annotations in this interface Mapper. See: <a
 * href="https://mybatis.org/mybatis-3/getting-started.html"></a>
 */
public interface UserMetaMapper {
  String TABLE_NAME = "user_meta";

  @Select(
      "SELECT user_id as userId FROM "
          + TABLE_NAME
          + " WHERE metalake_id = #{metalakeId} AND user_name = #{userName}"
          + " AND deleted_at = 0")
  Long selectUserIdByMetalakeIdAndName(
      @Param("metalakeId") Long metalakeId, @Param("userName") String name);

  @Select(
      "SELECT user_id as userId, user_name as userName,"
          + " metalake_id as metalakeId,"
          + " audit_info as auditInfo,"
          + " current_version as currentVersion, last_version as lastVersion,"
          + " deleted_at as deletedAt"
          + " FROM "
          + TABLE_NAME
          + " WHERE metalake_id = #{metalakeId} AND user_name = #{userName}"
          + " AND deleted_at = 0")
  UserPO selectUserMetaByMetalakeIdAndName(
      @Param("metalakeId") Long metalakeId, @Param("userName") String name);

  @Insert(
      "INSERT INTO "
          + TABLE_NAME
          + "(user_id, user_name,"
          + " metalake_id, audit_info,"
          + " current_version, last_version, deleted_at)"
          + " VALUES("
          + " #{userMeta.userId},"
          + " #{userMeta.userName},"
          + " #{userMeta.metalakeId},"
          + " #{userMeta.auditInfo},"
          + " #{userMeta.currentVersion},"
          + " #{userMeta.lastVersion},"
          + " #{userMeta.deletedAt}"
          + " )")
  void insertUserMeta(@Param("userMeta") UserPO userPO);

  @Insert(
      "INSERT INTO "
          + TABLE_NAME
          + "(user_id, user_name,"
          + "metalake_id, audit_info,"
          + " current_version, last_version, deleted_at)"
          + " VALUES("
          + " #{userMeta.userId},"
          + " #{userMeta.userName},"
          + " #{userMeta.metalakeId},"
          + " #{userMeta.auditInfo},"
          + " #{userMeta.currentVersion},"
          + " #{userMeta.lastVersion},"
          + " #{userMeta.deletedAt}"
          + " )"
          + " ON DUPLICATE KEY UPDATE"
          + " user_name = #{userMeta.userName},"
          + " metalake_id = #{userMeta.metalakeId},"
          + " audit_info = #{userMeta.auditInfo},"
          + " current_version = #{userMeta.currentVersion},"
          + " last_version = #{userMeta.lastVersion},"
          + " deleted_at = #{userMeta.deletedAt}")
  void insertUserMetaOnDuplicateKeyUpdate(@Param("userMeta") UserPO userPO);

  @Update(
      "UPDATE "
          + TABLE_NAME
          + " SET deleted_at = UNIX_TIMESTAMP(CURRENT_TIMESTAMP(3)) * 1000.0"
          + " WHERE user_id = #{userId} AND deleted_at = 0")
  void softDeleteUserMetaByUserId(@Param("userId") Long userId);

  @Update(
      "UPDATE "
          + TABLE_NAME
          + " SET deleted_at = UNIX_TIMESTAMP(CURRENT_TIMESTAMP(3)) * 1000.0"
          + " WHERE metalake_id = #{metalakeId} AND deleted_at = 0")
  void softDeleteUserMetasByMetalakeId(@Param("metalakeId") Long metalakeId);

  @Update(
      "UPDATE "
          + TABLE_NAME
          + " SET user_name = #{newUserMeta.userName},"
          + " metalake_id = #{newUserMeta.metalakeId},"
          + " audit_info = #{newUserMeta.auditInfo},"
          + " current_version = #{newUserMeta.currentVersion},"
          + " last_version = #{newUserMeta.lastVersion},"
          + " deleted_at = #{newUserMeta.deletedAt}"
          + " WHERE user_id = #{oldUserMeta.userId}"
          + " AND user_name = #{oldUserMeta.userName}"
          + " AND metalake_id = #{oldUserMeta.metalakeId}"
          + " AND audit_info = #{oldUserMeta.auditInfo}"
          + " AND current_version = #{oldUserMeta.currentVersion}"
          + " AND last_version = #{oldUserMeta.lastVersion}"
          + " AND deleted_at = 0")
  Integer updateUserMeta(
      @Param("newUserMeta") UserPO newUserPO, @Param("oldUserMeta") UserPO oldUserPO);
}
