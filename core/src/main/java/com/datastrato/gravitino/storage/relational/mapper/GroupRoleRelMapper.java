/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.storage.relational.mapper;

import com.datastrato.gravitino.storage.relational.po.GroupRoleRelPO;
import java.util.List;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Update;

/**
 * A MyBatis Mapper for table meta operation SQLs.
 *
 * <p>This interface class is a specification defined by MyBatis. It requires this interface class
 * to identify the corresponding SQLs for execution. We can write SQLs in an additional XML file, or
 * write SQLs with annotations in this interface Mapper. See: <a
 * href="https://mybatis.org/mybatis-3/getting-started.html"></a>
 */
public interface GroupRoleRelMapper {
  String TABLE_NAME = "group_role_rel";

  @Insert({
    "<script>",
    "INSERT INTO "
        + TABLE_NAME
        + "(group_id, role_id,"
        + " audit_info,"
        + " current_version, last_version, deleted_at)"
        + " VALUES ",
    "<foreach collection='groupRoleRels' item='item' separator=','>",
    "(#{item.groupId},"
        + " #{item.roleId},"
        + " #{item.auditInfo},"
        + " #{item.currentVersion},"
        + " #{item.lastVersion},"
        + " #{item.deletedAt})",
    "</foreach>",
    "</script>"
  })
  void batchInsertGroupRoleRel(@Param("groupRoleRels") List<GroupRoleRelPO> groupRoleRelPOS);

  @Insert({
    "<script>",
    "INSERT INTO "
        + TABLE_NAME
        + "(group_id, role_id,"
        + " audit_info,"
        + " current_version, last_version, deleted_at)"
        + " VALUES ",
    "<foreach collection='groupRoleRels' item='item' separator=','>",
    "(#{item.groupId},"
        + " #{item.roleId},"
        + " #{item.auditInfo},"
        + " #{item.currentVersion},"
        + " #{item.lastVersion},"
        + " #{item.deletedAt})",
    "</foreach>",
    " ON DUPLICATE KEY UPDATE"
        + " group_id = VALUES(group_id),"
        + " role_id = VALUES(role_id),"
        + " audit_info = VALUES(audit_info),"
        + " current_version = VALUES(current_version),"
        + " last_version = VALUES(last_version),"
        + " deleted_at = VALUES(deleted_at)",
    "</script>"
  })
  void batchInsertGroupRoleRelOnDuplicateKeyUpdate(
      @Param("groupRoleRels") List<GroupRoleRelPO> groupRoleRelPOS);

  @Update(
      "UPDATE "
          + TABLE_NAME
          + " SET deleted_at = UNIX_TIMESTAMP(CURRENT_TIMESTAMP(3)) * 1000.0"
          + " WHERE group_id = #{groupId} AND deleted_at = 0")
  void softDeleteGroupRoleRelByGroupId(@Param("groupId") Long groupId);

  @Update({
    "<script>",
    "UPDATE "
        + TABLE_NAME
        + " SET deleted_at = UNIX_TIMESTAMP(CURRENT_TIMESTAMP(3)) * 1000.0"
        + " WHERE group_id = #{groupId} AND role_id in (",
    "<foreach collection='roleIds' item='roleId' separator=','>",
    "#{roleId}",
    "</foreach>",
    ") " + "AND deleted_at = 0",
    "</script>"
  })
  void softDeleteGroupRoleRelByGroupAndRoles(
      @Param("groupId") Long groupId, @Param("roleIds") List<Long> roleIds);
}
