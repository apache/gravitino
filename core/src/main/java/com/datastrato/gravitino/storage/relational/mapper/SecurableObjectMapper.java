/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.storage.relational.mapper;

import com.datastrato.gravitino.storage.relational.po.SecurableObjectPO;
import java.util.List;
import org.apache.ibatis.annotations.Delete;
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
public interface SecurableObjectMapper {

  String SECURABLE_OBJECT_TABLE_NAME = "role_meta_securable_object";

  @Insert({
    "<script>",
    "INSERT INTO "
        + SECURABLE_OBJECT_TABLE_NAME
        + "(role_id, entity_id, type, privilege_names, privilege_conditions, "
        + " current_version, last_version, deleted_at)"
        + " VALUES ",
    "<foreach collection='securableObjects' item='item' separator=','>",
    "(#{item.roleId},"
        + " #{item.entityId},"
        + " #{item.type},"
        + " #{item.privilegeNames},"
        + " #{item.privilegeConditions},"
        + " #{item.currentVersion},"
        + " #{item.lastVersion},"
        + " #{item.deletedAt})",
    "</foreach>",
    "</script>"
  })
  void batchInsertSecurableObjects(
      @Param("securableObjects") List<SecurableObjectPO> securableObjectPOS);

  @Update(
      "UPDATE "
          + SECURABLE_OBJECT_TABLE_NAME
          + " SET deleted_at = UNIX_TIMESTAMP(CURRENT_TIMESTAMP(3)) * 1000.0"
          + " WHERE role_id = #{roleId} AND deleted_at = 0")
  void softDeleteSecurableObjectsByRoleId(@Param("roleId") Long roleId);

  @Select(
      "SELECT role_id as roleId, entity_id as entityId,"
          + " type as type, privilege_names as privilegeNames,"
          + " privilege_conditions as privilegeConditions, current_version as currentVersion,"
          + " last_version as lastVersion, deleted_at as deletedAt"
          + " FROM "
          + SECURABLE_OBJECT_TABLE_NAME
          + " WHERE role_id = #{roleId} AND deleted_at = 0")
  List<SecurableObjectPO> listSecurableObjectsByRoleId(@Param("roleId") Long roleId);

  @Delete(
      "DELETE FROM "
          + SECURABLE_OBJECT_TABLE_NAME
          + " WHERE deleted_at > 0 AND deleted_at < #{legacyTimeLine} LIMIT #{limit}")
  Integer deleteSecurableObjectsByLegacyTimeLine(
      @Param("legacyTimeLine") Long legacyTimeLine, @Param("limit") int limit);
}
