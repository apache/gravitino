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
import org.apache.gravitino.storage.relational.po.SecurableObjectPO;
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
  String ROLE_TABLE_NAME = "role_meta";

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
      @Param("securableObjects") List<SecurableObjectPO> securableObjectPOs);

  @Update(
      "UPDATE "
          + SECURABLE_OBJECT_TABLE_NAME
          + " SET deleted_at = (UNIX_TIMESTAMP() * 1000.0)"
          + " + EXTRACT(MICROSECOND FROM CURRENT_TIMESTAMP(3)) / 1000"
          + " WHERE role_id = #{roleId} AND deleted_at = 0")
  void softDeleteSecurableObjectsByRoleId(@Param("roleId") Long roleId);

  @Update(
      "UPDATE "
          + SECURABLE_OBJECT_TABLE_NAME
          + " ob SET ob.deleted_at = (UNIX_TIMESTAMP() * 1000.0)"
          + " + EXTRACT(MICROSECOND FROM CURRENT_TIMESTAMP(3)) / 1000"
          + " WHERE exists (SELECT * from "
          + ROLE_TABLE_NAME
          + " ro WHERE ro.metalake_id = #{metalakeId} AND ro.role_id = ob.role_id"
          + " AND ro.deleted_at = 0) AND ob.deleted_at = 0")
  void softDeleteRoleMetasByMetalakeId(@Param("metalakeId") Long metalakeId);

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
          + " WHERE deleted_at > 0 AND deleted_at < #{legacyTimeline} LIMIT #{limit}")
  Integer deleteSecurableObjectsByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit);
}
