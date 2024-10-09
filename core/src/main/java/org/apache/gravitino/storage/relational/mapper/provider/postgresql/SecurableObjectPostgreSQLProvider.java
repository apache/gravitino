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
package org.apache.gravitino.storage.relational.mapper.provider.postgresql;

import static org.apache.gravitino.storage.relational.mapper.SecurableObjectMapper.ROLE_TABLE_NAME;
import static org.apache.gravitino.storage.relational.mapper.SecurableObjectMapper.SECURABLE_OBJECT_TABLE_NAME;

import java.util.List;
import org.apache.gravitino.storage.relational.mapper.provider.base.SecurableObjectBaseSQLProvider;
import org.apache.gravitino.storage.relational.po.SecurableObjectPO;
import org.apache.ibatis.annotations.Param;

public class SecurableObjectPostgreSQLProvider extends SecurableObjectBaseSQLProvider {
  public String batchSoftDeleteSecurableObjects(
      @Param("securableObjects") List<SecurableObjectPO> securableObjectPOs) {
    return "<script>"
        + "UPDATE "
        + SECURABLE_OBJECT_TABLE_NAME
        + " SET deleted_at = floor(extract(epoch from((current_timestamp -"
        + " timestamp '1970-01-01 00:00:00')*1000)))"
        + " WHERE FALSE "
        + "<foreach collection='securableObjects' item='item' separator=' '>"
        + " OR (metadata_object_id = #{item.metadataObjectId} AND"
        + " role_id = #{item.roleId} AND deleted_at = 0 AND"
        + " privilege_names = #{item.privilegeNames} AND"
        + " privilege_conditions = #{item.privilegeConditions})"
        + "</foreach>"
        + "</script>";
  }

  @Override
  public String softDeleteSecurableObjectsByRoleId(Long roleId) {
    return "UPDATE "
        + SECURABLE_OBJECT_TABLE_NAME
        + " SET deleted_at = floor(extract(epoch from((current_timestamp -"
        + " timestamp '1970-01-01 00:00:00')*1000)))"
        + " WHERE role_id = #{roleId} AND deleted_at = 0";
  }

  @Override
  public String softDeleteRoleMetasByMetalakeId(Long metalakeId) {
    return "UPDATE "
        + SECURABLE_OBJECT_TABLE_NAME
        + " ob SET deleted_at = floor(extract(epoch from((current_timestamp -"
        + " timestamp '1970-01-01 00:00:00')*1000)))"
        + " WHERE exists (SELECT * from "
        + ROLE_TABLE_NAME
        + " ro WHERE ro.metalake_id = #{metalakeId} AND ro.role_id = ob.role_id"
        + " AND ro.deleted_at = 0) AND ob.deleted_at = 0";
  }
}
