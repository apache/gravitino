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

import static org.apache.gravitino.storage.relational.mapper.SecurableObjectMapper.ROLE_TABLE_NAME;
import static org.apache.gravitino.storage.relational.mapper.SecurableObjectMapper.SECURABLE_OBJECT_TABLE_NAME;

import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.storage.relational.JDBCBackend.JDBCBackendType;
import org.apache.gravitino.storage.relational.po.SecurableObjectPO;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.ibatis.annotations.Param;

public class SecurableObjectSQLProvider {

  private static final Map<JDBCBackendType, SecurableObjectBaseProvider>
      METALAKE_META_SQL_PROVIDER_MAP =
          ImmutableMap.of(
              JDBCBackendType.MYSQL, new SecurableObjectMySQLProvider(),
              JDBCBackendType.H2, new SecurableObjectH2Provider(),
              JDBCBackendType.PG, new SecurableObjectPostgreSQLProvider());

  public static SecurableObjectBaseProvider getProvider() {
    String databaseId =
        SqlSessionFactoryHelper.getInstance()
            .getSqlSessionFactory()
            .getConfiguration()
            .getDatabaseId();

    JDBCBackendType jdbcBackendType = JDBCBackendType.fromString(databaseId);
    return METALAKE_META_SQL_PROVIDER_MAP.get(jdbcBackendType);
  }

  static class SecurableObjectMySQLProvider extends SecurableObjectBaseProvider {}

  static class SecurableObjectH2Provider extends SecurableObjectBaseProvider {}

  static class SecurableObjectPostgreSQLProvider extends SecurableObjectBaseProvider {

    @Override
    public String softDeleteSecurableObjectsByRoleId(Long roleId) {
      return "UPDATE "
          + SECURABLE_OBJECT_TABLE_NAME
          + " SET deleted_at = floor(extract(epoch from((current_timestamp - timestamp '1970-01-01 00:00:00')*1000)))"
          + " WHERE role_id = #{roleId} AND deleted_at = 0";
    }

    @Override
    public String softDeleteRoleMetasByMetalakeId(Long metalakeId) {
      return "UPDATE "
          + SECURABLE_OBJECT_TABLE_NAME
          + " ob SET deleted_at = floor(extract(epoch from((current_timestamp - timestamp '1970-01-01 00:00:00')*1000)))"
          + " WHERE exists (SELECT * from "
          + ROLE_TABLE_NAME
          + " ro WHERE ro.metalake_id = #{metalakeId} AND ro.role_id = ob.role_id"
          + " AND ro.deleted_at = 0) AND ob.deleted_at = 0";
    }
  }

  public String batchInsertSecurableObjects(
      @Param("securableObjects") List<SecurableObjectPO> securableObjectPOs) {
    return getProvider().batchInsertSecurableObjects(securableObjectPOs);
  }

  public String softDeleteSecurableObjectsByRoleId(@Param("roleId") Long roleId) {
    return getProvider().softDeleteSecurableObjectsByRoleId(roleId);
  }

  public String softDeleteRoleMetasByMetalakeId(@Param("metalakeId") Long metalakeId) {
    return getProvider().softDeleteRoleMetasByMetalakeId(metalakeId);
  }

  public String listSecurableObjectsByRoleId(@Param("roleId") Long roleId) {
    return getProvider().listSecurableObjectsByRoleId(roleId);
  }

  public String deleteSecurableObjectsByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit) {
    return getProvider().deleteSecurableObjectsByLegacyTimeline(legacyTimeline, limit);
  }
}
