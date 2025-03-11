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

import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.storage.relational.JDBCBackend.JDBCBackendType;
import org.apache.gravitino.storage.relational.mapper.provider.base.SecurableObjectBaseSQLProvider;
import org.apache.gravitino.storage.relational.mapper.provider.postgresql.SecurableObjectPostgreSQLProvider;
import org.apache.gravitino.storage.relational.po.SecurableObjectPO;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.ibatis.annotations.Param;

public class SecurableObjectSQLProviderFactory {

  private static final Map<JDBCBackendType, SecurableObjectBaseSQLProvider>
      SECURABLE_OBJECTS_SQL_PROVIDER_MAP =
          ImmutableMap.of(
              JDBCBackendType.MYSQL, new SecurableObjectMySQLProvider(),
              JDBCBackendType.H2, new SecurableObjectH2Provider(),
              JDBCBackendType.POSTGRESQL, new SecurableObjectPostgreSQLProvider());

  public static SecurableObjectBaseSQLProvider getProvider() {
    String databaseId =
        SqlSessionFactoryHelper.getInstance()
            .getSqlSessionFactory()
            .getConfiguration()
            .getDatabaseId();

    JDBCBackendType jdbcBackendType = JDBCBackendType.fromString(databaseId);
    return SECURABLE_OBJECTS_SQL_PROVIDER_MAP.get(jdbcBackendType);
  }

  static class SecurableObjectMySQLProvider extends SecurableObjectBaseSQLProvider {}

  static class SecurableObjectH2Provider extends SecurableObjectBaseSQLProvider {}

  public static String batchInsertSecurableObjects(
      @Param("securableObjects") List<SecurableObjectPO> securableObjectPOs) {
    return getProvider().batchInsertSecurableObjects(securableObjectPOs);
  }

  public static String batchSoftDeleteSecurableObjects(
      @Param("securableObjects") List<SecurableObjectPO> securableObjectPOs) {
    return getProvider().batchSoftDeleteSecurableObjects(securableObjectPOs);
  }

  public static String softDeleteSecurableObjectsByRoleId(@Param("roleId") Long roleId) {
    return getProvider().softDeleteSecurableObjectsByRoleId(roleId);
  }

  public static String softDeleteSecurableObjectsByMetalakeId(
      @Param("metalakeId") Long metalakeId) {
    return getProvider().softDeleteSecurableObjectsByMetalakeId(metalakeId);
  }

  public static String softDeleteObjectRelsByMetadataObject(
      @Param("metadataObjectId") Long metadataObjectId,
      @Param("metadataObjectType") String metadataObjectType) {
    return getProvider().softDeleteObjectRelsByMetadataObject(metadataObjectId, metadataObjectType);
  }

  public static String softDeleteObjectRelsByCatalogId(@Param("catalogId") Long catalogId) {
    return getProvider().softDeleteObjectRelsByCatalogId(catalogId);
  }

  public static String softDeleteObjectRelsBySchemaId(@Param("schemaId") Long schemaId) {
    return getProvider().softDeleteObjectRelsBySchemaId(schemaId);
  }

  public static String listSecurableObjectsByRoleId(@Param("roleId") Long roleId) {
    return getProvider().listSecurableObjectsByRoleId(roleId);
  }

  public static String deleteSecurableObjectsByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit) {
    return getProvider().deleteSecurableObjectsByLegacyTimeline(legacyTimeline, limit);
  }
}
