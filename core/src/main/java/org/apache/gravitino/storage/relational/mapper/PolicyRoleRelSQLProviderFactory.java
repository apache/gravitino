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
import java.util.Map;
import org.apache.gravitino.storage.relational.JDBCBackend.JDBCBackendType;
import org.apache.gravitino.storage.relational.mapper.provider.base.PolicyRoleRelBaseSQLProvider;
import org.apache.gravitino.storage.relational.mapper.provider.postgresql.PolicyRoleRelPostgreSQLProvider;
import org.apache.gravitino.storage.relational.po.PolicyRoleRelPO;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.ibatis.annotations.Param;

/** Selects the policy-to-role reference SQL provider for the active JDBC backend. */
public class PolicyRoleRelSQLProviderFactory {

  private static final Map<JDBCBackendType, PolicyRoleRelBaseSQLProvider> PROVIDERS =
      ImmutableMap.of(
          JDBCBackendType.MYSQL, new PolicyRoleRelMySQLProvider(),
          JDBCBackendType.H2, new PolicyRoleRelH2Provider(),
          JDBCBackendType.POSTGRESQL, new PolicyRoleRelPostgreSQLProvider());

  /**
   * @return The SQL provider for the active backend.
   */
  public static PolicyRoleRelBaseSQLProvider getProvider() {
    String databaseId =
        SqlSessionFactoryHelper.getInstance()
            .getSqlSessionFactory()
            .getConfiguration()
            .getDatabaseId();
    return PROVIDERS.get(JDBCBackendType.fromString(databaseId));
  }

  /** Delegates a policy-anchored list query. */
  public static String listByPolicyId(@Param("policyId") Long policyId) {
    return getProvider().listByPolicyId(policyId);
  }

  /** Delegates a role-anchored list query. */
  public static String listByRoleName(
      @Param("metalakeName") String metalakeName, @Param("roleName") String roleName) {
    return getProvider().listByRoleName(metalakeName, roleName);
  }

  /** Delegates an insert operation. */
  public static String insert(@Param("relation") PolicyRoleRelPO relation) {
    return getProvider().insert(relation);
  }

  /** Delegates a reference soft delete. */
  public static String softDeleteByIdAndVersion(@Param("relation") PolicyRoleRelPO relation) {
    return getProvider().softDeleteByIdAndVersion(relation);
  }

  /** Delegates policy deletion cleanup. */
  public static String softDeleteByPolicyId(@Param("policyId") Long policyId) {
    return getProvider().softDeleteByPolicyId(policyId);
  }

  /** Delegates metalake deletion cleanup. */
  public static String softDeleteByMetalakeId(@Param("metalakeId") Long metalakeId) {
    return getProvider().softDeleteByMetalakeId(metalakeId);
  }

  /** Delegates expired reference cleanup. */
  public static String deleteByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit) {
    return getProvider().deleteByLegacyTimeline(legacyTimeline, limit);
  }

  static class PolicyRoleRelMySQLProvider extends PolicyRoleRelBaseSQLProvider {}

  static class PolicyRoleRelH2Provider extends PolicyRoleRelBaseSQLProvider {}
}
