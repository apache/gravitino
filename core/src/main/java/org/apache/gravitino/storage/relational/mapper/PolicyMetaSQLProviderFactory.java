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
import org.apache.gravitino.storage.relational.mapper.provider.base.PolicyMetaBaseSQLProvider;
import org.apache.gravitino.storage.relational.mapper.provider.postgresql.PolicyMetaPostgreSQLProvider;
import org.apache.gravitino.storage.relational.po.PolicyPO;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.ibatis.annotations.Param;

public class PolicyMetaSQLProviderFactory {

  private static final Map<JDBCBackendType, PolicyMetaBaseSQLProvider>
      POLICY_META_SQL_PROVIDER_MAP =
          ImmutableMap.of(
              JDBCBackendType.MYSQL, new PolicyMetaMySQLProvider(),
              JDBCBackendType.H2, new PolicyMetaH2Provider(),
              JDBCBackendType.POSTGRESQL, new PolicyMetaPostgreSQLProvider());

  public static PolicyMetaBaseSQLProvider getProvider() {
    String databaseId =
        SqlSessionFactoryHelper.getInstance()
            .getSqlSessionFactory()
            .getConfiguration()
            .getDatabaseId();

    JDBCBackendType jdbcBackendType = JDBCBackendType.fromString(databaseId);
    return POLICY_META_SQL_PROVIDER_MAP.get(jdbcBackendType);
  }

  public static String listPolicyPOsByMetalake(@Param("metalakeName") String metalakeName) {
    return getProvider().listPolicyPOsByMetalake(metalakeName);
  }

  public static String listPolicyPOsByMetalakeAndPolicyNames(
      @Param("metalakeName") String metalakeName, @Param("policyNames") List<String> policyNames) {
    return getProvider().listPolicyPOsByMetalakeAndPolicyNames(metalakeName, policyNames);
  }

  public static String insertPolicyMetaOnDuplicateKeyUpdate(
      @Param("policyMeta") PolicyPO policyPO) {
    return getProvider().insertPolicyMetaOnDuplicateKeyUpdate(policyPO);
  }

  public static String insertPolicyMeta(@Param("policyMeta") PolicyPO policyPO) {
    return getProvider().insertPolicyMeta(policyPO);
  }

  public static String selectPolicyMetaByMetalakeAndName(
      @Param("metalakeName") String metalakeName, @Param("policyName") String policyName) {
    return getProvider().selectPolicyMetaByMetalakeAndName(metalakeName, policyName);
  }

  public static String updatePolicyMeta(
      @Param("newPolicyMeta") PolicyPO newPolicyMeta,
      @Param("oldPolicyMeta") PolicyPO oldPolicyMeta) {
    return getProvider().updatePolicyMeta(newPolicyMeta, oldPolicyMeta);
  }

  public static String softDeletePolicyByMetalakeAndPolicyName(
      @Param("metalakeName") String metalakeName, @Param("policyName") String policyName) {
    return getProvider().softDeletePolicyByMetalakeAndPolicyName(metalakeName, policyName);
  }

  public static String deletePolicyMetasByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit) {
    return getProvider().deletePolicyMetasByLegacyTimeline(legacyTimeline, limit);
  }

  public static String softDeletePolicyMetasByMetalakeId(@Param("metalakeId") Long metalakeId) {
    return getProvider().softDeletePolicyMetasByMetalakeId(metalakeId);
  }

  static class PolicyMetaMySQLProvider extends PolicyMetaBaseSQLProvider {}

  static class PolicyMetaH2Provider extends PolicyMetaBaseSQLProvider {}
}
