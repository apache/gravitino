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
import org.apache.gravitino.storage.relational.mapper.provider.base.PolicyVersionBaseSQLProvider;
import org.apache.gravitino.storage.relational.mapper.provider.postgresql.PolicyVersionPostgreSQLProvider;
import org.apache.gravitino.storage.relational.po.PolicyVersionPO;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.ibatis.annotations.Param;

public class PolicyVersionSQLProviderFactory {

  private static final Map<JDBCBackendType, PolicyVersionBaseSQLProvider>
      POLICY_VERSION_SQL_PROVIDER_MAP =
          ImmutableMap.of(
              JDBCBackendType.MYSQL, new PolicyVersionMySQLProvider(),
              JDBCBackendType.H2, new PolicyVersionH2Provider(),
              JDBCBackendType.POSTGRESQL, new PolicyVersionPostgreSQLProvider());

  public static PolicyVersionBaseSQLProvider getProvider() {
    String databaseId =
        SqlSessionFactoryHelper.getInstance()
            .getSqlSessionFactory()
            .getConfiguration()
            .getDatabaseId();

    JDBCBackendType jdbcBackendType = JDBCBackendType.fromString(databaseId);
    return POLICY_VERSION_SQL_PROVIDER_MAP.get(jdbcBackendType);
  }

  public static String insertPolicyVersionOnDuplicateKeyUpdate(
      @Param("policyVersion") PolicyVersionPO policyVersionPO) {
    return getProvider().insertPolicyVersionOnDuplicateKeyUpdate(policyVersionPO);
  }

  public static String insertPolicyVersion(
      @Param("policyVersion") PolicyVersionPO policyVersionPO) {
    return getProvider().insertPolicyVersion(policyVersionPO);
  }

  public static String softDeletePolicyVersionByMetalakeAndPolicyName(
      @Param("metalakeName") String metalakeName, @Param("policyName") String policyName) {
    return getProvider().softDeletePolicyVersionByMetalakeAndPolicyName(metalakeName, policyName);
  }

  public static String deletePolicyVersionsByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit) {
    return getProvider().deletePolicyVersionsByLegacyTimeline(legacyTimeline, limit);
  }

  public static String selectPolicyVersionsByRetentionCount(
      @Param("versionRetentionCount") Long versionRetentionCount) {
    return getProvider().selectPolicyVersionsByRetentionCount(versionRetentionCount);
  }

  public static String softDeletePolicyVersionsByRetentionLine(
      @Param("policyId") Long policyId,
      @Param("versionRetentionLine") long versionRetentionLine,
      @Param("limit") int limit) {
    return getProvider()
        .softDeletePolicyVersionsByRetentionLine(policyId, versionRetentionLine, limit);
  }

  public static String softDeletePolicyVersionsByMetalakeId(@Param("metalakeId") Long metalakeId) {
    return getProvider().softDeletePolicyVersionsByMetalakeId(metalakeId);
  }

  static class PolicyVersionMySQLProvider extends PolicyVersionBaseSQLProvider {}

  static class PolicyVersionH2Provider extends PolicyVersionBaseSQLProvider {}
}
