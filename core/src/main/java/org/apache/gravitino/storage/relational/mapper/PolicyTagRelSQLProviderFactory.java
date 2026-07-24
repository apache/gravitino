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
import org.apache.gravitino.storage.relational.mapper.provider.base.PolicyTagRelBaseSQLProvider;
import org.apache.gravitino.storage.relational.mapper.provider.postgresql.PolicyTagRelPostgreSQLProvider;
import org.apache.gravitino.storage.relational.po.PolicyTagRelPO;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.ibatis.annotations.Param;

/** Factory for SQL providers used by {@link PolicyTagRelMapper}. */
public class PolicyTagRelSQLProviderFactory {

  private static final Map<JDBCBackendType, PolicyTagRelBaseSQLProvider>
      POLICY_TAG_RELATION_SQL_PROVIDER_MAP =
          ImmutableMap.of(
              JDBCBackendType.MYSQL, new PolicyTagRelMySQLProvider(),
              JDBCBackendType.H2, new PolicyTagRelH2Provider(),
              JDBCBackendType.POSTGRESQL, new PolicyTagRelPostgreSQLProvider());

  public static PolicyTagRelBaseSQLProvider getProvider() {
    String databaseId =
        SqlSessionFactoryHelper.getInstance()
            .getSqlSessionFactory()
            .getConfiguration()
            .getDatabaseId();

    JDBCBackendType jdbcBackendType = JDBCBackendType.fromString(databaseId);
    return POLICY_TAG_RELATION_SQL_PROVIDER_MAP.get(jdbcBackendType);
  }

  static class PolicyTagRelMySQLProvider extends PolicyTagRelBaseSQLProvider {}

  static class PolicyTagRelH2Provider extends PolicyTagRelBaseSQLProvider {}

  public static String listPolicyPOsByTagId(@Param("tagId") Long tagId) {
    return getProvider().listPolicyPOsByTagId(tagId);
  }

  public static String getPolicyPOByTagIdAndPolicyName(
      @Param("tagId") Long tagId, @Param("policyName") String policyName) {
    return getProvider().getPolicyPOByTagIdAndPolicyName(tagId, policyName);
  }

  public static String listTagPOsByPolicyId(@Param("policyId") Long policyId) {
    return getProvider().listTagPOsByPolicyId(policyId);
  }

  public static String batchInsertPolicyTagRels(
      @Param("policyTagRels") List<PolicyTagRelPO> policyTagRelPOs) {
    return getProvider().batchInsertPolicyTagRels(policyTagRelPOs);
  }

  public static String batchDeletePolicyTagRelsByPolicyIdsAndTagId(
      @Param("tagId") Long tagId, @Param("policyIds") List<Long> policyIds) {
    return getProvider().batchDeletePolicyTagRelsByPolicyIdsAndTagId(tagId, policyIds);
  }

  public static String softDeletePolicyTagRelsByMetalakeAndPolicyName(
      @Param("metalakeName") String metalakeName, @Param("policyName") String policyName) {
    return getProvider().softDeletePolicyTagRelsByMetalakeAndPolicyName(metalakeName, policyName);
  }

  public static String softDeletePolicyTagRelsByMetalakeAndTagName(
      @Param("metalakeName") String metalakeName, @Param("tagName") String tagName) {
    return getProvider().softDeletePolicyTagRelsByMetalakeAndTagName(metalakeName, tagName);
  }

  public static String softDeletePolicyTagRelsByMetalakeId(@Param("metalakeId") Long metalakeId) {
    return getProvider().softDeletePolicyTagRelsByMetalakeId(metalakeId);
  }

  public static String deletePolicyTagRelsByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit) {
    return getProvider().deletePolicyTagRelsByLegacyTimeline(legacyTimeline, limit);
  }
}
