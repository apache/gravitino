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

/** Selects the policy-to-tag relation SQL provider for the active JDBC backend. */
public class PolicyTagRelSQLProviderFactory {

  private static final Map<JDBCBackendType, PolicyTagRelBaseSQLProvider> PROVIDERS =
      ImmutableMap.of(
          JDBCBackendType.MYSQL, new PolicyTagRelMySQLProvider(),
          JDBCBackendType.H2, new PolicyTagRelH2Provider(),
          JDBCBackendType.POSTGRESQL, new PolicyTagRelPostgreSQLProvider());

  /**
   * @return The SQL provider for the active backend.
   */
  public static PolicyTagRelBaseSQLProvider getProvider() {
    String databaseId =
        SqlSessionFactoryHelper.getInstance()
            .getSqlSessionFactory()
            .getConfiguration()
            .getDatabaseId();
    return PROVIDERS.get(JDBCBackendType.fromString(databaseId));
  }

  /** Delegates a tag-anchored list query. */
  public static String listByTagNames(
      @Param("metalakeName") String metalakeName, @Param("tagNames") List<String> tagNames) {
    return getProvider().listByTagNames(metalakeName, tagNames);
  }

  /** Delegates a policy-anchored list query. */
  public static String listByPolicyNames(
      @Param("metalakeName") String metalakeName, @Param("policyNames") List<String> policyNames) {
    return getProvider().listByPolicyNames(metalakeName, policyNames);
  }

  /** Delegates a single relation query. */
  public static String getByPolicyIdAndTagId(
      @Param("policyId") Long policyId, @Param("tagId") Long tagId) {
    return getProvider().getByPolicyIdAndTagId(policyId, tagId);
  }

  /** Delegates an insert-if-absent operation. */
  public static String insertIfAbsent(@Param("relation") PolicyTagRelPO relation) {
    return getProvider().insertIfAbsent(relation);
  }

  /** Delegates a relation soft delete. */
  public static String softDeleteByIdAndVersion(@Param("relation") PolicyTagRelPO relation) {
    return getProvider().softDeleteByIdAndVersion(relation);
  }

  /** Delegates metalake deletion cleanup. */
  public static String softDeleteByMetalakeId(@Param("metalakeId") Long metalakeId) {
    return getProvider().softDeleteByMetalakeId(metalakeId);
  }

  /** Delegates tag deletion cleanup. */
  public static String softDeleteByTagId(@Param("tagId") Long tagId) {
    return getProvider().softDeleteByTagId(tagId);
  }

  /** Delegates expired relation cleanup. */
  public static String deleteByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit) {
    return getProvider().deleteByLegacyTimeline(legacyTimeline, limit);
  }

  static class PolicyTagRelMySQLProvider extends PolicyTagRelBaseSQLProvider {}

  static class PolicyTagRelH2Provider extends PolicyTagRelBaseSQLProvider {}
}
