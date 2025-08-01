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
import org.apache.gravitino.storage.relational.mapper.provider.base.PolicyMetadataObjectRelBaseSQLProvider;
import org.apache.gravitino.storage.relational.mapper.provider.postgresql.PolicyMetadataObjectRelPostgreSQLProvider;
import org.apache.gravitino.storage.relational.po.PolicyMetadataObjectRelPO;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.ibatis.annotations.Param;

public class PolicyMetadataObjectRelSQLProviderFactory {

  private static final Map<JDBCBackendType, PolicyMetadataObjectRelBaseSQLProvider>
      POLICY_METADATA_OBJECT_RELATION_SQL_PROVIDER_MAP =
          ImmutableMap.of(
              JDBCBackendType.MYSQL, new PolicyMetadataObjectRelMySQLProvider(),
              JDBCBackendType.H2, new PolicyMetadataObjectRelH2Provider(),
              JDBCBackendType.POSTGRESQL, new PolicyMetadataObjectRelPostgreSQLProvider());

  public static PolicyMetadataObjectRelBaseSQLProvider getProvider() {
    String databaseId =
        SqlSessionFactoryHelper.getInstance()
            .getSqlSessionFactory()
            .getConfiguration()
            .getDatabaseId();

    JDBCBackendType jdbcBackendType = JDBCBackendType.fromString(databaseId);
    return POLICY_METADATA_OBJECT_RELATION_SQL_PROVIDER_MAP.get(jdbcBackendType);
  }

  static class PolicyMetadataObjectRelMySQLProvider
      extends PolicyMetadataObjectRelBaseSQLProvider {}

  static class PolicyMetadataObjectRelH2Provider extends PolicyMetadataObjectRelBaseSQLProvider {}

  public static String listPolicyPOsByMetadataObjectIdAndType(
      @Param("metadataObjectId") Long metadataObjectId,
      @Param("metadataObjectType") String metadataObjectType) {
    return getProvider()
        .listPolicyPOsByMetadataObjectIdAndType(metadataObjectId, metadataObjectType);
  }

  public static String getPolicyPOsByMetadataObjectAndPolicyName(
      @Param("metadataObjectId") Long metadataObjectId,
      @Param("metadataObjectType") String metadataObjectType,
      @Param("policyName") String policyName) {
    return getProvider()
        .getPolicyPOsByMetadataObjectAndPolicyName(
            metadataObjectId, metadataObjectType, policyName);
  }

  public static String listPolicyMetadataObjectRelsByMetalakeAndPolicyName(
      @Param("metalakeName") String metalakeName, @Param("policyName") String policyName) {
    return getProvider()
        .listPolicyMetadataObjectRelsByMetalakeAndPolicyName(metalakeName, policyName);
  }

  public static String batchInsertPolicyMetadataObjectRels(
      @Param("policyRels") List<PolicyMetadataObjectRelPO> policyRelPOs) {
    return getProvider().batchInsertPolicyMetadataObjectRels(policyRelPOs);
  }

  public static String batchDeletePolicyMetadataObjectRelsByPolicyIdsAndMetadataObject(
      @Param("metadataObjectId") Long metadataObjectId,
      @Param("metadataObjectType") String metadataObjectType,
      @Param("policyIds") List<Long> policyIds) {
    return getProvider()
        .batchDeletePolicyMetadataObjectRelsByPolicyIdsAndMetadataObject(
            metadataObjectId, metadataObjectType, policyIds);
  }

  public static String softDeletePolicyMetadataObjectRelsByMetalakeAndPolicyName(
      @Param("metalakeName") String metalakeName, @Param("policyName") String policyName) {
    return getProvider()
        .softDeletePolicyMetadataObjectRelsByMetalakeAndPolicyName(metalakeName, policyName);
  }

  public static String softDeletePolicyMetadataObjectRelsByMetalakeId(
      @Param("metalakeId") Long metalakeId) {
    return getProvider().softDeletePolicyMetadataObjectRelsByMetalakeId(metalakeId);
  }

  public static String softDeletePolicyMetadataObjectRelsByMetadataObject(
      @Param("metadataObjectId") Long metadataObjectId,
      @Param("metadataObjectType") String metadataObjectType) {
    return getProvider()
        .softDeletePolicyMetadataObjectRelsByMetadataObject(metadataObjectId, metadataObjectType);
  }

  public static String softDeletePolicyMetadataObjectRelsByCatalogId(
      @Param("catalogId") Long catalogId) {
    return getProvider().softDeletePolicyMetadataObjectRelsByCatalogId(catalogId);
  }

  public static String softDeletePolicyMetadataObjectRelsBySchemaId(
      @Param("schemaId") Long schemaId) {
    return getProvider().softDeletePolicyMetadataObjectRelsBySchemaId(schemaId);
  }

  public static String softDeletePolicyMetadataObjectRelsByTableId(@Param("tableId") Long tableId) {
    return getProvider().softDeletePolicyMetadataObjectRelsByTableId(tableId);
  }

  public static String deletePolicyEntityRelsByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit) {
    return getProvider().deletePolicyEntityRelsByLegacyTimeline(legacyTimeline, limit);
  }
}
