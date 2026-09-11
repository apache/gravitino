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
import org.apache.gravitino.storage.relational.JDBCBackend;
import org.apache.gravitino.storage.relational.mapper.provider.base.JobTemplateMetaBaseSQLProvider;
import org.apache.gravitino.storage.relational.mapper.provider.postgresql.JobTemplateMetaPostgreSQLProvider;
import org.apache.gravitino.storage.relational.po.JobTemplatePO;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.ibatis.annotations.Param;

public class JobTemplateMetaSQLProviderFactory {

  private static final Map<JDBCBackend.JDBCBackendType, JobTemplateMetaBaseSQLProvider>
      JOB_TEMPLATE_META_SQL_PROVIDER_MAP =
          ImmutableMap.of(
              JDBCBackend.JDBCBackendType.MYSQL, new JobTemplateMetaMySQLProvider(),
              JDBCBackend.JDBCBackendType.H2, new JobTemplateMetaH2Provider(),
              JDBCBackend.JDBCBackendType.POSTGRESQL, new JobTemplateMetaPostgreSQLProvider());

  public static JobTemplateMetaBaseSQLProvider getProvider() {
    String databaseId =
        SqlSessionFactoryHelper.getInstance()
            .getSqlSessionFactory()
            .getConfiguration()
            .getDatabaseId();

    JDBCBackend.JDBCBackendType jdbcBackendType =
        JDBCBackend.JDBCBackendType.fromString(databaseId);
    return JOB_TEMPLATE_META_SQL_PROVIDER_MAP.get(jdbcBackendType);
  }

  static class JobTemplateMetaMySQLProvider extends JobTemplateMetaBaseSQLProvider {}

  static class JobTemplateMetaH2Provider extends JobTemplateMetaBaseSQLProvider {
    @Override
    public String selectJobTemplateByIdForShare(Long jobTemplateId) {
      return selectJobTemplateByIdForUpdate(jobTemplateId);
    }
  }

  public static String insertJobTemplateMeta(
      @Param("jobTemplateMeta") JobTemplatePO jobTemplatePO) {
    return getProvider().insertJobTemplateMeta(jobTemplatePO);
  }

  public static String insertJobTemplateMetaOnDuplicateKeyUpdate(
      @Param("jobTemplateMeta") JobTemplatePO jobTemplatePO) {
    return getProvider().insertJobTemplateMetaOnDuplicateKeyUpdate(jobTemplatePO);
  }

  public static String listJobTemplatePOsByMetalake(@Param("metalakeName") String metalakeName) {
    return getProvider().listJobTemplatePOsByMetalake(metalakeName);
  }

  public static String selectJobTemplatePOByMetalakeAndName(
      @Param("metalakeName") String metalakeName,
      @Param("jobTemplateName") String jobTemplateName) {
    return getProvider().selectJobTemplatePOByMetalakeAndName(metalakeName, jobTemplateName);
  }

  public static String softDeleteJobTemplateMetasByMetalakeId(
      @Param("metalakeId") Long metalakeId) {
    return getProvider().softDeleteJobTemplateMetasByMetalakeId(metalakeId);
  }

  public static String deleteJobTemplateMetasByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit) {
    return getProvider().deleteJobTemplateMetasByLegacyTimeline(legacyTimeline, limit);
  }

  public static String updateJobTemplateMeta(
      @Param("newJobTemplateMeta") JobTemplatePO newJobTemplatePO,
      @Param("oldJobTemplateMeta") JobTemplatePO oldJobTemplatePO) {
    return getProvider().updateJobTemplateMeta(newJobTemplatePO, oldJobTemplatePO);
  }

  public static String selectJobTemplateIdByMetalakeAndName(
      @Param("metalakeId") Long metalakeId, @Param("jobTemplateName") String jobTemplateName) {
    return getProvider().selectJobTemplateIdByMetalakeAndName(metalakeId, jobTemplateName);
  }

  public static String selectJobTemplateById(@Param("jobTemplateId") Long jobTemplateId) {
    return getProvider().selectJobTemplateById(jobTemplateId);
  }

  public static String listJobTemplatePOsByJobTemplateIds(
      @Param("jobTemplateIds") List<Long> jobTemplateIds) {
    return getProvider().listJobTemplatePOsByJobTemplateIds(jobTemplateIds);
  }

  public static String batchSelectJobTemplateByIdentifier(
      @Param("metalakeName") String metalakeName,
      @Param("jobTemplateNames") List<String> jobTemplateNames) {
    return getProvider().batchSelectJobTemplateByIdentifier(metalakeName, jobTemplateNames);
  }
  /**
   * Locks the active row for OCC identity validation.
   *
   * @param jobTemplateId the stable template ID
   * @return the SQL statement
   */
  public static String selectJobTemplateByIdForUpdate(@Param("jobTemplateId") Long jobTemplateId) {
    return getProvider().selectJobTemplateByIdForUpdate(jobTemplateId);
  }

  /**
   * Locks the active row for OCC identity validation.
   *
   * @param jobTemplateId the stable template ID
   * @return the SQL statement
   */
  public static String selectJobTemplateByIdForShare(@Param("jobTemplateId") Long jobTemplateId) {
    return getProvider().selectJobTemplateByIdForShare(jobTemplateId);
  }

  /**
   * Deletes active metadata using a stable identity and expected version.
   *
   * @param jobTemplateId the stable template ID
   * @param currentVersion the expected OCC version
   * @return the SQL statement
   */
  public static String softDeleteJobTemplateById(
      @Param("jobTemplateId") Long jobTemplateId, @Param("currentVersion") Long currentVersion) {
    return getProvider().softDeleteJobTemplateById(jobTemplateId, currentVersion);
  }
}
