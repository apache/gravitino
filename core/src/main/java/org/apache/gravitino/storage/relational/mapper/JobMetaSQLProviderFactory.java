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
import org.apache.gravitino.storage.relational.mapper.provider.base.JobMetaBaseSQLProvider;
import org.apache.gravitino.storage.relational.mapper.provider.postgresql.JobMetaPostgreSQLProvider;
import org.apache.gravitino.storage.relational.po.JobPO;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.ibatis.annotations.Param;

public class JobMetaSQLProviderFactory {

  private static final Map<JDBCBackend.JDBCBackendType, JobMetaBaseSQLProvider>
      JOB_META_SQL_PROVIDER_MAP =
          ImmutableMap.of(
              JDBCBackend.JDBCBackendType.MYSQL, new JobMetaMySQLProvider(),
              JDBCBackend.JDBCBackendType.H2, new JobMetaH2Provider(),
              JDBCBackend.JDBCBackendType.POSTGRESQL, new JobMetaPostgreSQLProvider());

  public static JobMetaBaseSQLProvider getProvider() {
    String databaseId =
        SqlSessionFactoryHelper.getInstance()
            .getSqlSessionFactory()
            .getConfiguration()
            .getDatabaseId();

    JDBCBackend.JDBCBackendType jdbcBackendType =
        JDBCBackend.JDBCBackendType.fromString(databaseId);
    return JOB_META_SQL_PROVIDER_MAP.get(jdbcBackendType);
  }

  static class JobMetaMySQLProvider extends JobMetaBaseSQLProvider {}

  static class JobMetaH2Provider extends JobMetaBaseSQLProvider {}

  public static String insertJobMeta(@Param("jobMeta") JobPO jobPO) {
    return getProvider().insertJobMeta(jobPO);
  }

  public static String insertJobMetaOnDuplicateKeyUpdate(@Param("jobMeta") JobPO jobPO) {
    return getProvider().insertJobMetaOnDuplicateKeyUpdate(jobPO);
  }

  public static String listJobPOsByMetalake(@Param("metalakeName") String metalakeName) {
    return getProvider().listJobPOsByMetalake(metalakeName);
  }

  public static String listJobPOsByMetalakeAndTemplate(
      @Param("metalakeName") String metalakeName,
      @Param("jobTemplateName") String jobTemplateName) {
    return getProvider().listJobPOsByMetalakeAndTemplate(metalakeName, jobTemplateName);
  }

  public static String selectJobPOByMetalakeAndRunId(
      @Param("metalakeName") String metalakeName, @Param("jobRunId") Long jobRunId) {
    return getProvider().selectJobPOByMetalakeAndRunId(metalakeName, jobRunId);
  }

  public static String updateJobMeta(
      @Param("newJobMeta") JobPO newJobPO, @Param("oldJobMeta") JobPO oldJobPO) {
    return getProvider().updateJobMeta(newJobPO, oldJobPO);
  }

  public static String softDeleteJobMetasByMetalakeId(@Param("metalakeId") Long metalakeId) {
    return getProvider().softDeleteJobMetasByMetalakeId(metalakeId);
  }

  public static String deleteJobMetasByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit) {
    return getProvider().deleteJobMetasByLegacyTimeline(legacyTimeline, limit);
  }

  public static String softDeleteJobMetasByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline) {
    return getProvider().softDeleteJobMetasByLegacyTimeline(legacyTimeline);
  }

  public static String batchSelectJobByRunIds(
      @Param("metalakeName") String metalakeName, @Param("jobRunIds") List<Long> jobRunIds) {
    return getProvider().batchSelectJobByRunIds(metalakeName, jobRunIds);
  }

  /**
   * Locks the active row for OCC identity validation.
   *
   * @param jobRunId the stable job run ID
   * @param metalakeId the owning metalake ID
   * @return the SQL statement
   */
  public static String selectJobRunIdForUpdate(
      @Param("jobRunId") Long jobRunId, @Param("metalakeId") Long metalakeId) {
    return getProvider().selectJobRunIdForUpdate(jobRunId, metalakeId);
  }

  /**
   * Deletes active metadata using a stable identity and expected version.
   *
   * @param jobRunId the stable job run ID
   * @param currentVersion the expected OCC version
   * @return the SQL statement
   */
  public static String softDeleteJobByRunIdWithVersion(
      @Param("jobRunId") Long jobRunId, @Param("currentVersion") Long currentVersion) {
    return getProvider().softDeleteJobByRunIdWithVersion(jobRunId, currentVersion);
  }

  /**
   * Deletes active metadata using a stable identity.
   *
   * @param jobTemplateId the stable template ID
   * @return the SQL statement
   */
  public static String softDeleteJobsByTemplateId(@Param("jobTemplateId") Long jobTemplateId) {
    return getProvider().softDeleteJobsByTemplateId(jobTemplateId);
  }

  /**
   * Builds a locking lookup for a nonterminal job belonging to a template.
   *
   * @param jobTemplateId the stable template ID
   * @return the SQL statement
   */
  public static String selectNonterminalJobForUpdate(@Param("jobTemplateId") Long jobTemplateId) {
    return getProvider().selectNonterminalJobForUpdate(jobTemplateId);
  }
}
