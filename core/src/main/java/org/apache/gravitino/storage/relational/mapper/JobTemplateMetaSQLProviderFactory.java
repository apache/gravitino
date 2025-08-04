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

  static class JobTemplateMetaH2Provider extends JobTemplateMetaBaseSQLProvider {}

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

  public static String softDeleteJobTemplateMetaByMetalakeAndName(
      @Param("metalakeName") String metalakeName,
      @Param("jobTemplateName") String jobTemplateName) {
    return getProvider().softDeleteJobTemplateMetaByMetalakeAndName(metalakeName, jobTemplateName);
  }

  public static String softDeleteJobTemplateMetasByMetalakeId(
      @Param("metalakeId") Long metalakeId) {
    return getProvider().softDeleteJobTemplateMetasByMetalakeId(metalakeId);
  }

  public static String deleteJobTemplateMetasByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit) {
    return getProvider().deleteJobTemplateMetasByLegacyTimeline(legacyTimeline, limit);
  }
}
