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
package org.apache.gravitino.storage.relational.mapper.provider.postgresql;

import org.apache.gravitino.storage.relational.mapper.JobTemplateMetaMapper;
import org.apache.gravitino.storage.relational.mapper.MetalakeMetaMapper;
import org.apache.gravitino.storage.relational.mapper.provider.base.JobTemplateMetaBaseSQLProvider;
import org.apache.gravitino.storage.relational.po.JobTemplatePO;
import org.apache.ibatis.annotations.Param;

public class JobTemplateMetaPostgreSQLProvider extends JobTemplateMetaBaseSQLProvider {

  @Override
  public String softDeleteJobTemplateMetaByMetalakeAndName(
      @Param("metalakeName") String metalakeName,
      @Param("jobTemplateName") String jobTemplateName) {
    return "UPDATE "
        + JobTemplateMetaMapper.TABLE_NAME
        + " SET deleted_at = floor(extract(epoch from((current_timestamp -"
        + " timestamp '1970-01-01 00:00:00')*1000))) "
        + " WHERE metalake_id IN ("
        + " SELECT metalake_id FROM "
        + MetalakeMetaMapper.TABLE_NAME
        + " WHERE metalake_name = #{metalakeName} AND deleted_at = 0)"
        + " AND job_template_name = #{jobTemplateName} AND deleted_at = 0";
  }

  @Override
  public String softDeleteJobTemplateMetasByMetalakeId(@Param("metalakeId") Long metalakeId) {
    return "UPDATE "
        + JobTemplateMetaMapper.TABLE_NAME
        + " SET deleted_at = floor(extract(epoch from((current_timestamp -"
        + " timestamp '1970-01-01 00:00:00')*1000))) "
        + " WHERE metalake_id = #{metalakeId} AND deleted_at = 0";
  }

  @Override
  public String insertJobTemplateMetaOnDuplicateKeyUpdate(
      @Param("jobTemplateMeta") JobTemplatePO jobTemplatePO) {
    return "INSERT INTO "
        + JobTemplateMetaMapper.TABLE_NAME
        + " (job_template_id, job_template_name, metalake_id, job_template_comment, "
        + " job_template_content, audit_info, current_version, last_version, deleted_at)"
        + " VALUES (#{jobTemplateMeta.jobTemplateId}, #{jobTemplateMeta.jobTemplateName},"
        + " #{jobTemplateMeta.metalakeId}, #{jobTemplateMeta.jobTemplateComment},"
        + " #{jobTemplateMeta.jobTemplateContent}, #{jobTemplateMeta.auditInfo},"
        + " #{jobTemplateMeta.currentVersion}, #{jobTemplateMeta.lastVersion},"
        + " #{jobTemplateMeta.deletedAt})"
        + " ON CONFLICT(job_template_id) DO UPDATE SET"
        + " job_template_name = #{jobTemplateMeta.jobTemplateName},"
        + " metalake_id = #{jobTemplateMeta.metalakeId},"
        + " job_template_comment = #{jobTemplateMeta.jobTemplateComment},"
        + " job_template_content = #{jobTemplateMeta.jobTemplateContent},"
        + " audit_info = #{jobTemplateMeta.auditInfo},"
        + " current_version = #{jobTemplateMeta.currentVersion},"
        + " last_version = #{jobTemplateMeta.lastVersion},"
        + " deleted_at = #{jobTemplateMeta.deletedAt}";
  }

  @Override
  public String deleteJobTemplateMetasByLegacyTimeline(Long legacyTimeline, int limit) {
    return "DELETE FROM "
        + JobTemplateMetaMapper.TABLE_NAME
        + " WHERE job_template_id IN (SELECT job_template_id FROM "
        + JobTemplateMetaMapper.TABLE_NAME
        + " WHERE deleted_at < #{legacyTimeline} AND deleted_at > 0 LIMIT #{limit})";
  }
}
