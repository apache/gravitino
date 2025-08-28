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

import org.apache.gravitino.storage.relational.mapper.JobMetaMapper;
import org.apache.gravitino.storage.relational.mapper.JobTemplateMetaMapper;
import org.apache.gravitino.storage.relational.mapper.MetalakeMetaMapper;
import org.apache.gravitino.storage.relational.mapper.provider.base.JobMetaBaseSQLProvider;
import org.apache.gravitino.storage.relational.po.JobPO;
import org.apache.ibatis.annotations.Param;

public class JobMetaPostgreSQLProvider extends JobMetaBaseSQLProvider {

  @Override
  public String insertJobMetaOnDuplicateKeyUpdate(@Param("jobMeta") JobPO jobPO) {
    return "INSERT INTO "
        + JobMetaMapper.TABLE_NAME
        + " (job_run_id, job_template_id, metalake_id,"
        + " job_execution_id, job_run_status, job_finished_at, audit_info, current_version,"
        + " last_version, deleted_at)"
        + " VALUES (#{jobMeta.jobRunId},"
        + " (SELECT job_template_id FROM "
        + JobTemplateMetaMapper.TABLE_NAME
        + " WHERE job_template_name = #{jobMeta.jobTemplateName}"
        + " AND metalake_id = #{jobMeta.metalakeId} AND deleted_at = 0),"
        + " #{jobMeta.metalakeId}, #{jobMeta.jobExecutionId},"
        + " #{jobMeta.jobRunStatus}, #{jobMeta.jobFinishedAt}, #{jobMeta.auditInfo},"
        + " #{jobMeta.currentVersion}, #{jobMeta.lastVersion},"
        + " #{jobMeta.deletedAt})"
        + " ON CONFLICT (job_run_id) DO UPDATE SET"
        + " job_template_id = (SELECT job_template_id FROM "
        + JobTemplateMetaMapper.TABLE_NAME
        + " WHERE job_template_name = #{jobMeta.jobTemplateName}"
        + " AND metalake_id = #{jobMeta.metalakeId} AND deleted_at = 0),"
        + " metalake_id = #{jobMeta.metalakeId},"
        + " job_execution_id = #{jobMeta.jobExecutionId},"
        + " job_run_status = #{jobMeta.jobRunStatus},"
        + " job_finished_at = #{jobMeta.jobFinishedAt},"
        + " audit_info = #{jobMeta.auditInfo},"
        + " current_version = #{jobMeta.currentVersion},"
        + " last_version = #{jobMeta.lastVersion},"
        + " deleted_at = #{jobMeta.deletedAt}";
  }

  @Override
  public String softDeleteJobMetaByMetalakeAndTemplate(
      @Param("metalakeName") String metalakeName,
      @Param("jobTemplateName") String jobTemplateName) {
    return "UPDATE "
        + JobMetaMapper.TABLE_NAME
        + " SET deleted_at = floor(extract(epoch from((current_timestamp -"
        + " timestamp '1970-01-01 00:00:00')*1000))) "
        + " WHERE metalake_id IN ("
        + " SELECT metalake_id FROM "
        + MetalakeMetaMapper.TABLE_NAME
        + " WHERE metalake_name = #{metalakeName} AND deleted_at = 0)"
        + " AND job_template_id IN ("
        + " SELECT job_template_id FROM "
        + JobTemplateMetaMapper.TABLE_NAME
        + " WHERE job_template_name = #{jobTemplateName} AND deleted_at = 0)"
        + " AND deleted_at = 0";
  }

  @Override
  public String softDeleteJobMetasByMetalakeId(@Param("metalakeId") Long metalakeId) {
    return "UPDATE "
        + JobMetaMapper.TABLE_NAME
        + " SET deleted_at = floor(extract(epoch from((current_timestamp -"
        + " timestamp '1970-01-01 00:00:00')*1000))) "
        + " WHERE metalake_id = #{metalakeId} AND deleted_at = 0";
  }

  public String softDeleteJobMetaByRunId(@Param("jobRunId") Long jobRunId) {
    return "UPDATE "
        + JobMetaMapper.TABLE_NAME
        + " SET deleted_at = floor(extract(epoch from((current_timestamp -"
        + " timestamp '1970-01-01 00:00:00')*1000))) "
        + " WHERE job_run_id = #{jobRunId} AND deleted_at = 0";
  }

  @Override
  public String softDeleteJobMetasByLegacyTimeline(@Param("legacyTimeline") Long legacyTimeline) {
    return "UPDATE "
        + JobMetaMapper.TABLE_NAME
        + " SET deleted_at = floor(extract(epoch from((current_timestamp -"
        + " timestamp '1970-01-01 00:00:00')*1000))) "
        + " WHERE job_finished_at < #{legacyTimeline} AND job_finished_at > 0 AND deleted_at = 0";
  }

  @Override
  public String deleteJobMetasByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit) {
    return "DELETE FROM "
        + JobMetaMapper.TABLE_NAME
        + " WHERE job_run_id IN (SELECT job_run_id FROM "
        + JobMetaMapper.TABLE_NAME
        + " WHERE deleted_at < #{legacyTimeline} AND deleted_at > 0 LIMIT #{limit})";
  }
}
