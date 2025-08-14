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
package org.apache.gravitino.storage.relational.mapper.provider.base;

import org.apache.gravitino.storage.relational.mapper.JobMetaMapper;
import org.apache.gravitino.storage.relational.mapper.JobTemplateMetaMapper;
import org.apache.gravitino.storage.relational.mapper.MetalakeMetaMapper;
import org.apache.gravitino.storage.relational.po.JobPO;
import org.apache.ibatis.annotations.Param;

public class JobMetaBaseSQLProvider {

  public String insertJobMeta(@Param("jobMeta") JobPO jobPO) {
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
        + " #{jobMeta.deletedAt})";
  }

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
        + " ON DUPLICATE KEY UPDATE"
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

  public String listJobPOsByMetalake(@Param("metalakeName") String metalakeName) {
    return "SELECT jrm.job_run_id AS jobRunId, jtm.job_template_name AS jobTemplateName,"
        + " jrm.metalake_id AS metalakeId, jrm.job_execution_id AS jobExecutionId,"
        + " jrm.job_run_status AS jobRunStatus, jrm.job_finished_at AS jobFinishedAt,"
        + " jrm.audit_info AS auditInfo,"
        + " jrm.current_version AS currentVersion, jrm.last_version AS lastVersion,"
        + " jrm.deleted_at AS deletedAt"
        + " FROM "
        + JobMetaMapper.TABLE_NAME
        + " jrm JOIN "
        + JobTemplateMetaMapper.TABLE_NAME
        + " jtm ON jrm.job_template_id = jtm.job_template_id"
        + " JOIN "
        + MetalakeMetaMapper.TABLE_NAME
        + " mm ON jrm.metalake_id = mm.metalake_id"
        + " WHERE mm.metalake_name = #{metalakeName} AND jrm.deleted_at = 0 AND mm.deleted_at = 0"
        + " AND jtm.deleted_at = 0";
  }

  public String listJobPOsByMetalakeAndTemplate(
      @Param("metalakeName") String metalakeName,
      @Param("jobTemplateName") String jobTemplateName) {
    return "SELECT jrm.job_run_id AS jobRunId, jtm.job_template_name AS jobTemplateName,"
        + " jrm.metalake_id AS metalakeId, jrm.job_execution_id AS jobExecutionId,"
        + " jrm.job_run_status AS jobRunStatus, jrm.job_finished_at AS jobFinishedAt, "
        + " jrm.audit_info AS auditInfo,"
        + " jrm.current_version AS currentVersion, jrm.last_version AS lastVersion,"
        + " jrm.deleted_at AS deletedAt"
        + " FROM "
        + JobMetaMapper.TABLE_NAME
        + " jrm JOIN "
        + JobTemplateMetaMapper.TABLE_NAME
        + " jtm ON jrm.job_template_id = jtm.job_template_id"
        + " JOIN "
        + MetalakeMetaMapper.TABLE_NAME
        + " mm ON jrm.metalake_id = mm.metalake_id"
        + " WHERE mm.metalake_name = #{metalakeName} AND jtm.job_template_name = #{jobTemplateName}"
        + " AND jrm.deleted_at = 0 AND mm.deleted_at = 0 AND jtm.deleted_at = 0";
  }

  public String selectJobPOByMetalakeAndRunId(
      @Param("metalakeName") String metalakeName, @Param("jobRunId") Long jobRunId) {
    return "SELECT jrm.job_run_id AS jobRunId, jtm.job_template_name AS jobTemplateName,"
        + " jrm.metalake_id AS metalakeId, jrm.job_execution_id AS jobExecutionId,"
        + " jrm.job_run_status AS jobRunStatus, jrm.job_finished_at AS jobFinishedAt,"
        + " jrm.audit_info AS auditInfo,"
        + " jrm.current_version AS currentVersion, jrm.last_version AS lastVersion,"
        + " jrm.deleted_at AS deletedAt"
        + " FROM "
        + JobMetaMapper.TABLE_NAME
        + " jrm JOIN "
        + JobTemplateMetaMapper.TABLE_NAME
        + " jtm ON jrm.job_template_id = jtm.job_template_id"
        + " JOIN "
        + MetalakeMetaMapper.TABLE_NAME
        + " mm ON jrm.metalake_id = mm.metalake_id"
        + " WHERE mm.metalake_name = #{metalakeName} AND jrm.job_run_id = #{jobRunId}"
        + " AND jrm.deleted_at = 0 AND mm.deleted_at = 0 AND jtm.deleted_at = 0";
  }

  public String softDeleteJobMetaByMetalakeAndTemplate(
      @Param("metalakeName") String metalakeName,
      @Param("jobTemplateName") String jobTemplateName) {
    return "UPDATE "
        + JobMetaMapper.TABLE_NAME
        + " SET deleted_at = (UNIX_TIMESTAMP() * 1000.0)"
        + " + EXTRACT(MICROSECOND FROM CURRENT_TIMESTAMP(3)) / 1000.0"
        + " WHERE metalake_id = ("
        + " SELECT metalake_id FROM "
        + MetalakeMetaMapper.TABLE_NAME
        + " WHERE metalake_name = #{metalakeName} AND deleted_at = 0)"
        + " AND job_template_id IN ("
        + " SELECT job_template_id FROM "
        + JobTemplateMetaMapper.TABLE_NAME
        + " WHERE job_template_name = #{jobTemplateName} AND deleted_at = 0)"
        + " AND deleted_at = 0";
  }

  public String softDeleteJobMetasByMetalakeId(@Param("metalakeId") Long metalakeId) {
    return "UPDATE "
        + JobMetaMapper.TABLE_NAME
        + " SET deleted_at = (UNIX_TIMESTAMP() * 1000.0)"
        + " + EXTRACT(MICROSECOND FROM CURRENT_TIMESTAMP(3)) / 1000.0"
        + " WHERE metalake_id = #{metalakeId} AND deleted_at = 0";
  }

  public String softDeleteJobMetaByRunId(@Param("jobRunId") Long jobRunId) {
    return "UPDATE "
        + JobMetaMapper.TABLE_NAME
        + " SET deleted_at = (UNIX_TIMESTAMP() * 1000.0)"
        + " + EXTRACT(MICROSECOND FROM CURRENT_TIMESTAMP(3)) / 1000.0"
        + " WHERE job_run_id = #{jobRunId} AND deleted_at = 0";
  }

  public String softDeleteJobMetasByLegacyTimeline(@Param("legacyTimeline") Long legacyTimeline) {
    return "UPDATE "
        + JobMetaMapper.TABLE_NAME
        + " SET deleted_at = (UNIX_TIMESTAMP() * 1000.0)"
        + " + EXTRACT(MICROSECOND FROM CURRENT_TIMESTAMP(3)) / 1000.0"
        + " WHERE job_finished_at < #{legacyTimeline} AND job_finished_at > 0 AND deleted_at = 0";
  }

  public String deleteJobMetasByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit) {
    return "DELETE FROM "
        + JobMetaMapper.TABLE_NAME
        + " WHERE deleted_at < #{legacyTimeline} AND deleted_at > 0 LIMIT #{limit}";
  }
}
