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

import org.apache.gravitino.storage.relational.mapper.JobTemplateMetaMapper;
import org.apache.gravitino.storage.relational.mapper.MetalakeMetaMapper;
import org.apache.gravitino.storage.relational.po.JobTemplatePO;
import org.apache.ibatis.annotations.Param;

public class JobTemplateMetaBaseSQLProvider {

  public String insertJobTemplateMeta(@Param("jobTemplateMeta") JobTemplatePO jobTemplatePO) {
    return "INSERT INTO "
        + JobTemplateMetaMapper.TABLE_NAME
        + " (job_template_id, job_template_name, metalake_id,"
        + " job_template_comment, job_template_content, audit_info,"
        + " current_version, last_version, deleted_at)"
        + " VALUES (#{jobTemplateMeta.jobTemplateId}, #{jobTemplateMeta.jobTemplateName},"
        + " #{jobTemplateMeta.metalakeId}, #{jobTemplateMeta.jobTemplateComment},"
        + " #{jobTemplateMeta.jobTemplateContent}, #{jobTemplateMeta.auditInfo},"
        + " #{jobTemplateMeta.currentVersion}, #{jobTemplateMeta.lastVersion},"
        + " #{jobTemplateMeta.deletedAt})";
  }

  public String insertJobTemplateMetaOnDuplicateKeyUpdate(
      @Param("jobTemplateMeta") JobTemplatePO jobTemplatePO) {
    return "INSERT INTO "
        + JobTemplateMetaMapper.TABLE_NAME
        + " (job_template_id, job_template_name, metalake_id,"
        + " job_template_comment, job_template_content, audit_info, current_version,"
        + " last_version, deleted_at)"
        + " VALUES (#{jobTemplateMeta.jobTemplateId}, #{jobTemplateMeta.jobTemplateName},"
        + " #{jobTemplateMeta.metalakeId}, #{jobTemplateMeta.jobTemplateComment},"
        + " #{jobTemplateMeta.jobTemplateContent}, #{jobTemplateMeta.auditInfo},"
        + " #{jobTemplateMeta.currentVersion}, #{jobTemplateMeta.lastVersion},"
        + " #{jobTemplateMeta.deletedAt})"
        + " ON DUPLICATE KEY UPDATE"
        + " job_template_name = #{jobTemplateMeta.jobTemplateName},"
        + " metalake_id = #{jobTemplateMeta.metalakeId},"
        + " job_template_comment = #{jobTemplateMeta.jobTemplateComment},"
        + " job_template_content = #{jobTemplateMeta.jobTemplateContent},"
        + " audit_info = #{jobTemplateMeta.auditInfo},"
        + " current_version = #{jobTemplateMeta.currentVersion},"
        + " last_version = #{jobTemplateMeta.lastVersion},"
        + " deleted_at = #{jobTemplateMeta.deletedAt}";
  }

  public String listJobTemplatePOsByMetalake(@Param("metalakeName") String metalakeName) {
    return "SELECT jtm.job_template_id AS jobTemplateId, jtm.job_template_name AS jobTemplateName,"
        + " jtm.metalake_id AS metalakeId, jtm.job_template_comment AS jobTemplateComment,"
        + " jtm.job_template_content AS jobTemplateContent, jtm.audit_info AS auditInfo,"
        + " jtm.current_version AS currentVersion, jtm.last_version AS lastVersion,"
        + " jtm.deleted_at AS deletedAt"
        + " FROM "
        + JobTemplateMetaMapper.TABLE_NAME
        + " jtm JOIN "
        + MetalakeMetaMapper.TABLE_NAME
        + " mm ON jtm.metalake_id = mm.metalake_id"
        + " WHERE mm.metalake_name = #{metalakeName} AND jtm.deleted_at = 0 AND mm.deleted_at = 0";
  }

  public String selectJobTemplatePOByMetalakeAndName(
      @Param("metalakeName") String metalakeName,
      @Param("jobTemplateName") String jobTemplateName) {
    return "SELECT jtm.job_template_id AS jobTemplateId, jtm.job_template_name AS jobTemplateName,"
        + " jtm.metalake_id AS metalakeId, jtm.job_template_comment AS jobTemplateComment,"
        + " jtm.job_template_content AS jobTemplateContent, jtm.audit_info AS auditInfo,"
        + " jtm.current_version AS currentVersion, jtm.last_version AS lastVersion,"
        + " jtm.deleted_at AS deletedAt"
        + " FROM "
        + JobTemplateMetaMapper.TABLE_NAME
        + " jtm JOIN "
        + MetalakeMetaMapper.TABLE_NAME
        + " mm ON jtm.metalake_id = mm.metalake_id"
        + " WHERE mm.metalake_name = #{metalakeName} AND jtm.job_template_name = #{jobTemplateName}"
        + " AND jtm.deleted_at = 0 AND mm.deleted_at = 0";
  }

  public String softDeleteJobTemplateMetaByMetalakeAndName(
      @Param("metalakeName") String metalakeName,
      @Param("jobTemplateName") String jobTemplateName) {
    return "UPDATE "
        + JobTemplateMetaMapper.TABLE_NAME
        + " SET deleted_at = (UNIX_TIMESTAMP() * 1000.0)"
        + " + EXTRACT(MICROSECOND FROM CURRENT_TIMESTAMP(3)) / 1000.0"
        + " WHERE job_template_name = #{jobTemplateName} AND metalake_id ="
        + " (SELECT metalake_id FROM "
        + MetalakeMetaMapper.TABLE_NAME
        + " WHERE metalake_name = #{metalakeName} AND deleted_at = 0)"
        + " AND deleted_at = 0";
  }

  public String softDeleteJobTemplateMetasByMetalakeId(@Param("metalakeId") Long metalakeId) {
    return "UPDATE "
        + JobTemplateMetaMapper.TABLE_NAME
        + " SET deleted_at = (UNIX_TIMESTAMP() * 1000.0)"
        + " + EXTRACT(MICROSECOND FROM CURRENT_TIMESTAMP(3)) / 1000.0"
        + " WHERE metalake_id = #{metalakeId} AND deleted_at = 0";
  }

  public String deleteJobTemplateMetasByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit) {
    return "DELETE FROM "
        + JobTemplateMetaMapper.TABLE_NAME
        + " WHERE deleted_at < #{legacyTimeline} AND deleted_at > 0 LIMIT #{limit}";
  }
}
