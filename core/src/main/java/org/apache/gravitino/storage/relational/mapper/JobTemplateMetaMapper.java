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

import java.util.List;
import org.apache.gravitino.storage.relational.po.JobTemplatePO;
import org.apache.ibatis.annotations.DeleteProvider;
import org.apache.ibatis.annotations.InsertProvider;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.SelectProvider;
import org.apache.ibatis.annotations.UpdateProvider;

public interface JobTemplateMetaMapper {
  String TABLE_NAME = "job_template_meta";

  @InsertProvider(type = JobTemplateMetaSQLProviderFactory.class, method = "insertJobTemplateMeta")
  void insertJobTemplateMeta(@Param("jobTemplateMeta") JobTemplatePO jobTemplatePO);

  @InsertProvider(
      type = JobTemplateMetaSQLProviderFactory.class,
      method = "insertJobTemplateMetaOnDuplicateKeyUpdate")
  void insertJobTemplateMetaOnDuplicateKeyUpdate(
      @Param("jobTemplateMeta") JobTemplatePO jobTemplatePO);

  @SelectProvider(
      type = JobTemplateMetaSQLProviderFactory.class,
      method = "listJobTemplatePOsByMetalake")
  List<JobTemplatePO> listJobTemplatePOsByMetalake(@Param("metalakeName") String metalakeName);

  @SelectProvider(
      type = JobTemplateMetaSQLProviderFactory.class,
      method = "selectJobTemplatePOByMetalakeAndName")
  JobTemplatePO selectJobTemplatePOByMetalakeAndName(
      @Param("metalakeName") String metalakeName, @Param("jobTemplateName") String jobTemplateName);

  @UpdateProvider(
      type = JobTemplateMetaSQLProviderFactory.class,
      method = "softDeleteJobTemplateMetaByMetalakeAndName")
  Integer softDeleteJobTemplateMetaByMetalakeAndName(
      @Param("metalakeName") String metalakeName, @Param("jobTemplateName") String jobTemplateName);

  @UpdateProvider(
      type = JobTemplateMetaSQLProviderFactory.class,
      method = "softDeleteJobTemplateMetasByMetalakeId")
  void softDeleteJobTemplateMetasByMetalakeId(@Param("metalakeId") Long metalakeId);

  @DeleteProvider(
      type = JobTemplateMetaSQLProviderFactory.class,
      method = "deleteJobTemplateMetasByLegacyTimeline")
  Integer deleteJobTemplateMetasByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit);
}
