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
import javax.annotation.Nullable;
import org.apache.gravitino.storage.relational.po.JobPO;
import org.apache.ibatis.annotations.DeleteProvider;
import org.apache.ibatis.annotations.InsertProvider;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.SelectProvider;
import org.apache.ibatis.annotations.UpdateProvider;

public interface JobMetaMapper {
  String TABLE_NAME = "job_run_meta";

  @InsertProvider(type = JobMetaSQLProviderFactory.class, method = "insertJobMeta")
  void insertJobMeta(@Param("jobMeta") JobPO jobPO);

  @InsertProvider(
      type = JobMetaSQLProviderFactory.class,
      method = "insertJobMetaOnDuplicateKeyUpdate")
  void insertJobMetaOnDuplicateKeyUpdate(@Param("jobMeta") JobPO jobPO);

  @SelectProvider(type = JobMetaSQLProviderFactory.class, method = "listJobPOsByMetalake")
  List<JobPO> listJobPOsByMetalake(@Param("metalakeName") String metalakeName);

  @SelectProvider(
      type = JobMetaSQLProviderFactory.class,
      method = "listJobPOsByMetalakeAndTemplate")
  List<JobPO> listJobPOsByMetalakeAndTemplate(
      @Param("metalakeName") String metalakeName, @Param("jobTemplateName") String jobTemplateName);

  @SelectProvider(type = JobMetaSQLProviderFactory.class, method = "selectJobPOByMetalakeAndRunId")
  JobPO selectJobPOByMetalakeAndRunId(
      @Param("metalakeName") String metalakeName, @Param("jobRunId") Long jobRunId);

  @UpdateProvider(type = JobMetaSQLProviderFactory.class, method = "updateJobMeta")
  Integer updateJobMeta(@Param("newJobMeta") JobPO newJobPO, @Param("oldJobMeta") JobPO oldJobPO);

  @UpdateProvider(type = JobMetaSQLProviderFactory.class, method = "softDeleteJobMetasByMetalakeId")
  void softDeleteJobMetasByMetalakeId(@Param("metalakeId") Long metalakeId);

  @UpdateProvider(
      type = JobMetaSQLProviderFactory.class,
      method = "softDeleteJobMetasByLegacyTimeline")
  void softDeleteJobMetasByLegacyTimeline(@Param("legacyTimeline") Long legacyTimeline);

  @DeleteProvider(type = JobMetaSQLProviderFactory.class, method = "deleteJobMetasByLegacyTimeline")
  Integer deleteJobMetasByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit);

  @SelectProvider(type = JobMetaSQLProviderFactory.class, method = "batchSelectJobByRunIds")
  List<JobPO> batchSelectJobByRunIds(
      @Param("metalakeName") String metalakeName, @Param("jobRunIds") List<Long> jobRunIds);
  /**
   * Locks the active row for OCC identity validation.
   *
   * @param jobRunId the stable job run ID
   * @param metalakeId the owning metalake ID
   * @return the active row identity, or null if missing
   */
  @Nullable
  @SelectProvider(type = JobMetaSQLProviderFactory.class, method = "selectJobRunIdForUpdate")
  Long selectJobRunIdForUpdate(
      @Param("jobRunId") Long jobRunId, @Param("metalakeId") Long metalakeId);

  /**
   * Deletes active metadata using a stable identity and expected version.
   *
   * @param jobRunId the stable job run ID
   * @param currentVersion the expected OCC version
   * @return the affected row count
   */
  @UpdateProvider(
      type = JobMetaSQLProviderFactory.class,
      method = "softDeleteJobByRunIdWithVersion")
  int softDeleteJobByRunIdWithVersion(
      @Param("jobRunId") Long jobRunId, @Param("currentVersion") Long currentVersion);

  /**
   * Deletes active metadata using a stable identity.
   *
   * @param jobTemplateId the stable template ID
   * @return the affected row count
   */
  @UpdateProvider(type = JobMetaSQLProviderFactory.class, method = "softDeleteJobsByTemplateId")
  int softDeleteJobsByTemplateId(@Param("jobTemplateId") Long jobTemplateId);
  /**
   * Locks a nonterminal job belonging to the template using a current database read.
   *
   * @param jobTemplateId the stable template ID
   * @return a nonterminal job ID, or null if there are none
   */
  @Nullable
  @SelectProvider(type = JobMetaSQLProviderFactory.class, method = "selectNonterminalJobForUpdate")
  Long selectNonterminalJobForUpdate(@Param("jobTemplateId") Long jobTemplateId);
}
