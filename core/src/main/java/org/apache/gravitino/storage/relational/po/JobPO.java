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
package org.apache.gravitino.storage.relational.po;

import static org.apache.gravitino.storage.relational.utils.POConverters.DEFAULT_DELETED_AT;
import static org.apache.gravitino.storage.relational.utils.POConverters.INIT_VERSION;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.base.Preconditions;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import lombok.experimental.Accessors;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.job.JobHandle;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.JobEntity;

@Getter
@Accessors(fluent = true)
@EqualsAndHashCode
@ToString
public class JobPO {

  private Long jobRunId;
  private String jobTemplateName;
  private Long metalakeId;
  private String jobExecutionId;
  private String jobRunStatus;
  private Long jobFinishedAt;
  private String auditInfo;
  private Long currentVersion;
  private Long lastVersion;
  private Long deletedAt;

  public JobPO() {
    // Default constructor for JPA
  }

  @lombok.Builder(setterPrefix = "with")
  private JobPO(
      Long jobRunId,
      String jobTemplateName,
      Long metalakeId,
      String jobExecutionId,
      String jobRunStatus,
      Long jobFinishedAt,
      String auditInfo,
      Long currentVersion,
      Long lastVersion,
      Long deletedAt) {
    Preconditions.checkArgument(jobRunId != null, "jobRunId cannot be null");
    Preconditions.checkArgument(
        StringUtils.isNotBlank(jobTemplateName), "jobTemplateName cannot be blank");
    Preconditions.checkArgument(metalakeId != null, "metalakeId cannot be null");
    Preconditions.checkArgument(
        StringUtils.isNotBlank(jobExecutionId), "jobExecutionId cannot be blank");
    Preconditions.checkArgument(
        StringUtils.isNotBlank(jobRunStatus), "jobRunStatus cannot be blank");
    Preconditions.checkArgument(jobFinishedAt != null, "jobFinishedAt cannot be null");
    Preconditions.checkArgument(StringUtils.isNotBlank(auditInfo), "auditInfo cannot be blank");
    Preconditions.checkArgument(currentVersion != null, "currentVersion cannot be null");
    Preconditions.checkArgument(lastVersion != null, "lastVersion cannot be null");
    Preconditions.checkArgument(deletedAt != null, "deletedAt cannot be null");

    this.jobRunId = jobRunId;
    this.jobTemplateName = jobTemplateName;
    this.metalakeId = metalakeId;
    this.jobExecutionId = jobExecutionId;
    this.jobRunStatus = jobRunStatus;
    this.jobFinishedAt = jobFinishedAt;
    this.auditInfo = auditInfo;
    this.currentVersion = currentVersion;
    this.lastVersion = lastVersion;
    this.deletedAt = deletedAt;
  }

  public static class JobPOBuilder {
    // Builder class for JobPO
    // Lombok will generate the builder methods based on the fields defined in JobPO
  }

  public static JobPO initializeJobPO(JobEntity jobEntity, JobPOBuilder builder) {
    // finishedAt is a required field on JobEntity - the caller (e.g. JobManager, when the job
    // transitions to a terminal state) is guaranteed to have already set it, using the storage
    // layer's "not finished" sentinel (<= 0) otherwise. The entity GC cleaner relies on this
    // timestamp being set to clean up terminated jobs later.
    try {
      return builder
          .withJobRunId(jobEntity.id())
          .withJobTemplateName(jobEntity.jobTemplateName())
          .withJobExecutionId(jobEntity.jobExecutionId())
          .withJobRunStatus(jobEntity.status().name())
          .withJobFinishedAt(jobEntity.finishedAt())
          .withAuditInfo(JsonUtils.anyFieldMapper().writeValueAsString(jobEntity.auditInfo()))
          .withCurrentVersion(INIT_VERSION)
          .withLastVersion(INIT_VERSION)
          .withDeletedAt(DEFAULT_DELETED_AT)
          .build();
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to serialize job entity", e);
    }
  }

  public static JobEntity fromJobPO(JobPO jobPO, Namespace namespace) {
    try {
      return JobEntity.builder()
          .withId(jobPO.jobRunId)
          .withJobExecutionId(jobPO.jobExecutionId)
          .withNamespace(namespace)
          .withStatus(JobHandle.Status.valueOf(jobPO.jobRunStatus))
          .withJobTemplateName(jobPO.jobTemplateName)
          .withAuditInfo(JsonUtils.anyFieldMapper().readValue(jobPO.auditInfo, AuditInfo.class))
          .withFinishedAt(jobPO.jobFinishedAt())
          .build();
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to deserialize job PO", e);
    }
  }
}
