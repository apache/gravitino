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
  private Long jobStartedAt;
  private Long jobFinishedAt;
  private String runtimeJobTemplate;
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
      Long jobStartedAt,
      Long jobFinishedAt,
      String runtimeJobTemplate,
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
    Preconditions.checkArgument(jobStartedAt != null, "jobStartedAt cannot be null");
    Preconditions.checkArgument(jobFinishedAt != null, "jobFinishedAt cannot be null");
    // runtimeJobTemplate is legitimately nullable: rows created before this field was introduced
    // have no resolved template to backfill.
    Preconditions.checkArgument(StringUtils.isNotBlank(auditInfo), "auditInfo cannot be blank");
    Preconditions.checkArgument(currentVersion != null, "currentVersion cannot be null");
    Preconditions.checkArgument(lastVersion != null, "lastVersion cannot be null");
    Preconditions.checkArgument(deletedAt != null, "deletedAt cannot be null");

    this.jobRunId = jobRunId;
    this.jobTemplateName = jobTemplateName;
    this.metalakeId = metalakeId;
    this.jobExecutionId = jobExecutionId;
    this.jobRunStatus = jobRunStatus;
    this.jobStartedAt = jobStartedAt;
    this.jobFinishedAt = jobFinishedAt;
    this.runtimeJobTemplate = runtimeJobTemplate;
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
    // startedAt/finishedAt are required fields on JobEntity - the caller (e.g. JobManager, when
    // the job transitions to STARTED/a terminal state) is guaranteed to have already set them,
    // using the storage layer's "not started"/"not finished" sentinel (<= 0) otherwise. The
    // entity GC cleaner relies on the finishedAt timestamp being set to clean up terminated jobs
    // later.
    try {
      return builder
          .withJobRunId(jobEntity.id())
          .withJobTemplateName(jobEntity.jobTemplateName())
          .withJobExecutionId(jobEntity.jobExecutionId())
          .withJobRunStatus(jobEntity.status().name())
          .withJobStartedAt(jobEntity.startedAt())
          .withJobFinishedAt(jobEntity.finishedAt())
          .withRuntimeJobTemplate(jobEntity.runtimeJobTemplate())
          .withAuditInfo(JsonUtils.anyFieldMapper().writeValueAsString(jobEntity.auditInfo()))
          .withCurrentVersion(INIT_VERSION)
          .withLastVersion(INIT_VERSION)
          .withDeletedAt(DEFAULT_DELETED_AT)
          .build();
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to serialize job entity", e);
    }
  }

  /**
   * Builds the {@link JobPO} to persist for an update, carrying forward the identity fields ({@code
   * jobRunId}, {@code jobTemplateName}) from the old PO since a job's template is immutable once
   * the job is created, and bumping the version counters. Unlike those identity fields, {@code
   * runtimeJobTemplate} is taken from {@code newJobEntity} rather than carried forward from the old
   * PO - this layer stores whatever the caller passes, it does not enforce that the resolved
   * runtime template never changes. That invariant is the caller's responsibility (see {@code
   * JobManager}'s updater functions, which always carry the existing value forward). This does not
   * perform any optimistic-concurrency check; the caller is responsible for any such guarantee.
   *
   * @param oldJobPO the existing {@link JobPO} being updated
   * @param newJobEntity the {@link JobEntity} with the updated status/timestamps/audit info
   * @param builder the builder to populate, pre-configured with the {@code metalakeId}
   * @return the {@code JobPO} object with updated fields
   */
  public static JobPO updateJobPO(JobPO oldJobPO, JobEntity newJobEntity, JobPOBuilder builder) {
    try {
      Long lastVersion = oldJobPO.lastVersion() + 1;
      Long currentVersion = lastVersion;

      return builder
          .withJobRunId(oldJobPO.jobRunId())
          .withJobTemplateName(oldJobPO.jobTemplateName())
          .withJobExecutionId(newJobEntity.jobExecutionId())
          .withJobRunStatus(newJobEntity.status().name())
          .withJobStartedAt(newJobEntity.startedAt())
          .withJobFinishedAt(newJobEntity.finishedAt())
          .withRuntimeJobTemplate(newJobEntity.runtimeJobTemplate())
          .withAuditInfo(JsonUtils.anyFieldMapper().writeValueAsString(newJobEntity.auditInfo()))
          .withCurrentVersion(currentVersion)
          .withLastVersion(lastVersion)
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
          .withStartedAt(jobPO.jobStartedAt())
          .withFinishedAt(jobPO.jobFinishedAt())
          .withRuntimeJobTemplate(jobPO.runtimeJobTemplate())
          .build();
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to deserialize job PO", e);
    }
  }
}
