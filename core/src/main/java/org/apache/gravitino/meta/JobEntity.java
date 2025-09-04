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

package org.apache.gravitino.meta;

import com.google.common.collect.Maps;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import lombok.ToString;
import org.apache.gravitino.Auditable;
import org.apache.gravitino.Entity;
import org.apache.gravitino.Field;
import org.apache.gravitino.HasIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.job.JobHandle;

@ToString
public class JobEntity implements Entity, Auditable, HasIdentifier {

  public static final Field ID =
      Field.required("id", Long.class, "The unique id of the job entity.");
  public static final Field JOB_EXECUTION_ID =
      Field.required(
          "job_execution_id",
          String.class,
          "The unique execution id of the job, used for tracking.");
  public static final Field STATUS =
      Field.required("status", JobHandle.Status.class, "The status of the job.");
  public static final Field TEMPLATE_NAME =
      Field.required("job_template_name", String.class, "The name of the job template.");
  public static final Field AUDIT_INFO =
      Field.required(
          "audit_info", AuditInfo.class, "The audit details of the job template entity.");
  public static final Field FINISHED_AT =
      Field.optional("job_finished_at", Long.class, "The time when the job finished execution.");

  private Long id;
  private String jobExecutionId;
  private JobHandle.Status status;
  private String jobTemplateName;
  private Namespace namespace;
  private AuditInfo auditInfo;
  private Long finishedAt;

  private JobEntity() {}

  @Override
  public Map<Field, Object> fields() {
    Map<Field, Object> fields = Maps.newHashMap();
    fields.put(ID, id);
    fields.put(JOB_EXECUTION_ID, jobExecutionId);
    fields.put(TEMPLATE_NAME, jobTemplateName);
    fields.put(STATUS, status);
    fields.put(AUDIT_INFO, auditInfo);
    fields.put(FINISHED_AT, finishedAt);
    return Collections.unmodifiableMap(fields);
  }

  @Override
  public Long id() {
    return id;
  }

  public String jobExecutionId() {
    return jobExecutionId;
  }

  @Override
  public String name() {
    return JobHandle.JOB_ID_PREFIX + id;
  }

  @Override
  public Namespace namespace() {
    return namespace;
  }

  public JobHandle.Status status() {
    return status;
  }

  public String jobTemplateName() {
    return jobTemplateName;
  }

  public Long finishedAt() {
    return finishedAt;
  }

  @Override
  public AuditInfo auditInfo() {
    return auditInfo;
  }

  @Override
  public EntityType type() {
    return EntityType.JOB;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof JobEntity)) {
      return false;
    }

    JobEntity that = (JobEntity) o;
    return Objects.equals(id, that.id)
        && Objects.equals(jobExecutionId, that.jobExecutionId)
        && Objects.equals(status, that.status)
        && Objects.equals(jobTemplateName, that.jobTemplateName)
        && Objects.equals(namespace, that.namespace)
        && Objects.equals(auditInfo, that.auditInfo);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, jobExecutionId, namespace, status, jobTemplateName, auditInfo);
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private final JobEntity jobEntity;

    private Builder() {
      this.jobEntity = new JobEntity();
    }

    public Builder withId(Long id) {
      jobEntity.id = id;
      return this;
    }

    public Builder withJobExecutionId(String jobExecutionId) {
      jobEntity.jobExecutionId = jobExecutionId;
      return this;
    }

    public Builder withNamespace(Namespace namespace) {
      jobEntity.namespace = namespace;
      return this;
    }

    public Builder withStatus(JobHandle.Status status) {
      jobEntity.status = status;
      return this;
    }

    public Builder withJobTemplateName(String jobTemplateName) {
      jobEntity.jobTemplateName = jobTemplateName;
      return this;
    }

    public Builder withAuditInfo(AuditInfo auditInfo) {
      jobEntity.auditInfo = auditInfo;
      return this;
    }

    public Builder withFinishedAt(Long finishedAt) {
      jobEntity.finishedAt = finishedAt;
      return this;
    }

    public JobEntity build() {
      jobEntity.validate();
      return jobEntity;
    }
  }
}
