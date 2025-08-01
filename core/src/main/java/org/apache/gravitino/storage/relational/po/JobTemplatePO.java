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
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.JobTemplateEntity;

@Getter
@Accessors(fluent = true)
@EqualsAndHashCode
@ToString
public class JobTemplatePO {

  private Long jobTemplateId;
  private String jobTemplateName;
  private Long metalakeId;
  private String jobTemplateComment;
  private String jobTemplateContent;
  private String auditInfo;
  private Long currentVersion;
  private Long lastVersion;
  private Long deletedAt;

  public JobTemplatePO() {
    // Default constructor for JPA
  }

  @lombok.Builder(setterPrefix = "with")
  private JobTemplatePO(
      Long jobTemplateId,
      String jobTemplateName,
      Long metalakeId,
      String jobTemplateComment,
      String jobTemplateContent,
      String auditInfo,
      Long currentVersion,
      Long lastVersion,
      Long deletedAt) {
    Preconditions.checkArgument(jobTemplateId != null, "jobTemplateId cannot be null");
    Preconditions.checkArgument(
        StringUtils.isNotBlank(jobTemplateName), "jobTemplateName cannot be blank");
    Preconditions.checkArgument(metalakeId != null, "metalakeId cannot be null");
    Preconditions.checkArgument(
        StringUtils.isNotBlank(jobTemplateContent), "jobTemplateContent cannot be blank");
    Preconditions.checkArgument(StringUtils.isNotBlank(auditInfo), "auditInfo cannot be blank");
    Preconditions.checkArgument(currentVersion != null, "currentVersion cannot be null");
    Preconditions.checkArgument(lastVersion != null, "lastVersion cannot be null");
    Preconditions.checkArgument(deletedAt != null, "deletedAt cannot be null");

    this.jobTemplateId = jobTemplateId;
    this.jobTemplateName = jobTemplateName;
    this.metalakeId = metalakeId;
    this.jobTemplateComment = jobTemplateComment;
    this.jobTemplateContent = jobTemplateContent;
    this.auditInfo = auditInfo;
    this.currentVersion = currentVersion;
    this.lastVersion = lastVersion;
    this.deletedAt = deletedAt;
  }

  public static class JobTemplatePOBuilder {
    // Builder class for JobTemplatePO
    // Lombok will generate the builder methods based on the fields defined in JobTemplatePO
  }

  public static JobTemplatePO initializeJobTemplatePO(
      JobTemplateEntity jobTemplateEntity, JobTemplatePOBuilder builder) {
    try {
      return builder
          .withJobTemplateId(jobTemplateEntity.id())
          .withJobTemplateName(jobTemplateEntity.name())
          .withJobTemplateComment(jobTemplateEntity.comment())
          .withJobTemplateContent(
              JsonUtils.anyFieldMapper().writeValueAsString(jobTemplateEntity.templateContent()))
          .withAuditInfo(
              JsonUtils.anyFieldMapper().writeValueAsString(jobTemplateEntity.auditInfo()))
          .withCurrentVersion(INIT_VERSION)
          .withLastVersion(INIT_VERSION)
          .withDeletedAt(DEFAULT_DELETED_AT)
          .build();
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to serialize job template entity", e);
    }
  }

  public static JobTemplateEntity fromJobTemplatePO(
      JobTemplatePO jobTemplatePO, Namespace namespace) {
    try {
      return JobTemplateEntity.builder()
          .withId(jobTemplatePO.jobTemplateId())
          .withName(jobTemplatePO.jobTemplateName())
          .withNamespace(namespace)
          .withComment(jobTemplatePO.jobTemplateComment())
          .withTemplateContent(
              JsonUtils.anyFieldMapper()
                  .readValue(
                      jobTemplatePO.jobTemplateContent(), JobTemplateEntity.TemplateContent.class))
          .withAuditInfo(
              JsonUtils.anyFieldMapper().readValue(jobTemplatePO.auditInfo(), AuditInfo.class))
          .build();
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to deserialize job template PO", e);
    }
  }
}
